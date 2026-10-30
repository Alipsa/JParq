package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqConnection;

/** Verifies binary literals and typed JDBC parameters against stored values. */
class BinaryTemporalParameterTest {
  @TempDir
  Path directory;

  private static final Date DATE = Date.valueOf("2024-01-01");
  private static final Time TIME = Time.valueOf("10:20:30");
  private static final Timestamp TIMESTAMP = Timestamp.valueOf("2024-01-01 10:20:30.123456");
  private static final byte[] BYTES = {
      0, 65, (byte) 255
  };

  /**
   * Rejects timestamp parameter years whose JDBC era would be lost.
   *
   * @param year
   *          unrepresentable signed year
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(ints = {
      -5, 0
  })
  void rejectsTimestampEraLoss(int year) throws Exception {
    try (var connection = connection(true, true); var statement = connection.prepareStatement("SELECT ?")) {
      statement.setObject(1, java.time.LocalDate.of(year, 1, 1).atStartOfDay());
      var failure = assertThrows(java.sql.SQLDataException.class, statement::executeQuery);
      assertEquals("22008", failure.getSQLState());
      statement.setObject(1, java.time.LocalDateTime.of(1, 1, 1, 0, 0));
      try (var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals(java.time.LocalDateTime.of(1, 1, 1, 0, 0), result.getTimestamp(1).toLocalDateTime());
      }
    }
  }

  /**
   * Writes distinct binary and temporal rows plus a NULL row.
   *
   * @throws Exception
   *           if the fixture cannot be written
   */
  @BeforeEach
  void createFixture() throws Exception {
    Schema binary = Schema.create(Schema.Type.BYTES);
    binary.addProp("jparq.binary", true);
    Schema schema = SchemaBuilder.record("typed").fields().requiredInt("id").name("b")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL), binary)).noDefault().name("d")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL),
            LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))))
        .noDefault().name("t")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL),
            LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))))
        .noDefault().name("ts").type(Schema.createUnion(Schema.create(Schema.Type.NULL),
            LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))))
        .noDefault().endRecord();
    var file = new org.apache.hadoop.fs.Path(directory.resolve("typed.parquet").toUri());
    try (
        var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, new Configuration(false)))
            .withSchema(schema).withConf(new Configuration(false)).build()) {
      for (int id = 1; id <= 3; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        if (id < 3) {
          row.put("b", ByteBuffer.wrap(id == 1 ? BYTES : new byte[]{
              66
          }));
          row.put("d", (int) DATE.toLocalDate().toEpochDay() + id - 1);
          row.put("t", (int) (TIME.toLocalTime().toNanoOfDay() / 1_000_000) + (id - 1) * 1000);
          row.put("ts",
              TIMESTAMP.toInstant().getEpochSecond() * 1_000_000 + TIMESTAMP.getNanos() / 1000 + (id - 1) * 1_000_000);
        }
        writer.write(row);
      }
    }
  }

  /**
   * Preserves literal and bound value types in table-free and physical
   * projections.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void projectionsRetainTypes(boolean predicate, boolean projection) throws Exception {
    try (var connection = connection(predicate, projection)) {
      for (String from : List.of("", " FROM typed WHERE id = 1")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT X'0041fF', 0x0041FF, X'', X'41' || X'42'" + from)) {
          assertTrue(result.next());
          assertArrayEquals(BYTES, assertInstanceOf(byte[].class, result.getObject(1)));
          assertArrayEquals(BYTES, assertInstanceOf(byte[].class, result.getObject(2)));
          assertArrayEquals(new byte[0], assertInstanceOf(byte[].class, result.getObject(3)));
          assertArrayEquals(new byte[]{
              65, 66
          }, assertInstanceOf(byte[].class, result.getObject(4)));
          assertFalse(result.next());
        }
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{
            99, 0, 65, (byte) 255, 99
        });
        buffer.position(1).limit(4);
        ByteBuffer parameterBuffer = buffer.asReadOnlyBuffer();
        Timestamp precise = Timestamp.valueOf("2024-01-01 10:20:30.123456789");
        try (var statement = connection.prepareStatement("SELECT ?" + from)) {
          for (Object value : List.of(BYTES, parameterBuffer, DATE, TIME, precise, DATE.toLocalDate(),
              TIME.toLocalTime(), precise.toLocalDateTime(), new byte[0])) {
            switch (value) {
              case byte[] bytes -> statement.setBytes(1, bytes);
              case Date date -> statement.setDate(1, date);
              case Time time -> statement.setTime(1, time);
              case Timestamp timestamp -> statement.setTimestamp(1, timestamp);
              default -> statement.setObject(1, value);
            }
            try (var result = statement.executeQuery()) {
              assertTrue(result.next(), "Bound projection" + from + ": " + value);
              Object actual = result.getObject(1);
              switch (value) {
                case byte[] bytes -> assertArrayEquals(bytes, assertInstanceOf(byte[].class, actual));
                case ByteBuffer ignored -> assertArrayEquals(BYTES, assertInstanceOf(byte[].class, actual));
                case Date ignored -> assertEquals(DATE, assertInstanceOf(Date.class, actual));
                case Time ignored -> assertEquals(TIME, assertInstanceOf(Time.class, actual));
                case Timestamp ignored -> assertEquals(precise, assertInstanceOf(Timestamp.class, actual));
                case java.time.LocalDate ignored -> assertEquals(DATE, assertInstanceOf(Date.class, actual));
                case java.time.LocalTime ignored -> assertEquals(TIME, assertInstanceOf(Time.class, actual));
                case java.time.LocalDateTime ignored ->
                  assertEquals(precise, assertInstanceOf(Timestamp.class, actual));
                default -> fail("Unexpected parameter " + value);
              }
              assertFalse(result.next());
            }
          }
          statement.setBytes(1, null);
          try (var result = statement.executeQuery()) {
            assertTrue(result.next());
            assertNull(result.getObject(1));
          }
        }
        assertEquals(1, parameterBuffer.position());
        assertEquals(4, parameterBuffer.limit());
      }
    }
  }

  /**
   * Compares each parameter and literal with the corresponding physical column.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void comparisonsMatchStoredTypes(boolean predicate, boolean projection) throws Exception {
    try (var connection = connection(predicate, projection)) {
      for (String condition : List.of("b = X'0041FF'", "b = 0x0041ff", "d = DATE '2024-01-01'", "t = TIME '10:20:30'",
          "ts = TIMESTAMP '2024-01-01 10:20:30.123456'")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT id FROM typed WHERE " + condition)) {
          assertTrue(result.next(), condition);
          assertEquals(1, result.getInt(1), condition);
          assertFalse(result.next(), condition);
        }
      }
      for (String column : List.of("b", "buffer", "d", "t", "ts")) {
        try (var statement = connection
            .prepareStatement("SELECT id FROM typed WHERE " + (column.equals("buffer") ? "b" : column) + " = ?")) {
          switch (column) {
            case "b" -> statement.setBytes(1, BYTES);
            case "buffer" -> statement.setObject(1, ByteBuffer.wrap(BYTES));
            case "d" -> statement.setDate(1, DATE);
            case "t" -> statement.setTime(1, TIME);
            case "ts" -> statement.setTimestamp(1, TIMESTAMP);
            default -> fail("Unexpected column " + column);
          }
          try (var result = statement.executeQuery()) {
            assertTrue(result.next(), column);
            assertEquals(1, result.getInt(1), column);
            assertFalse(result.next(), column);
          }
        }
      }
    }
  }

  /**
   * Opens a connection with explicit optimizer settings.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @return the test connection
   * @throws java.sql.SQLException
   *           if the connection cannot be opened
   */
  private JParqConnection connection(boolean predicate, boolean projection) throws java.sql.SQLException {
    return new JParqConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection,
        new java.util.Properties());
  }
}
