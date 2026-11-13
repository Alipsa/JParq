package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression coverage for decoding physical MAP values consistently with the
 * values inside struct fields (issue #313).
 */
@Isolated("Changes the JVM default time zone")
class MapJsonSerializationTest {
  @TempDir
  Path directory;

  /**
   * Decodes logical map values in JSON constructors, including maps inside
   * arrays, structs, and nested containers, for stored, derived, and CTE inputs
   * with and without pushdown.
   *
   * @param pushdown
   *          whether projection and predicate pushdown are enabled
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void serializesStoredMaps(boolean pushdown) throws Exception {
    writeMaps();
    try (var connection = DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?projectionPushdown=" + pushdown + "&predicatePushdown=" + pushdown)) {
      for (String input : List.of("maps", "(SELECT * FROM maps) a",
          "(WITH c AS (SELECT * FROM maps) SELECT * FROM c) a")) {
        for (String function : List.of("JSON_ARRAY(%s)", "JSON_OBJECT(KEY 'k' VALUE %s)")) {
          String from = " FROM " + input + " WHERE id = 1";
          assertJson(connection, function, "m", from, "{\"d\":\"2024-01-01\"}");
          assertJson(connection, function, "am", from, "[{\"d\":\"2024-01-01\"},null]");
          assertJson(connection, function, "dm", from, "{\"p\":1.50}");
          assertJson(connection, function, "tm", from, "{\"t\":\"2023-11-14T22:13:20.123Z\"}");
          assertJson(connection, function, "um", from, "{\"u\":\"2023-11-14T22:13:20.123456Z\"}");
          assertJson(connection, function, "un", from, "{\"n\":\"2023-11-14T22:13:20.123456789Z\"}");
          assertJson(connection, function, "lm", from, "{\"l\":\"2023-11-14T22:13:20.123456\"}");
          // Sub-millisecond TIME digits are truncated by JDBC decoding (#521);
          // the JSON output pads the declared precision with zeros.
          assertJson(connection, function, "lt", from, "{\"t\":\"12:00:00.123000\"}");
          assertJson(connection, function, "sm", from, "{\"md\":{\"d\":\"2024-01-01\"}}");
          assertJson(connection, function, "cm", from, "{\"n\":[1,null,2]}");
          // Binary values stay data: JSON-looking content is not parsed.
          assertJson(connection, function, "bj", from, "{\"j\":\"{\\\"a\\\":1}\"}");
          assertJson(connection, function, "bh", from, "{\"h\":\"hi\"}");
          assertJson(connection, function, "nm", " FROM " + input + " WHERE id = 2", "null");
          assertJson(connection, function, "ARRAY[m, m]", from, "[{\"d\":\"2024-01-01\"},{\"d\":\"2024-01-01\"}]");
        }
      }
    }
  }

  /**
   * Materializes map columns through derived tables, CTEs, and UNIONs, decoding
   * their values for JDBC getters and preserving NULL maps.
   *
   * @param pushdown
   *          whether projection and predicate pushdown are enabled
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void materializesStoredMaps(boolean pushdown) throws Exception {
    writeMaps();
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("d", Date.valueOf("2024-01-01"));
    Map<String, Object> expectedMicros = Map.of("u", Timestamp.from(Instant.parse("2023-11-14T22:13:20.123456Z")));
    Map<String, Object> expectedNanos = Map.of("n", Timestamp.from(Instant.ofEpochSecond(1_700_000_000L, 123_456_789)));
    Map<String, Object> expectedLocal = Map.of("l",
        Timestamp.valueOf(LocalDateTime.parse("2023-11-14T22:13:20.123456")));
    try (var connection = DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?projectionPushdown=" + pushdown + "&predicatePushdown=" + pushdown)) {
      for (String input : List.of("maps", "(SELECT * FROM maps) a",
          "(WITH c AS (SELECT * FROM maps) SELECT * FROM c) a")) {
        String sql = "SELECT m, am, nm, um, un, lm FROM " + input + " WHERE id = 1";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(expected, result.getObject(1), sql);
          Object[] arrays = assertInstanceOf(Object[].class, result.getObject(2), sql);
          assertEquals(2, arrays.length, sql);
          assertEquals(expected, arrays[0], sql);
          assertNull(arrays[1], sql);
          assertEquals(expected, result.getObject(3), sql);
          assertEquals(expectedMicros, result.getObject(4), sql);
          assertEquals(expectedNanos, result.getObject(5), sql);
          assertEquals(expectedLocal, result.getObject(6), sql);
          assertFalse(result.next(), sql);
        }
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT nm FROM " + input + " WHERE id = 2")) {
          assertTrue(result.next());
          assertNull(result.getObject(1));
          assertFalse(result.next());
        }
      }
      String union = "SELECT JSON_ARRAY(lm), JSON_ARRAY(um) FROM (SELECT lm, um FROM maps WHERE id = 1"
          + " UNION ALL SELECT lm, um FROM maps WHERE id = 1) u";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(union)) {
        for (int row = 0; row < 2; row++) {
          assertTrue(result.next(), union);
          assertEquals("[{\"l\":\"2023-11-14T22:13:20.123456\"}]", result.getString(1), union);
          assertEquals("[{\"u\":\"2023-11-14T22:13:20.123456Z\"}]", result.getString(2), union);
        }
        assertFalse(result.next(), union);
      }
    }
  }

  /**
   * Retains local timestamp fields of maps inside ARRAY constructors without
   * normalizing nonexistent wall-clock times through the host zone
   * ([#446](https://github.com/Alipsa/JParq/issues/446)). The gap value is
   * asserted on paths that retain local fields; materialization decodes local
   * timestamps through JDBC {@link Timestamp}, which normalizes gap values in the
   * host zone, so derived and CTE inputs use a non-gap value.
   *
   * @param zone
   *          JVM default time zone
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UTC", "Europe/Stockholm", "America/New_York"
  })
  void serializesLocalTimestampMapsInGapZones(String zone) throws Exception {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      writeGapMaps();
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
        for (String expression : List.of("gm", "ARRAY[gm, gm]")) {
          String value = expression.startsWith("ARRAY")
              ? "[{\"g\":\"2024-03-31T02:30:00.123456\"}," + "{\"g\":\"2024-03-31T02:30:00.123456\"}]"
              : "{\"g\":\"2024-03-31T02:30:00.123456\"}";
          String sql = "SELECT JSON_ARRAY(" + expression + ") FROM gapmaps";
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertTrue(result.next(), zone + ": " + sql);
            assertEquals("[" + value + "]", result.getString(1), zone + ": " + sql);
            assertFalse(result.next(), sql);
          }
        }
        for (String input : List.of("gapmaps", "(SELECT * FROM gapmaps) a",
            "(WITH c AS (SELECT * FROM gapmaps) SELECT * FROM c) a")) {
          String sql = "SELECT JSON_ARRAY(lm) FROM " + input;
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertTrue(result.next(), zone + ": " + sql);
            assertEquals("[{\"l\":\"2023-11-14T22:13:20.123456\"}]", result.getString(1), zone + ": " + sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  /**
   * Writes a table with physical map columns of logical, nested, and NULL values.
   *
   * @throws IOException
   *           if fixture writing fails
   */
  private void writeMaps() throws IOException {
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema decimal = LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema timeMicros = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampMillis = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampMicros = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampNanos = LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG));
    Schema localTimestampMicros = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema nullableDate = Schema.createUnion(Schema.create(Schema.Type.NULL), date);
    Schema nullableInteger = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    Schema mapDates = Schema.createMap(date);
    Schema nullableMapDates = Schema.createUnion(Schema.create(Schema.Type.NULL), mapDates);
    Schema structMaps = SchemaBuilder.record("structMaps").fields().name("md").type(mapDates).noDefault().endRecord();
    Schema bytes = Schema.create(Schema.Type.BYTES);
    Schema schema = SchemaBuilder.record("maps").fields().requiredInt("id").name("m").type(nullableMapDates).noDefault()
        .name("am")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL),
            Schema.createArray(Schema.createUnion(Schema.create(Schema.Type.NULL), mapDates))))
        .noDefault().name("dm").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(decimal)))
        .noDefault().name("tm")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(timestampMillis))).noDefault()
        .name("um").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(timestampMicros)))
        .noDefault().name("un")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(timestampNanos))).noDefault()
        .name("lm").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(localTimestampMicros)))
        .noDefault().name("lt").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(timeMicros)))
        .noDefault().name("sm").type(Schema.createUnion(Schema.create(Schema.Type.NULL), structMaps)).noDefault()
        .name("cm")
        .type(
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(Schema.createArray(nullableInteger))))
        .noDefault().name("bj").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(bytes)))
        .noDefault().name("bh").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(bytes)))
        .noDefault().name("nm").type(nullableMapDates).noDefault().endRecord();
    Configuration configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("maps.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      var dates = new LinkedHashMap<String, Object>();
      dates.put("d", (int) LocalDate.parse("2024-01-01").toEpochDay());
      var row = new GenericData.Record(schema);
      row.put("id", 1);
      row.put("m", dates);
      row.put("am", Arrays.asList(dates, null));
      row.put("dm", Map.of("p", ByteBuffer.wrap(new BigDecimal("1.50").unscaledValue().toByteArray())));
      row.put("tm", Map.of("t", Instant.parse("2023-11-14T22:13:20.123Z").toEpochMilli()));
      row.put("um", Map.of("u", 1_700_000_000_123_456L));
      row.put("un", Map.of("n", 1_700_000_000_123_456_789L));
      row.put("lm", Map.of("l", 1_700_000_000_123_456L));
      row.put("lt", Map.of("t", 43_200_123_456L));
      var struct = new GenericData.Record(structMaps);
      struct.put("md", dates);
      row.put("sm", struct);
      row.put("cm", Map.of("n", Arrays.asList(1, null, 2)));
      row.put("bj", Map.of("j", ByteBuffer.wrap("{\"a\":1}".getBytes(StandardCharsets.UTF_8))));
      row.put("bh", Map.of("h", ByteBuffer.wrap("hi".getBytes(StandardCharsets.UTF_8))));
      row.put("nm", dates);
      writer.write(row);
      var nulls = new GenericData.Record(schema);
      nulls.put("id", 2);
      for (String field : List.of("m", "am", "dm", "tm", "um", "un", "lm", "lt", "sm", "cm", "bj", "bh", "nm")) {
        nulls.put(field, null);
      }
      writer.write(nulls);
    }
  }

  /**
   * Writes a table with local timestamp map values, one falling in the
   * Europe/Stockholm daylight-saving gap and one at an ordinary wall-clock time.
   *
   * @throws IOException
   *           if fixture writing fails
   */
  private void writeGapMaps() throws IOException {
    Schema localTimestampMicros = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema = SchemaBuilder.record("gapmaps").fields().requiredInt("id").name("gm")
        .type(Schema.createMap(localTimestampMicros)).noDefault().name("lm")
        .type(Schema.createMap(localTimestampMicros)).noDefault().endRecord();
    Configuration configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("gapmaps.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      var row = new GenericData.Record(schema);
      row.put("id", 1);
      row.put("gm", Map.of("g", toLocalMicros(LocalDateTime.parse("2024-03-31T02:30:00.123456"))));
      row.put("lm", Map.of("l", toLocalMicros(LocalDateTime.parse("2023-11-14T22:13:20.123456"))));
      writer.write(row);
    }
  }

  /**
   * Converts wall-clock fields to the local timestamp storage representation.
   *
   * @param local
   *          wall-clock fields
   * @return stored epoch micros against the UTC origin
   */
  private static long toLocalMicros(LocalDateTime local) {
    Instant instant = local.toInstant(ZoneOffset.UTC);
    return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000);
  }

  /**
   * Checks one constructor's serialized output and result cardinality.
   *
   * @param connection
   *          open JDBC connection
   * @param function
   *          constructor format string
   * @param expression
   *          value expression
   * @param from
   *          optional FROM and WHERE clauses
   * @param value
   *          expected JSON value
   * @throws SQLException
   *           if querying fails
   */
  private static void assertJson(Connection connection, String function, String expression, String from, String value)
      throws SQLException {
    String sql = "SELECT " + function.formatted(expression) + from;
    String expected = function.startsWith("JSON_OBJECT") ? "{\"k\":" + value + "}" : "[" + value + "]";
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      assertTrue(result.next(), sql);
      assertEquals(expected, result.getString(1), sql);
      assertFalse(result.next(), sql);
    }
  }
}
