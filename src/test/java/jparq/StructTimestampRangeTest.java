package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.function.ArrayFunctions;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/**
 * Verifies full-range, lossless STRUCT timestamp combinations with nanosecond
 * fields.
 */
@Isolated("Changes the JVM default time zone and user.timezone")
class StructTimestampRangeTest {
  private static final long LOADED = 1_700_000_000_123_456_789L;
  private static final Instant LOADED_INSTANT = Instant.ofEpochSecond(1_700_000_000L, 123_456_789L);
  private TimeZone previousDefault;
  private String previousProperty;

  /**
   * Pins the JVM and session time zones before computing expected timestamp
   * views.
   */
  @BeforeEach
  void useStockholmTimeZone() {
    previousDefault = TimeZone.getDefault();
    previousProperty = System.getProperty("user.timezone");
    System.setProperty("user.timezone", "Europe/Stockholm");
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Stockholm"));
  }

  /** Restores both JVM time-zone sources after each test invocation. */
  @AfterEach
  void restoreTimeZone() {
    TimeZone.setDefault(previousDefault);
    if (previousProperty == null) {
      System.clearProperty("user.timezone");
    } else {
      System.setProperty("user.timezone", previousProperty);
    }
  }

  /**
   * Retains range and precision in either order and when combining already
   * normalized records.
   *
   * @param date
   *          stored date outside the long-based nanosecond range
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "9999-12-31", "2300-01-01", "1600-01-01", "0001-01-01"
  })
  void combinesFullRangeTimestamps(String date) {
    var instant = Instant.parse(date + "T00:00:00Z");
    for (boolean local : List.of(false, true)) {
      final var expected = visibleValue(instant, local);
      final var loadedValue = visibleValue(LOADED_INSTANT, local);
      Schema coarseSchema = (local ? LogicalTypes.localTimestampMicros() : LogicalTypes.timestampMicros())
          .addToSchema(Schema.create(Schema.Type.LONG));
      Schema nanosSchema = (local ? LogicalTypes.localTimestampNanos() : LogicalTypes.timestampNanos())
          .addToSchema(Schema.create(Schema.Type.LONG));
      var coarse = record("coarse", coarseSchema, instant.getEpochSecond() * 1_000_000L);
      var fine = record("fine", nanosSchema, LOADED);
      for (var pair : List.of(List.of(coarse, fine), List.of(fine, coarse))) {
        var result = ArrayFunctions.homogenizeArrayValues(new ArrayList<>(pair));
        for (int index = 0; index < pair.size(); index++) {
          assertEquals(pair.get(index) == coarse ? expected : loadedValue, field((IndexedRecord) result.get(index)));
        }
        var combined = ArrayFunctions.homogenizeArrayValues(List.of(result.getFirst(), coarse, fine));
        assertEquals(expected, field((IndexedRecord) combined.get(1)));
        assertEquals(loadedValue, field((IndexedRecord) combined.get(2)));
      }
    }
  }

  /**
   * Checks a representable negative nanosecond boundary without intermediate
   * multiplication overflow.
   */
  @Test
  void convertsNegativeNanosecondBoundary() {
    Schema plain = LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG));
    Schema annotated = LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG));
    annotated.addProp("storage", "other writer");
    var values = ArrayFunctions
        .homogenizeArrayValues(List.of(record("first", plain, 0L), record("boundary", annotated, Long.MIN_VALUE)));
    assertEquals(Long.MIN_VALUE, ((IndexedRecord) values.get(1)).get(0));
  }

  /**
   * Reads actual nested Parquet NANOS annotations through projections and
   * materializations.
   *
   * @param date
   *          stored date beyond the nanosecond epoch range
   * @param directory
   *          database directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "9999-12-31", "2300-01-01", "1600-01-01", "0001-01-01"
  })
  void readsStoredTimestampRange(String date, @TempDir Path directory) throws Exception {
    var instant = Instant.parse(date + "T00:00:00Z");
    final var expected = visibleValue(instant, false);
    final var loadedValue = visibleValue(LOADED_INSTANT, false);
    writeTimestamps(directory, instant, LOADED);
    for (boolean pushdown : List.of(false, true)) {
      try (
          var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?projectionPushdown=" + pushdown)) {
        for (String sql : List.of("SELECT ARRAY[valid_to,loaded] FROM t", "SELECT ARRAY[loaded,valid_to] FROM t",
            "SELECT a FROM (SELECT ARRAY[valid_to,loaded] AS a FROM t) d",
            "WITH c AS (SELECT ARRAY[valid_to,loaded] AS a FROM t) SELECT a FROM c")) {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            Object[] records = (Object[]) result.getObject(1);
            int coarse = sql.contains("ARRAY[loaded,") ? 1 : 0;
            assertEquals(expected, field((IndexedRecord) records[coarse]), sql);
            assertEquals(loadedValue, field((IndexedRecord) records[1 - coarse]), sql);
          }
        }
        try (var statement = connection.createStatement();
            var result = statement
                .executeQuery("SELECT u.x FROM t CROSS JOIN UNNEST(ARRAY[t.valid_to,t.loaded]) AS u(x)")) {
          assertTrue(result.next());
          assertEquals(Types.TIMESTAMP, result.getMetaData().getColumnType(1));
          assertEquals(expected, result.getObject(1));
          assertTrue(result.next());
          assertEquals(loadedValue, result.getObject(1));
        }
      }
    }
  }

  /**
   * Keeps the standalone JVM-zone view and typed timestamp value when only
   * sibling storage units change.
   *
   * @param directory
   *          database directory
   * @throws Exception
   *           if fixture writing or querying fails
   */
  @Test
  void preservesStandaloneTimestampViewAcrossSiblingUnits(@TempDir Path directory) throws Exception {
    var instant = Instant.parse("2026-06-01T12:00:00Z");
    var expected = LocalDateTime.parse("2026-06-01T14:00:00");
    writeTimestamps(directory, instant, instant.getEpochSecond() * 1_000_000_000L);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT valid_to, loaded FROM t")) {
        assertTrue(result.next());
        for (int column = 1; column <= 2; column++) {
          var value = assertInstanceOf(Timestamp.class, field((IndexedRecord) result.getObject(column)));
          assertEquals(expected, value.toLocalDateTime());
          assertEquals(instant, value.toInstant());
        }
      }
      for (String array : List.of("ARRAY[t.valid_to,t.valid_to]", "ARRAY[t.loaded,t.loaded]",
          "ARRAY[t.valid_to,t.loaded]", "ARRAY[t.loaded,t.valid_to]")) {
        boolean mixed = array.contains("valid_to") && array.contains("loaded");
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT u.x FROM t CROSS JOIN UNNEST(" + array + ") AS u(x)")) {
          assertEquals(Types.TIMESTAMP, result.getMetaData().getColumnType(1), array);
          for (int row = 0; row < 2; row++) {
            assertTrue(result.next(), array);
            Object value = result.getObject(1);
            if (mixed) {
              assertEquals(expected, assertInstanceOf(LocalDateTime.class, value), array);
            } else {
              assertEquals(expected, assertInstanceOf(Timestamp.class, value).toLocalDateTime(), array);
            }
            assertEquals(expected, result.getTimestamp(1).toLocalDateTime(), array);
            assertEquals(instant, result.getTimestamp(1).toInstant(), array);
          }
        }
      }
    }
  }

  /**
   * Computes the same wall-clock view as standalone decoding for the source's
   * timestamp family.
   *
   * @param instant
   *          stored epoch value
   * @param local
   *          whether the stored fields are local rather than UTC-adjusted
   * @return expected visible datetime fields
   */
  private static LocalDateTime visibleValue(Instant instant, boolean local) {
    return local ? LocalDateTime.ofInstant(instant, ZoneOffset.UTC) : Timestamp.from(instant).toLocalDateTime();
  }

  /**
   * Writes real nested Parquet timestamp annotations.
   *
   * @param directory
   *          database directory
   * @param instant
   *          coarser timestamp value
   * @param nanos
   *          nanosecond timestamp value
   * @throws Exception
   *           if writing the fixture fails
   */
  private static void writeTimestamps(Path directory, Instant instant, long nanos) throws Exception {
    var schema = MessageTypeParser.parseMessageType("""
        message timestamps {
          required group valid_to { required int64 x (TIMESTAMP(MICROS,true)); }
          required group loaded { required int64 x (TIMESTAMP(NANOS,true)); }
        }
        """);
    var row = new SimpleGroupFactory(schema).newGroup();
    row.addGroup("valid_to").add("x", instant.getEpochSecond() * 1_000_000L);
    row.addGroup("loaded").add("x", nanos);
    var config = new org.apache.hadoop.conf.Configuration();
    var file = new org.apache.hadoop.fs.Path(directory.resolve("t.parquet").toUri());
    try (var writer = ExampleParquetWriter.builder(file).withConf(config).withType(schema).build()) {
      writer.write(row);
    }
  }

  /**
   * Decodes a STRUCT field and checks that its SQL type remains TIMESTAMP.
   *
   * @param record
   *          normalized record
   * @return decoded field value
   */
  private static Object field(IndexedRecord record) {
    Schema schema = record.getSchema().getFields().getFirst().schema();
    assertEquals(Types.TIMESTAMP, JdbcTypeMapper.mapSchemaToJdbcType(schema));
    return AvroCoercions.unwrap(record.get(0), schema);
  }

  /**
   * Creates a stored timestamp STRUCT.
   *
   * @param name
   *          record name
   * @param type
   *          timestamp declaration
   * @param value
   *          raw epoch units
   * @return timestamp record
   */
  private static GenericData.Record record(String name, Schema type, long value) {
    Schema schema = SchemaBuilder.record(name).fields().name("x").type(type).noDefault().endRecord();
    var record = new GenericData.Record(schema);
    record.put(0, value);
    return record;
  }
}
