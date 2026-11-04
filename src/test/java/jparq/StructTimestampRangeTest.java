package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.function.ArrayFunctions;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/**
 * Verifies full-range, lossless STRUCT timestamp combinations with nanosecond
 * fields.
 */
class StructTimestampRangeTest {
  private static final long LOADED = 1_700_000_000_123_456_789L;
  private static final LocalDateTime LOADED_VALUE = LocalDateTime
      .ofInstant(Instant.ofEpochSecond(1_700_000_000L, 123_456_789L), ZoneOffset.UTC);

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
    final var expected = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    for (boolean local : List.of(false, true)) {
      Schema coarseSchema = (local ? LogicalTypes.localTimestampMicros() : LogicalTypes.timestampMicros())
          .addToSchema(Schema.create(Schema.Type.LONG));
      Schema nanosSchema = (local ? LogicalTypes.localTimestampNanos() : LogicalTypes.timestampNanos())
          .addToSchema(Schema.create(Schema.Type.LONG));
      var coarse = record("coarse", coarseSchema, instant.getEpochSecond() * 1_000_000L);
      var fine = record("fine", nanosSchema, LOADED);
      for (var pair : List.of(List.of(coarse, fine), List.of(fine, coarse))) {
        var result = ArrayFunctions.homogenizeArrayValues(new ArrayList<>(pair));
        for (int index = 0; index < pair.size(); index++) {
          assertEquals(pair.get(index) == coarse ? expected : LOADED_VALUE, field((IndexedRecord) result.get(index)));
        }
        var combined = ArrayFunctions.homogenizeArrayValues(List.of(result.getFirst(), coarse, fine));
        assertEquals(expected, field((IndexedRecord) combined.get(1)));
        assertEquals(LOADED_VALUE, field((IndexedRecord) combined.get(2)));
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
    final var expected = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    var schema = MessageTypeParser.parseMessageType("""
        message timestamps {
          required group valid_to { required int64 x (TIMESTAMP(MICROS,true)); }
          required group loaded { required int64 x (TIMESTAMP(NANOS,true)); }
        }
        """);
    var row = new SimpleGroupFactory(schema).newGroup();
    row.addGroup("valid_to").add("x", instant.getEpochSecond() * 1_000_000L);
    row.addGroup("loaded").add("x", LOADED);
    var config = new org.apache.hadoop.conf.Configuration();
    var file = new org.apache.hadoop.fs.Path(directory.resolve("t.parquet").toUri());
    try (var writer = ExampleParquetWriter.builder(file).withConf(config).withType(schema).build()) {
      writer.write(row);
    }
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
            assertEquals(LOADED_VALUE, field((IndexedRecord) records[1 - coarse]), sql);
          }
        }
        try (var statement = connection.createStatement();
            var result = statement
                .executeQuery("SELECT u.x FROM t CROSS JOIN UNNEST(ARRAY[t.valid_to,t.loaded]) AS u(x)")) {
          assertTrue(result.next());
          assertEquals(Types.TIMESTAMP, result.getMetaData().getColumnType(1));
          assertEquals(expected, result.getObject(1));
          assertTrue(result.next());
          assertEquals(LOADED_VALUE, result.getObject(1));
        }
      }
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
