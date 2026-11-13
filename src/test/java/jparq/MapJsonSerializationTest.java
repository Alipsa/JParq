package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression coverage for decoding physical MAP values consistently with the
 * values inside struct fields (issue #313).
 */
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
          assertJson(connection, function, "m", " FROM " + input + " WHERE id = 1", "{\"d\":\"2024-01-01\"}");
          assertJson(connection, function, "am", " FROM " + input + " WHERE id = 1", "[{\"d\":\"2024-01-01\"},null]");
          assertJson(connection, function, "dm", " FROM " + input + " WHERE id = 1", "{\"p\":1.50}");
          assertJson(connection, function, "tm", " FROM " + input + " WHERE id = 1",
              "{\"t\":\"2023-11-14T22:13:20.123Z\"}");
          assertJson(connection, function, "sm", " FROM " + input + " WHERE id = 1", "{\"md\":{\"d\":\"2024-01-01\"}}");
          assertJson(connection, function, "cm", " FROM " + input + " WHERE id = 1", "{\"n\":[1,null,2]}");
          assertJson(connection, function, "nm", " FROM " + input + " WHERE id = 2", "null");
        }
      }
    }
  }

  /**
   * Materializes map columns through derived tables and CTEs, decoding their
   * values for JDBC getters and preserving NULL maps.
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
    try (var connection = DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?projectionPushdown=" + pushdown + "&predicatePushdown=" + pushdown)) {
      for (String input : List.of("maps", "(SELECT * FROM maps) a",
          "(WITH c AS (SELECT * FROM maps) SELECT * FROM c) a")) {
        String sql = "SELECT m, am, nm FROM " + input + " WHERE id = 1";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(expected, result.getObject(1), sql);
          Object[] arrays = assertInstanceOf(Object[].class, result.getObject(2), sql);
          assertEquals(2, arrays.length, sql);
          assertEquals(expected, arrays[0], sql);
          assertNull(arrays[1], sql);
          assertEquals(expected, result.getObject(3), sql);
          assertFalse(result.next(), sql);
        }
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT nm FROM " + input + " WHERE id = 2")) {
          assertTrue(result.next());
          assertNull(result.getObject(1));
          assertFalse(result.next());
        }
      }
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
    Schema timestamp = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema nullableDate = Schema.createUnion(Schema.create(Schema.Type.NULL), date);
    Schema nullableInteger = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    Schema mapDates = Schema.createMap(date);
    Schema nullableMapDates = Schema.createUnion(Schema.create(Schema.Type.NULL), mapDates);
    Schema structMaps = SchemaBuilder.record("structMaps").fields().name("md").type(mapDates).noDefault().endRecord();
    Schema schema = SchemaBuilder.record("maps").fields().requiredInt("id").name("m").type(nullableMapDates).noDefault()
        .name("am")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL),
            Schema.createArray(Schema.createUnion(Schema.create(Schema.Type.NULL), mapDates))))
        .noDefault().name("dm").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(decimal)))
        .noDefault().name("tm").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(timestamp)))
        .noDefault().name("sm").type(Schema.createUnion(Schema.create(Schema.Type.NULL), structMaps)).noDefault()
        .name("cm")
        .type(
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(Schema.createArray(nullableInteger))))
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
      var struct = new GenericData.Record(structMaps);
      struct.put("md", dates);
      row.put("sm", struct);
      row.put("cm", Map.of("n", Arrays.asList(1, null, 2)));
      row.put("nm", dates);
      writer.write(row);
      var nulls = new GenericData.Record(schema);
      nulls.put("id", 2);
      for (String field : List.of("m", "am", "dm", "tm", "sm", "cm", "nm")) {
        nulls.put(field, null);
      }
      writer.write(nulls);
    }
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
