package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.engine.function.JsonFunctions;

/** Regression coverage for temporal SQL/JSON constructor values. */
@Isolated("Changes the JVM default time zone")
class JsonTemporalSerializationTest {
  @TempDir
  Path directory;

  /**
   * Includes seconds in whole-minute times and retains computed local and offset
   * values with nanosecond precision.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void serializesComputedTemporalValues() throws Exception {
    List<Object> values = List.of(LocalTime.of(1, 0), LocalDateTime.parse("2024-01-02T03:04:05.123456789"),
        OffsetTime.parse("03:04:05.123456789+02:00"), OffsetDateTime.parse("2024-01-02T03:04:05.123456789+02:00"),
        OffsetTime.parse("01:00:00+02:00"), OffsetDateTime.parse("2024-01-02T03:04:00+02:00"));
    String expected = "[\"01:00:00\",\"2024-01-02T03:04:05.123456789\","
        + "\"03:04:05.123456789+02:00\",\"2024-01-02T03:04:05.123456789+02:00\","
        + "\"01:00:00+02:00\",\"2024-01-02T03:04:00+02:00\"]";
    assertEquals(expected, JsonFunctions.jsonArray(values));
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT JSON_ARRAY(TIME '01:00:00')")) {
      assertTrue(result.next());
      assertEquals("[\"01:00:00\"]", result.getString(1));
      assertFalse(result.next());
    }
  }

  /**
   * Keeps non-temporal large-object arguments valid during temporal schema
   * resolution.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void preservesLargeObjectArguments() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT JSON_ARRAY(CAST('hello' AS CLOB), "
            + "CAST(X'0102' AS BLOB), CASE WHEN TRUE THEN CAST('world' AS CLOB) ELSE NULL END)")) {
      assertTrue(result.next());
      assertEquals("[\"hello\",\"AQI=\",\"world\"]", result.getString(1));
      assertFalse(result.next());
    }
  }

  /**
   * Uses ISO text and preserves UTC versus local timestamp semantics in every
   * container and through query materialization, independently of the host zone.
   *
   * @param zone
   *          JVM default time zone
   * @throws Exception
   *           if fixture writing or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UTC", "Europe/Stockholm", "America/New_York"
  })
  void serializesTemporalValues(String zone) throws Exception {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      List<Schema> types = List.of(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)),
          LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)),
          LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
          LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
          LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG)),
          LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
          LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
          LogicalTypes.localTimestampNanos().addToSchema(Schema.create(Schema.Type.LONG)));
      List<Number> values = List.of(3_600_123, 3_600_123_000L, 1_700_000_000_123L, 1_700_000_000_123_456L, -1L,
          1_700_000_000_123L, 1_700_000_000_123_456L, -1L);
      List<String> expected = List.of("01:00:00.123", "01:00:00.123", "2023-11-14T22:13:20.123Z",
          "2023-11-14T22:13:20.123456Z", "1969-12-31T23:59:59.999999999Z", "2023-11-14T22:13:20.123",
          "2023-11-14T22:13:20.123456", "1969-12-31T23:59:59.999999999");
      for (int index = 0; index < types.size(); index++) {
        writeValues("t" + index, types.get(index), values.get(index));
      }
      assertNullFirstDeclarations();
      for (boolean pushdown : List.of(true, false)) {
        try (var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?projectionPushdown=" + pushdown + "&predicatePushdown=" + pushdown)) {
          for (int index = 0; index < types.size(); index++) {
            String scalar = "\"" + expected.get(index) + "\"";
            for (String[] sample : List.of(new String[]{
                "v", scalar
            }, new String[]{
                "a", "[" + scalar + ",null]"
            }, new String[]{
                "r", "{\"v\":" + scalar + "}"
            }, new String[]{
                "ARRAY[v,NULL]", "[" + scalar + ",null]"
            })) {
              String field = sample[0].startsWith("ARRAY") ? "v" : sample[0];
              for (String input : List.of("t" + index, "(SELECT " + field + " FROM t" + index + ") d",
                  "(WITH c AS (SELECT " + field + " FROM t" + index + ") SELECT * FROM c) d")) {
                if ("a".equals(field) && index >= 3 && input.startsWith("(")) {
                  // Physical timestamp ARRAY re-encoding fails before JSON (#517).
                  continue;
                }
                if ("v".equals(field) && (index == 3 || index == 4) && input.startsWith("(")) {
                  // UTC physical scalar materialization loses precision (#373).
                  continue;
                }
                String sql = "SELECT JSON_ARRAY(" + sample[0] + "), JSON_OBJECT(KEY 'k' VALUE " + sample[0] + ") FROM "
                    + input;
                try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
                  assertTrue(result.next(), sql);
                  assertEquals("[" + sample[1] + "]", result.getString(1), zone + ": " + sql);
                  assertEquals("{\"k\":" + sample[1] + "}", result.getString(2), zone + ": " + sql);
                  assertFalse(result.next(), sql);
                }
              }
            }
          }
          assertLocalExpressions(connection);
        }
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  /**
   * Does not cache an unknown declaration merely because the first row is NULL.
   * Also exercises nullable schemas and unquoted uppercase field references.
   *
   * @throws Exception
   *           if expression parsing or evaluation fails
   */
  private static void assertNullFirstDeclarations() throws Exception {
    for (boolean local : List.of(false, true)) {
      Schema timestamp = (local ? LogicalTypes.localTimestampMillis() : LogicalTypes.timestampMillis())
          .addToSchema(Schema.create(Schema.Type.LONG));
      Schema schema = SchemaBuilder.record("nullableTimestamp").fields().name("v")
          .type(Schema.createUnion(Schema.create(Schema.Type.NULL), timestamp)).noDefault().endRecord();
      var record = new GenericData.Record(schema);
      var evaluator = new ValueExpressionEvaluator(schema);
      var expression = net.sf.jsqlparser.parser.CCJSqlParserUtil.parseExpression("JSON_ARRAY(V)");
      assertEquals("[null]", evaluator.eval(expression, record));
      record.put("v", 1_700_000_000_123L);
      String expected = "[\"2023-11-14T22:13:20.123" + (local ? "" : "Z") + "\"]";
      assertEquals(expected, evaluator.eval(expression, record));
      assertEquals(expected, evaluator.eval(expression, record));
    }
  }

  /**
   * Checks literals, casts, arithmetic, keywords, identifier folding, scalar
   * subqueries, and correlated references without UTC shifts or invented zones.
   *
   * @param connection
   *          open test database connection
   * @throws Exception
   *           if query execution or JSON parsing fails
   */
  private static void assertLocalExpressions(Connection connection) throws Exception {
    String local = "2023-11-14T22:13:20.123";
    for (String[] sample : List.of(new String[]{
        "V", local
    }, new String[]{
        "CAST(v AS TIMESTAMP)", local
    }, new String[]{
        "CAST(v AS TIMESTAMP WITHOUT TIME ZONE)", local
    }, new String[]{
        "v + INTERVAL '1' DAY", "2023-11-15T22:13:20.123"
    }, new String[]{
        "v - INTERVAL '1' DAY", "2023-11-13T22:13:20.123"
    }, new String[]{
        "(SELECT v FROM t5)", local
    }, new String[]{
        "TIMESTAMP '2024-01-02 03:04:05'", "2024-01-02T03:04:05"
    }, new String[]{
        "CAST('2024-01-02 03:04:05' AS TIMESTAMP)", "2024-01-02T03:04:05"
    }, new String[]{
        "TIME '01:02:03.120'", "01:02:03.120"
    }, new String[]{
        "CAST('01:02:03.120' AS TIME(3))", "01:02:03.120"
    })) {
      String expected = "\"" + sample[1] + "\"";
      for (String expression : List.of(sample[0], "ARRAY[" + sample[0] + "]")) {
        String value = expression.startsWith("ARRAY") ? "[" + expected + "]" : expected;
        try (var statement = connection.createStatement();
            var result = statement.executeQuery(
                "SELECT JSON_ARRAY(" + expression + "), JSON_OBJECT(KEY 'k' VALUE " + expression + ") FROM t5")) {
          assertTrue(result.next(), expression);
          assertEquals("[" + value + "]", result.getString(1), expression);
          assertEquals("{\"k\":" + value + "}", result.getString(2), expression);
          assertFalse(result.next(), expression);
        }
      }
    }
    for (String sql : List.of("SELECT JSON_ARRAY(x) FROM (SELECT v AS x FROM t5) d",
        "WITH c AS (SELECT v AS x FROM t5) SELECT JSON_ARRAY(x) FROM c",
        "SELECT (SELECT JSON_ARRAY(o.v) FROM t0) FROM t5 o")) {
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
        assertTrue(result.next(), sql);
        assertEquals("[\"" + local + "\"]", result.getString(1), sql);
        assertFalse(result.next(), sql);
      }
    }
    try (var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT JSON_ARRAY(LOCALTIMESTAMP), " + "JSON_OBJECT(KEY 'k' VALUE LOCALTIME) FROM t5")) {
      assertTrue(result.next());
      var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
      LocalDateTime.parse(mapper.readTree(result.getString(1)).get(0).asText());
      LocalTime.parse(mapper.readTree(result.getString(2)).get("k").asText());
      assertFalse(result.next());
    }
  }

  /**
   * Writes scalars and nested temporal values with nullable array elements.
   *
   * @param name
   *          table name
   * @param type
   *          temporal logical type
   * @param value
   *          stored clock or epoch units
   * @throws Exception
   *           if fixture writing fails
   */
  private void writeValues(String name, Schema type, Number value) throws Exception {
    Schema detail = SchemaBuilder.record(name + "Detail").fields().name("v").type(type).noDefault().endRecord();
    Schema schema = SchemaBuilder.record(name).fields().name("v").type(type).noDefault().name("a")
        .type(Schema.createArray(Schema.createUnion(Schema.create(Schema.Type.NULL), type))).noDefault().name("r")
        .type(detail).noDefault().endRecord();
    var nested = new GenericData.Record(detail);
    nested.put("v", value);
    var row = new GenericData.Record(schema);
    row.put("v", value);
    row.put("a", Arrays.asList(value, null));
    row.put("r", nested);
    Configuration configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve(name + ".parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      writer.write(row);
    }
  }
}
