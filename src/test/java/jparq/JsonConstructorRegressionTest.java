package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/**
 * Exercises constructor regressions involving source values and promoted types.
 */
@Isolated("Changes the JVM default time zone")
class JsonConstructorRegressionTest {
  @TempDir
  Path directory;

  /**
   * Retains direct JSON text, correct arithmetic result types, UTC provenance,
   * and local fields that fall inside a daylight-saving gap.
   *
   * @param zone
   *          host time zone
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UTC", "Europe/Stockholm", "America/New_York"
  })
  void preservesConstructorValues(String zone) throws Exception {
    TimeZone original = TimeZone.getDefault();
    String previousProperty = System.getProperty("user.timezone");
    try {
      System.setProperty("user.timezone", zone);
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      writeFixture();
      for (boolean pushdown : List.of(true, false)) {
        try (var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?projectionPushdown=" + pushdown + "&predicatePushdown=" + pushdown)) {
          assertValue(connection, "j", " FROM t", "{\"a\":1}");
          assertValue(connection, "'{\"a\":1}'", "", "{\"a\":1}");
          assertValue(connection, "'[1,2]'", "", "[1,2]");
          assertValue(connection, "COALESCE(j,'{}')", " FROM t", "{\"a\":1}");
          assertValue(connection, "CASE WHEN TRUE THEN j ELSE '{}' END", " FROM t", "{\"a\":1}");
          assertValue(connection, "CASE WHEN TRUE THEN ARRAY[1] ELSE ARRAY[CAST(2 AS DOUBLE)] END", "", "[1]");
          assertValue(connection, "COALESCE(ARRAY[1],ARRAY[CAST(2 AS DOUBLE)])", "", "[1]");
          assertValue(connection, "COALESCE(ARRAY[1],ARRAY[CAST(2 AS DECIMAL(8,2))])", "", "[1]");
          assertValue(connection, "ARRAY[j]", " FROM t", "[\"{\\\"a\\\":1}\"]");
          assertValue(connection, "lts", " FROM t", "\"2024-03-31T02:30:00\"");
          assertValue(connection, "LTS", " FROM t", "\"2024-03-31T02:30:00\"");
          assertValue(connection, "la", " FROM t", "[\"2024-03-31T02:30:00\"]");
          for (String expression : List.of("COALESCE(ts,CURRENT_TIMESTAMP)",
              "CASE WHEN TRUE THEN ts ELSE CURRENT_TIMESTAMP END",
              "CASE 1 WHEN 1 THEN ts ELSE CURRENT_TIMESTAMP END")) {
            assertValue(connection, expression, " FROM t", "\"2024-07-01T12:00:00Z\"");
            assertValue(connection, "ARRAY[" + expression + "]", " FROM t", "[\"2024-07-01T12:00:00Z\"]");
          }
          String localElement = "2000-01-01T01:00:00";
          String utcWall = DateTimeFormatter.ISO_LOCAL_DATE_TIME
              .format(Timestamp.from(Instant.parse("2024-07-01T12:00:00Z")).toLocalDateTime());
          for (String expression : List.of("COALESCE(ts,TIMESTAMP '2000-01-01 01:00:00')",
              "COALESCE(ts,CAST('invalid' AS TIMESTAMP))",
              "CASE WHEN TRUE THEN ts ELSE TIMESTAMP '2000-01-01 01:00:00' END",
              "CASE WHEN FALSE THEN TIMESTAMP '2000-01-01 01:00:00' ELSE ts END")) {
            assertValue(connection, expression, " FROM t", "\"" + utcWall + "\"");
            assertValue(connection, "ARRAY[" + expression + "]", " FROM t", "[\"" + utcWall + "\"]");
          }
          assertValue(connection, "COALESCE(NULLIF(ts,ts),TIMESTAMP '2000-01-01 01:00:00')", " FROM t",
              "\"" + localElement + "\"");
          assertValue(connection, "ARRAY[ts,TIMESTAMP '2000-01-01 01:00:00']", " FROM t",
              "[\"" + utcWall + "\",\"" + localElement + "\"]");
          assertValue(connection, "ARRAY[null_ts,TIMESTAMP '2000-01-01 01:00:00']", " FROM t",
              "[null,\"" + localElement + "\"]");
          assertValue(connection, "ARRAY[lts]", " FROM t", "[\"2024-03-31T02:30:00\"]");
          final String offsetGap = DateTimeFormatter.ISO_OFFSET_DATE_TIME
              .format(DateTimeFunctions.offsetTimestamp(LocalDateTime.parse("2024-03-31T02:30:00")));
          String nested = "[[\"2024-03-31T02:30:00\"],[\"" + utcWall + "\"]]";
          assertValue(connection, "ARRAY[la,ARRAY[ts]]", " FROM t", nested);
          assertValue(connection, "ARRAY[ARRAY[lts],ARRAY[ts]]", " FROM t", nested);
          try (var statement = connection.createStatement();
              var result = statement.executeQuery("SELECT JSON_ARRAY(la,ts) FROM t")) {
            assertTrue(result.next());
            assertEquals("[[\"2024-03-31T02:30:00\"],\"2024-07-01T12:00:00Z\"]", result.getString(1));
            assertFalse(result.next());
          }
          assertEngineArrayComparison(connection, utcWall);
          assertScalarAndSetConsistency(connection, utcWall);
          assertSummerInstantPromotion(connection);
          assertWrappedUtcConditional(connection);
          assertValue(connection, "ARRAY[nla]", " FROM t", "[[\"2024-03-31T02:30:00\",null]]");
          String storedMixed = "[[\"2024-03-31T02:30:00\",null],[\"" + utcWall + "\",null]]";
          assertValue(connection, "ARRAY[nla,ua]", " FROM t", storedMixed);
          assertValue(connection, "ARRAY[nla,ARRAY(SELECT ts FROM t)]", " FROM t",
              "[[\"2024-03-31T02:30:00\",null],[\"" + utcWall + "\"]]");
          String scalarMixed = "[\"" + utcWall + "\",\"2024-03-31T02:30:00\"]";
          assertValue(connection, "ARRAY[ts,lts]", " FROM t", scalarMixed);
          for (String expression : List.of("COALESCE(lts,CURRENT_TIMESTAMP)",
              "CASE WHEN TRUE THEN lts ELSE CURRENT_TIMESTAMP END")) {
            assertValue(connection, expression, " FROM t", "\"" + offsetGap + "\"");
            assertValue(connection, "ARRAY[" + expression + "]", " FROM t", "[\"" + offsetGap + "\"]");
          }
          assertValue(connection, "d + INTERVAL '1' HOUR", " FROM t", "\"2024-01-02T01:00:00\"");
          assertConditionalCompatibility(connection);
          try (var statement = connection.createStatement();
              var result = statement.executeQuery("SELECT JSON_ARRAY(ARRAY[lts,IFNULL(NULL,CURRENT_TIMESTAMP)]), "
                  + "JSON_ARRAY(ARRAY[COALESCE(lts,CURRENT_TIMESTAMP),CURRENT_TIMESTAMP]), "
                  + "JSON_ARRAY((SELECT LOCALTIMESTAMP)),JSON_ARRAY(LOCALTIMESTAMP) FROM t")) {
            assertTrue(result.next());
            assertEquals(result.getString(2), result.getString(1));
            assertEquals(result.getString(4), result.getString(3));
            assertFalse(result.next());
          }
          assertValue(connection, "COALESCE(ts,DATE '2024-01-01')", " FROM t", "\"2024-07-01T12:00:00Z\"");
          for (String sql : List.of("SELECT ARRAY[CURRENT_TIMESTAMP,IFNULL(NULL,1)]",
              "SELECT JSON_ARRAY(ARRAY[CURRENT_TIMESTAMP,IFNULL(NULL,1)])",
              "SELECT CASE WHEN TRUE THEN ARRAY[CURRENT_TIMESTAMP,IFNULL(NULL,1)] "
                  + "ELSE ARRAY[CURRENT_TIMESTAMP,CURRENT_TIMESTAMP] END",
              "SELECT JSON_ARRAY(CASE WHEN TRUE THEN ARRAY[CURRENT_TIMESTAMP,IFNULL(NULL,1)] "
                  + "ELSE ARRAY[CURRENT_TIMESTAMP,CURRENT_TIMESTAMP] END)")) {
            try (var statement = connection.createStatement()) {
              SQLException failure = assertThrows(SQLException.class, () -> {
                try (var result = statement.executeQuery(sql)) {
                  result.next();
                  result.getObject(1);
                }
              }, sql);
              assertEquals("42000", failure.getSQLState(), sql);
            }
          }
          for (String[] sample : List.of(new String[]{
              "0", "2024-01-02T00:00:00"
          }, new String[]{
              "24", "2024-01-03T00:00:00"
          })) {
            assertValue(connection, "DATE '2024-01-02' + INTERVAL '" + sample[0] + "' HOUR", "",
                "\"" + sample[1] + "\"");
          }
          for (String expression : List.of("COALESCE(INTERVAL '1' DAY,INTERVAL '2' HOUR)",
              "CASE WHEN TRUE THEN INTERVAL '1' DAY ELSE INTERVAL '2' HOUR END")) {
            try (var statement = connection.createStatement();
                var result = statement
                    .executeQuery("SELECT JSON_ARRAY(" + expression + "), CAST(" + expression + " AS VARCHAR)")) {
              assertTrue(result.next(), expression);
              assertEquals("[\"" + result.getString(2) + "\"]", result.getString(1), expression);
              assertFalse(result.next(), expression);
            }
          }
          for (String[] sample : List.of(new String[]{
              "HOUR", "2024-01-02T01:00:00"
          }, new String[]{
              "MINUTE", "2024-01-02T00:01:00"
          }, new String[]{
              "SECOND", "2024-01-02T00:00:01"
          }, new String[]{
              "DAY", "2024-01-03"
          })) {
            String arithmetic = "DATE '2024-01-02' + INTERVAL '1' " + sample[0];
            String expected = "\"" + sample[1] + "\"";
            assertValue(connection, arithmetic, "", expected);
            assertValue(connection, "ARRAY[" + arithmetic + "]", "", "[" + expected + "]");
            assertValue(connection, "a", " FROM (SELECT ARRAY[" + arithmetic + "] AS a) d", "[" + expected + "]");
            assertValue(connection, "a", " FROM (WITH c AS (SELECT ARRAY[" + arithmetic + "] AS a) SELECT a FROM c) d",
                "[" + expected + "]");
          }
        }
      }
    } finally {
      TimeZone.setDefault(original);
      if (previousProperty == null) {
        System.clearProperty("user.timezone");
      } else {
        System.setProperty("user.timezone", previousProperty);
      }
    }
  }

  /**
   * Keeps conditional DATE/TIMESTAMP combinations accepted while the shared
   * datetime promotion fix is tracked in #431, and follows shared numeric
   * COALESCE evaluation so a future #524 fix is inherited automatically.
   *
   * @param connection
   *          open test connection
   * @throws Exception
   *           if query execution fails
   */
  private static void assertConditionalCompatibility(Connection connection) throws Exception {
    for (String expression : List.of("COALESCE(CAST(NULL AS TIMESTAMP),DATE '2024-01-01')",
        "CASE WHEN FALSE THEN TIMESTAMP '2024-01-02 03:04:05' ELSE DATE '2024-01-01' END",
        "COALESCE(DATE '2024-01-01',CURRENT_TIMESTAMP)",
        "CASE WHEN TRUE THEN DATE '2024-01-01' ELSE CURRENT_TIMESTAMP END")) {
      assertValue(connection, expression, "", "\"2024-01-01\"");
    }
    assertValue(connection, "CASE WHEN TRUE THEN TIMESTAMP '2024-01-02 03:04:05' ELSE DATE '2024-01-01' END", "",
        "\"2024-01-02T03:04:05\"");
    String choice = "CASE WHEN TRUE THEN DATE '2024-01-01' ELSE TIMESTAMP '2024-01-02 03:04:05' END";
    String coalesce = "COALESCE(CAST(NULL AS TIMESTAMP),DATE '2024-01-01')";
    for (String conditional : List.of(choice, coalesce)) {
      String arithmetic = "(" + conditional + ") + INTERVAL '1' DAY";
      assertValue(connection, arithmetic, " FROM t", "\"2024-01-02\"");
      assertValue(connection, "INTERVAL '1' DAY + (" + conditional + ")", " FROM t", "\"2024-01-02\"");
      assertValue(connection, "CAST(" + arithmetic + " AS DATE)", " FROM t", "\"2024-01-02\"");
      assertValue(connection, "CAST(" + arithmetic + " AS TIMESTAMP)", " FROM t", "\"2024-01-02T00:00:00\"");
      assertValue(connection, "CAST(" + arithmetic + " AS VARCHAR(30))", " FROM t", "\"2024-01-02\"");
    }
    try (var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT JSON_ARRAY(COALESCE(id,1.5)),CAST(COALESCE(id,1.5) AS VARCHAR(20)) FROM t")) {
      assertTrue(result.next());
      assertEquals("[" + result.getString(2) + "]", result.getString(1));
      assertFalse(result.next());
    }
  }

  /**
   * Requires JSON, JDBC, character casts, and materialization to retain the same
   * promoted ARRAY elements and instants.
   *
   * @param connection
   *          open test connection
   * @param utcWall
   *          UTC column's JDBC TIMESTAMP wall-clock fields
   * @throws Exception
   *           if query execution fails
   */
  private static void assertEngineArrayComparison(Connection connection, String utcWall) throws Exception {
    String arrayExpression = "ARRAY[ts,TIMESTAMP '2000-01-01 01:00:00']";
    for (String[] shape : List.of(new String[]{
        arrayExpression, " FROM t"
    }, new String[]{
        "x", " FROM (SELECT " + arrayExpression + " AS x FROM t) d"
    }, new String[]{
        "x", " FROM (WITH c AS (SELECT " + arrayExpression + " AS x FROM t) SELECT x FROM c) d"
    })) {
      String expression = shape[0];
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT JSON_ARRAY(" + expression + "),CAST(" + expression
              + " AS VARCHAR(100))," + expression + shape[1])) {
        assertTrue(result.next());
        assertEquals("[[\"" + utcWall + "\",\"2000-01-01T01:00:00\"]]", result.getString(1));
        var array = result.getArray(3);
        try {
          Object[] elements = (Object[]) array.getArray();
          Timestamp utc = assertInstanceOf(Timestamp.class, elements[0]);
          Timestamp local = assertInstanceOf(Timestamp.class, elements[1]);
          assertEquals(LocalDateTime.parse(utcWall), utc.toLocalDateTime());
          assertEquals(LocalDateTime.parse("2000-01-01T01:00:00"), local.toLocalDateTime());
          assertEquals(java.sql.Types.TIMESTAMP, array.getBaseType());
          assertEquals(Arrays.toString(elements), result.getString(2));
        } finally {
          array.free();
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Preserves selected UTC provenance through nested conditionals and arithmetic.
   *
   * @param connection
   *          open test connection
   * @throws Exception
   *           if query execution fails
   */
  private static void assertWrappedUtcConditional(Connection connection) throws Exception {
    for (String conditional : List.of("CASE WHEN TRUE THEN ts ELSE DATE '2024-01-01' END",
        "COALESCE(ts,DATE '2024-01-01')")) {
      assertValue(connection, "(" + conditional + ") + INTERVAL '1' HOUR", " FROM t", "\"2024-07-01T13:00:00Z\"");
      assertValue(connection, "INTERVAL '1' HOUR + (" + conditional + ")", " FROM t", "\"2024-07-01T13:00:00Z\"");
      assertValue(connection, "((" + conditional + ") + INTERVAL '2' HOUR) - INTERVAL '1' HOUR", " FROM t",
          "\"2024-07-01T13:00:00Z\"");
    }
  }

  /**
   * Compares scalar and ARRAY combinations across JDBC, JSON, and
   * materialization.
   *
   * @param connection
   *          open connection
   * @param utcWall
   *          UTC column's JDBC wall-clock fields
   * @throws Exception
   *           if a query fails
   */
  private static void assertScalarAndSetConsistency(Connection connection, String utcWall) throws Exception {
    for (String expression : List.of("COALESCE(ts,wall)", "COALESCE(null_ts,wall)",
        "COALESCE(null_ts,TIMESTAMP '2024-01-02 03:04:05')", "CASE WHEN TRUE THEN ts ELSE wall END",
        "CASE WHEN FALSE THEN ts ELSE wall END")) {
      String expected = expression.equals("COALESCE(ts,wall)") || expression.contains("TRUE")
          ? utcWall
          : "2024-01-02T03:04:05";
      for (String[] shape : List.of(new String[]{
          expression, " FROM t"
      }, new String[]{
          "x", " FROM (SELECT " + expression + " AS x FROM t) d"
      }, new String[]{
          "x", " FROM (WITH c AS (SELECT " + expression + " AS x FROM t) SELECT x FROM c) d"
      })) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery(
                "SELECT JSON_ARRAY(" + shape[0] + "),CAST(" + shape[0] + " AS VARCHAR(100))," + shape[0] + shape[1])) {
          assertTrue(result.next(), expression);
          Timestamp value = assertInstanceOf(Timestamp.class, result.getObject(3), expression);
          assertEquals(java.sql.Types.TIMESTAMP, result.getMetaData().getColumnType(3), expression);
          assertEquals(LocalDateTime.parse(expected), value.toLocalDateTime(), expression);
          assertEquals("[\"" + expected + "\"]", result.getString(1), expression);
          assertEquals(value.toString(), result.getString(2), expression);
          assertFalse(result.next());
        }
      }
    }
    for (String expression : List.of("COALESCE(ARRAY[ts],ARRAY[wall])",
        "CASE WHEN TRUE THEN ARRAY[ts] ELSE ARRAY[wall] END")) {
      for (String[] shape : List.of(new String[]{
          expression, " FROM t"
      }, new String[]{
          "x", " FROM (SELECT " + expression + " AS x FROM t) d"
      })) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT JSON_ARRAY(" + shape[0] + ")," + shape[0] + shape[1])) {
          assertTrue(result.next());
          assertEquals("[[\"" + utcWall + "\"]]", result.getString(1));
          var array = result.getArray(2);
          try {
            assertEquals(java.sql.Types.TIMESTAMP, array.getBaseType());
            Timestamp value = assertInstanceOf(Timestamp.class, ((Object[]) array.getArray())[0]);
            assertEquals(LocalDateTime.parse(utcWall), value.toLocalDateTime());
          } finally {
            array.free();
          }
        }
      }
    }
    for (boolean array : List.of(false, true)) {
      String first = array ? "ARRAY[ts]" : "ts";
      String second = array ? "ARRAY[wall]" : "wall";
      String union = "SELECT " + first + " AS x FROM t UNION ALL SELECT " + second + " AS x FROM t";
      for (String query : List.of("SELECT x FROM (" + union + ") d", "WITH c AS (" + union + ") SELECT x FROM c")) {
        String source = " FROM (" + query + ") s";
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT JSON_ARRAY(x),x" + source)) {
          for (String expected : List.of(utcWall, "2024-01-02T03:04:05")) {
            assertTrue(result.next(), query);
            String value = "\"" + expected + "\"";
            assertEquals("[" + (array ? "[" + value + "]" : value) + "]", result.getString(1), query);
            if (array) {
              var elements = result.getArray(2);
              try {
                assertEquals(java.sql.Types.TIMESTAMP, elements.getBaseType());
                assertInstanceOf(Timestamp.class, ((Object[]) elements.getArray())[0]);
              } finally {
                elements.free();
              }
            } else {
              assertEquals(java.sql.Types.TIMESTAMP, result.getMetaData().getColumnType(2));
              assertInstanceOf(Timestamp.class, result.getObject(2));
            }
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Preserves a summer UTC instant when an actual WITH TIME ZONE operand is
   * present.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if a query fails
   */
  private static void assertSummerInstantPromotion(Connection connection) throws Exception {
    for (String expression : List.of("COALESCE(ts,CURRENT_TIMESTAMP)", "CAST(ts AS TIMESTAMP WITH TIME ZONE)",
        "CASE WHEN TRUE THEN ts ELSE CURRENT_TIMESTAMP END")) {
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT " + expression + ",JSON_ARRAY(" + expression + ") FROM t")) {
        assertTrue(result.next());
        OffsetDateTime value = assertInstanceOf(OffsetDateTime.class, result.getObject(1));
        assertEquals(Instant.parse("2024-07-01T12:00:00Z"), value.toInstant());
        assertEquals(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, result.getMetaData().getColumnType(1));
        assertEquals("[\"2024-07-01T12:00:00Z\"]", result.getString(2));
      }
    }
    for (boolean array : List.of(false, true)) {
      String first = array ? "ARRAY[ts]" : "ts";
      String second = array ? "ARRAY[CURRENT_TIMESTAMP]" : "CURRENT_TIMESTAMP";
      String union = "SELECT " + first + " AS x FROM t UNION ALL SELECT " + second + " AS x FROM t";
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT x,JSON_ARRAY(x) FROM (" + union + ") d")) {
        assertTrue(result.next());
        OffsetDateTime value;
        if (array) {
          var values = result.getArray(1);
          try {
            assertEquals(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, values.getBaseType());
            value = assertInstanceOf(OffsetDateTime.class, ((Object[]) values.getArray())[0]);
          } finally {
            values.free();
          }
        } else {
          assertEquals(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, result.getMetaData().getColumnType(1));
          value = assertInstanceOf(OffsetDateTime.class, result.getObject(1));
        }
        assertEquals(Instant.parse("2024-07-01T12:00:00Z"), value.toInstant());
        assertEquals(array ? "[[\"2024-07-01T12:00:00Z\"]]" : "[\"2024-07-01T12:00:00Z\"]", result.getString(2));
      }
    }
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[ua,ARRAY[CURRENT_TIMESTAMP,NULL]] FROM t")) {
      assertTrue(result.next());
      var array = result.getArray(1);
      try {
        Object first = ((Object[]) array.getArray())[0];
        Object value = first instanceof Object[] elements ? elements[0] : ((List<?>) first).getFirst();
        assertEquals(Instant.parse("2024-07-01T12:00:00Z"), assertInstanceOf(OffsetDateTime.class, value).toInstant());
      } finally {
        array.free();
      }
    }
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[ts,CURRENT_TIMESTAMP] FROM t")) {
      assertTrue(result.next());
      var array = result.getArray(1);
      try {
        assertEquals(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, array.getBaseType());
        OffsetDateTime value = assertInstanceOf(OffsetDateTime.class, ((Object[]) array.getArray())[0]);
        assertEquals(Instant.parse("2024-07-01T12:00:00Z"), value.toInstant());
      } finally {
        array.free();
      }
    }
  }

  /**
   * Checks both constructors without changing the expression's evaluation.
   *
   * @param connection
   *          open database connection
   * @param expression
   *          value expression
   * @param from
   *          optional input clause
   * @param expected
   *          expected JSON value
   * @throws Exception
   *           if query execution fails
   */
  private static void assertValue(Connection connection, String expression, String from, String expected)
      throws Exception {
    String sql = "SELECT JSON_ARRAY(" + expression + "), JSON_OBJECT(KEY 'k' VALUE " + expression + ")" + from;
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      assertTrue(result.next(), sql);
      assertEquals("[" + expected + "]", result.getString(1), sql);
      assertEquals("{\"k\":" + expected + "}", result.getString(2), sql);
      assertFalse(result.next(), sql);
    }
  }

  /**
   * Writes a summer UTC instant and local values in Stockholm's spring gap.
   *
   * @throws Exception
   *           if fixture writing fails
   */
  private void writeFixture() throws Exception {
    Schema utc = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema local = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema = SchemaBuilder.record("jsonReview").fields().requiredInt("id").requiredString("j").name("null_ts")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL), utc)).noDefault().name("ts").type(utc).noDefault()
        .name("lts").type(local).noDefault().name("wall").type(local).noDefault().name("la")
        .type(Schema.createArray(local)).noDefault().name("nla")
        .type(Schema.createArray(Schema.createUnion(Schema.create(Schema.Type.NULL), local))).noDefault().name("ua")
        .type(Schema.createArray(Schema.createUnion(Schema.create(Schema.Type.NULL), utc))).noDefault().name("d")
        .type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault().endRecord();
    var row = new GenericData.Record(schema);
    row.put("id", 1);
    row.put("j", "{\"a\":1}");
    row.put("ts", Instant.parse("2024-07-01T12:00:00Z").toEpochMilli());
    long gap = LocalDateTime.parse("2024-03-31T02:30:00").toInstant(ZoneOffset.UTC).toEpochMilli();
    row.put("lts", gap);
    row.put("wall", LocalDateTime.parse("2024-01-02T03:04:05").toInstant(ZoneOffset.UTC).toEpochMilli());
    row.put("la", List.of(gap));
    row.put("nla", Arrays.asList(gap, null));
    row.put("ua", Arrays.asList(Instant.parse("2024-07-01T12:00:00Z").toEpochMilli(), null));
    row.put("d", (int) LocalDate.parse("2024-01-02").toEpochDay());
    var configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("t.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      writer.write(row);
    }
  }
}
