package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.helper.TemporalInterval;

/** JDBC regressions for date arithmetic and materialized day counts. */
class DateArithmeticTest {

  @TempDir
  Path directory;

  /**
   * Writes physical DATE, decimal, and VARCHAR columns, including a NULL date.
   *
   * @throws Exception
   *           if the fixture cannot be written
   */
  @BeforeEach
  void writeDates() throws Exception {
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema decimal = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema timestamp = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema = SchemaBuilder.record("dates").fields().requiredInt("id").name("d").type().unionOf().nullType().and()
        .type(date).endUnion().nullDefault().name("amt").type(decimal).noDefault().requiredString("s").name("ts1")
        .type(timestamp).noDefault().name("ts2").type(timestamp).noDefault().endRecord();
    Configuration configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("small.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 0; id < 2; id++) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("ts1", java.sql.Timestamp.valueOf("2024-01-01 00:00:00").getTime());
        record.put("ts2", java.sql.Timestamp.valueOf("2024-01-02 00:00:00").getTime());
        record.put("amt", ByteBuffer.wrap(new BigDecimal("123.45").unscaledValue().toByteArray()));
        record.put("d", id == 0 ? (int) LocalDate.of(2024, 3, 1).toEpochDay() : null);
        record.put("s", id == 0 ? "TemporalInterval[period=P0D, duration=PT24H]" : "A");
        writer.write(record);
      }
    }
  }

  /**
   * Rejects unsupported operands at JDBC boundaries with a classified error.
   *
   * @param expression
   *          unsupported arithmetic to execute
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "d + 1", "DATE '2024-01-01' + 1", "1 + d", "d - 1", "1 - d", "d * 2", "d / 2", "d + d",
      "(d - d) + INTERVAL '1 day'", "INTERVAL '1 day' + (d - d)", "(d - d) - INTERVAL '1 day'",
      "(d - d) * INTERVAL '1 day'", "d - '2024-01-01'"
  })
  void unsupportedArithmeticRaisesSqlException(String expression) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT " + expression + " AS x FROM small WHERE id = 0",
          "SELECT t.x FROM (SELECT " + expression + " AS x FROM small WHERE id = 0) t",
          "SELECT id FROM small WHERE id = 0 AND " + expression + " IS NOT NULL")) {
        var failure = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            result.next();
            result.getObject(1);
          }
        }, sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("Unsupported temporal arithmetic"), failure::getMessage);
      }
    }
  }

  /**
   * Classifies subtraction between TIME and a date-bearing value as unsupported
   * arithmetic, including predicates evaluated while reading physical rows.
   *
   * @param expression
   *          mixed TIME subtraction to execute
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "DATE '2024-01-02' - TIME '10:00:00'", "TIME '10:00:00' - DATE '2024-01-02'",
      "TIMESTAMP '2024-01-02 00:00:00' - TIME '10:00:00'", "TIME '10:00:00' - TIMESTAMP '2024-01-02 00:00:00'"
  })
  void mixedTimeDifferencesRaiseSyntaxErrors(String expression) throws Exception {
    String projection = "SELECT " + expression + " AS x FROM small WHERE id = 0";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT " + expression, projection, "SELECT * FROM (" + projection + ") t",
          "WITH t AS (" + projection + ") SELECT * FROM t",
          "SELECT id FROM small WHERE " + expression + " > INTERVAL '1' HOUR")) {
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            result.next();
            result.getObject(1);
          }
        }, sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("Unsupported temporal arithmetic - between"), failure::getMessage);
        assertTrue(failure.getMessage().contains("TIME"), failure::getMessage);
        assertFalse(failure.getMessage().contains("java.time"), failure::getMessage);
      }
    }
  }

  /**
   * Preserves non-DATE temporal differences as interval values across JDBC and
   * materialization boundaries, including nested UNNEST arrays.
   *
   * @param expression
   *          temporal difference expression
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CAST('2024-01-02 00:00:00' AS TIMESTAMP) - CAST('2024-01-01 00:00:00' AS TIMESTAMP)",
      "TIME '12:00:00' - TIME '10:00:00'", "DATE '2024-01-02' - TIMESTAMP '2024-01-01 12:00:00'",
      "TIMESTAMP '2024-01-02 12:00:00' - DATE '2024-01-01'", "CURRENT_TIMESTAMP - CURRENT_TIMESTAMP",
      "CURRENT_TIME - CURRENT_TIME", "-(TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00')",
      "INTERVAL '1' HOUR + (TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00')",
      "ARRAY[TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00']", "INTERVAL '1' DAY",
      "INTERVAL '1' DAY - INTERVAL '1' HOUR", "INTERVAL '1' DAY + INTERVAL '1' HOUR", "INTERVAL '14' MONTH"
  })
  void intervalOutputRetainsIntervals(String expression) throws Exception {
    String projection = "SELECT " + expression + " AS x FROM small WHERE id = 0";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT " + expression, projection, "SELECT * FROM (" + projection + ") t",
          "WITH t AS (" + projection + ") SELECT * FROM t", "SELECT * FROM (VALUES (" + expression + ")) t(x)",
          "SELECT * FROM UNNEST(ARRAY[" + expression + "]) AS u(x)",
          "SELECT * FROM UNNEST(ARRAY[ARRAY[" + expression + "]]) AS u(x)")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          Object value = result.getObject(1);
          while (value instanceof Object[] array) {
            assertEquals(1, array.length, sql);
            value = array[0];
          }
          assertInstanceOf(TemporalInterval.class, value, sql);
          assertFalse(result.getString(1).contains("TemporalInterval["), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Preserves interval arrays while allowing COUNT and ordinality-only output.
   *
   * @throws Exception
   *           if opening the connection fails
   */
  @Test
  void unnestPreservesMixedDifferencesAndOrdinalityOnlyOutput() throws Exception {
    String difference = "TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      try (var result = statement
          .executeQuery("SELECT * FROM UNNEST(ARRAY[INTERVAL '1' DAY, " + difference + "]) AS u(x)")) {
        for (int row = 0; row < 2; row++) {
          assertTrue(result.next());
          TemporalInterval interval = assertInstanceOf(TemporalInterval.class, result.getObject(1));
          assertEquals(Duration.ofDays(1), interval.toDuration());
          assertEquals("+1 00:00:00", result.getString(1));
        }
        assertFalse(result.next());
      }
      for (String sql : List.of("SELECT COUNT(*) FROM UNNEST(ARRAY[" + difference + "]) AS u(x)",
          "SELECT n FROM UNNEST(ARRAY[" + difference + "]) WITH ORDINALITY AS u(x, n)")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Preserves comparisons, filtering, extraction, and sorting of internal
   * differences while exposing only ordinary JDBC values.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void temporalDifferencesRemainUsableInsideEngine() throws Exception {
    String difference = "(TIMESTAMP '2024-01-03 00:00:00' - TIMESTAMP '2024-01-01 00:00:00')";
    String projection = "SELECT " + difference + " > INTERVAL '1' HOUR AS longer, " + "EXTRACT(DAY FROM " + difference
        + ") AS days, " + "EXTRACT(HOUR FROM TIME '12:00:00' - TIME '10:00:00') AS hours " + "FROM small WHERE "
        + difference + " > INTERVAL '1' HOUR ORDER BY " + difference;
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of(projection, "SELECT * FROM (" + projection + ") t",
          "WITH t AS (" + projection + ") SELECT * FROM t")) {
        try (var result = statement.executeQuery(sql)) {
          for (int row = 0; row < 2; row++) {
            assertTrue(result.next(), sql);
            assertEquals(true, result.getObject(1), sql);
            assertEquals(2L, result.getObject(2), sql);
            assertEquals(2L, result.getObject(3), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Preserves interval values through getters, materialization, and predicate
   * subqueries without exposing diagnostic text.
   *
   * @throws Exception
   *           if opening the connection fails
   */
  @Test
  void temporalDifferenceOutputSurvivesMaterialization() throws Exception {
    String difference = "TIMESTAMP '2024-01-03 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT " + difference, "SELECT MAX(" + difference + ") FROM small",
          "SELECT x FROM (SELECT " + difference + " AS x) t",
          "WITH t AS (SELECT " + difference + " AS x) SELECT x FROM t")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(Duration.ofDays(2), assertInstanceOf(TemporalInterval.class, result.getObject(1)).toDuration());
          assertEquals("+2 00:00:00", result.getString(1), sql);
        }
      }
      for (String sql : List.of("SELECT EXTRACT(DAY FROM x) FROM (SELECT " + difference + " AS x) t",
          "WITH t AS (SELECT " + difference + " AS x) SELECT EXTRACT(DAY FROM x) FROM t",
          "SELECT EXTRACT(DAY FROM x) FROM (VALUES (" + difference + ")) t(x)",
          "SELECT EXTRACT(DAY FROM x) FROM UNNEST(ARRAY[" + difference + "]) u(x)")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(2, result.getInt(1), sql);
        }
      }
      for (String sql : List.of("WITH t AS (SELECT " + difference + " AS x) SELECT x > INTERVAL '1' HOUR FROM t",
          "SELECT " + difference + " IN (SELECT " + difference + " FROM small)",
          "SELECT (SELECT MAX(" + difference + ") FROM small) > INTERVAL '1' HOUR")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertTrue(result.getBoolean(1), sql);
        }
      }
    }
  }

  /**
   * Reports stable interval metadata and typed getters for physical temporal
   * arithmetic, including empty materialized queries and LATERAL inputs.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void intervalMetadataAndTypedGetters() throws Exception {
    String projection = "SELECT ts2 - ts1 AS x FROM small WHERE id = 0";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of(projection, "SELECT x FROM (" + projection + ") t",
          "WITH t AS (" + projection + ") SELECT x FROM t", projection + " UNION ALL " + projection,
          "SELECT x FROM small s CROSS JOIN LATERAL "
              + "(SELECT q.ts2 - q.ts1 AS x FROM small q WHERE q.id = s.id) t WHERE s.id = 0")) {
        try (var result = statement.executeQuery(sql)) {
          assertEquals(Types.OTHER, result.getMetaData().getColumnType(1), sql);
          assertEquals("INTERVAL", result.getMetaData().getColumnTypeName(1), sql);
          assertEquals(TemporalInterval.class.getName(), result.getMetaData().getColumnClassName(1), sql);
          assertTrue(result.next(), sql);
          assertEquals(Duration.ofDays(1), result.getObject(1, TemporalInterval.class).toDuration(), sql);
          assertEquals(result.getObject(1), result.getObject("x", TemporalInterval.class), sql);
          assertEquals("+1 00:00:00", result.getObject("x", String.class), sql);
        }
      }
      for (String sql : List.of("SELECT ts2 - ts1 AS x FROM small WHERE id = 99",
          "SELECT x FROM (SELECT ts2 - ts1 AS x FROM small WHERE id = 99) t")) {
        try (var result = statement.executeQuery(sql)) {
          assertEquals("INTERVAL", result.getMetaData().getColumnTypeName(1), sql);
          assertEquals(TemporalInterval.class.getName(), result.getMetaData().getColumnClassName(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      try (var result = statement.executeQuery("SELECT INTERVAL '14' MONTH AS x")) {
        assertTrue(result.next());
        assertEquals("+1-02", result.getString(1));
        assertEquals(TemporalInterval.class.getName(), result.getMetaData().getColumnClassName(1));
      }
    }
  }

  /**
   * Uses interval values for set keys and ORDER BY expressions instead of text.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void intervalSetKeysAndOrderingRetainTypes() throws Exception {
    String difference = "TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      try (var result = statement
          .executeQuery("SELECT COUNT(*) FROM (SELECT " + difference + " AS x UNION SELECT INTERVAL '1' DAY AS x) t")) {
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
      }
      try (var result = statement.executeQuery(
          "SELECT INTERVAL '2' DAY AS x UNION ALL SELECT " + difference + " AS x ORDER BY EXTRACT(DAY FROM x)")) {
        assertTrue(result.next());
        assertEquals("+1 00:00:00", result.getString(1));
        assertTrue(result.next());
        assertEquals("+2 00:00:00", result.getString(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Exposes intervals through JDBC text and explicit character casts.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void intervalCharacterCastsUseSqlText() throws Exception {
    String difference = "TIMESTAMP '2024-01-02 00:00:00.123456789' - TIMESTAMP '2024-01-01 00:00:00'";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String type : List.of("VARCHAR", "CHAR", "CLOB")) {
        try (var result = statement.executeQuery("SELECT CAST((" + difference + ") AS " + type + ")")) {
          assertTrue(result.next());
          assertEquals("+1 00:00:00.123456789", result.getString(1));
        }
      }
      try (var result = statement.executeQuery("SELECT CAST((" + difference + ") AS VARCHAR(5))")) {
        assertTrue(result.next());
        assertEquals("+1 00", result.getString(1));
      }
    }
  }

  /**
   * Uses SQL interval text in JSON values, including nested arrays.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void intervalJsonValuesUseSqlText() throws Exception {
    String difference = "TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT JSON_OBJECT('a' VALUE (" + difference + "))")) {
        assertTrue(result.next());
        assertEquals("{\"a\":\"+1 00:00:00\"}", result.getString(1));
      }
      for (String sql : List.of("SELECT JSON_ARRAY(ARRAY[" + difference + "])",
          "SELECT JSON_ARRAY(x) FROM (SELECT ARRAY[" + difference + "] AS x) t")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next());
          assertEquals("[[\"+1 00:00:00\"]]", result.getString(1));
        }
      }
    }
  }

  /**
   * Rejects implicit interval operands in string operations with a SQL type
   * error.
   *
   * @param template
   *          text-consuming SQL expression with one difference placeholder
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "%s || ''", "'' || %s", "CONCAT('x', %s)", "UPPER(%s)", "LOWER(%s)", "LENGTH(%s)", "OCTET_LENGTH(%s)",
      "ASCII(%s)", "STRING_AGG(%s, ',')", "%s LIKE 'Temporal%%'"
  })
  void temporalDifferencesCannotBeConvertedToText(String template) throws Exception {
    String difference = "(TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00')";
    String projection = "SELECT " + template.formatted(difference) + " AS x FROM small";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of(projection, "SELECT * FROM (" + projection + ") t",
          "WITH t AS (" + projection + ") SELECT * FROM t")) {
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            result.next();
            result.getObject(1);
          }
        }, sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("INTERVAL requires"), failure::getMessage);
      }
    }
  }

  /**
   * Rejects comparisons that would coerce a difference to diagnostic text,
   * including reversed operands and comparisons inside predicates and HAVING.
   *
   * @param operator
   *          comparison operator to exercise
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "=", "<>", "!=", "<", ">", "<=", ">="
  })
  void temporalDifferencesCannotBeComparedWithText(String operator) throws Exception {
    String difference = "(TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00')";
    String text = "'TemporalInterval[period=P0D, duration=PT24H]'";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String comparison : List.of(difference + " " + operator + " " + text,
          text + " " + operator + " " + difference, "ARRAY[" + difference + "] " + operator + " ARRAY[" + text + "]")) {
        String projection = "SELECT " + comparison + " AS x FROM small";
        for (String sql : List.of(projection, "SELECT * FROM (" + projection + ") t",
            "WITH t AS (" + projection + ") SELECT * FROM t", "SELECT id FROM small WHERE " + comparison,
            "SELECT id FROM small GROUP BY id HAVING " + comparison)) {
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("INTERVAL requires"), failure::getMessage);
        }
      }
    }
  }

  /**
   * Rejects schema-driven coercion of a difference to a VARCHAR column before
   * predicate and join comparisons can consume its internal text.
   *
   * @param operator
   *          comparison operator to exercise
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "=", "<>", "!=", "<", ">", "<=", ">="
  })
  void temporalDifferencesCannotBeComparedWithVarcharColumns(String operator) throws Exception {
    String difference = "(TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00')";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String comparison : List.of(difference + " " + operator + " b.s", "b.s " + operator + " " + difference)) {
        for (String sql : List.of("SELECT COUNT(*) FROM small b WHERE " + comparison,
            "SELECT COUNT(*) FROM small a JOIN (SELECT s FROM small WHERE id = 0) b ON " + comparison,
            "SELECT COUNT(*) FROM small a, (SELECT s FROM small WHERE id = 0) b WHERE " + comparison)) {
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              result.next();
              result.getObject(1);
            }
          }, sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("INTERVAL requires"), failure::getMessage);
        }
      }
    }
  }

  /**
   * Applies the same text-conversion restriction to window ordering over mixed
   * temporal-difference and text values.
   *
   * @throws Exception
   *           if opening the connection fails
   */
  @Test
  void windowOrderingCannotCompareTemporalDifferenceText() throws Exception {
    String difference = "TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("CASE WHEN id = 0 THEN " + difference + " ELSE 'A' END",
          "CASE WHEN id = 0 THEN 'A' ELSE " + difference + " END")) {
        String sql = "SELECT ROW_NUMBER() OVER (ORDER BY " + expression + ") FROM small";
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            result.next();
            result.getObject(1);
          }
        }, sql);
        assertEquals("42000", failure.getSQLState(), sql);
      }
    }
  }

  /**
   * Checks row existence without reading unused temporal difference projections,
   * including correlated predicates and HAVING.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void existsDoesNotReadTemporalDifferenceOutput() throws Exception {
    String difference = "TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'";
    String subquery = "SELECT " + difference + " FROM small t";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT id FROM small WHERE EXISTS (" + subquery + ") ORDER BY id",
          "SELECT id FROM small s WHERE EXISTS (" + subquery + " WHERE t.id = s.id) ORDER BY id",
          "SELECT id FROM small WHERE NOT EXISTS (" + subquery + " WHERE id = 99) ORDER BY id",
          "SELECT id FROM small GROUP BY id HAVING EXISTS (" + subquery + ") ORDER BY id")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(0, result.getInt(1), sql);
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      try (var result = statement.executeQuery("SELECT id FROM small WHERE EXISTS (" + subquery + " WHERE id = 99)")) {
        assertFalse(result.next());
      }
    }
  }

  /**
   * Checks SQL row existence for set operations containing interval values.
   *
   * @param operation
   *          set operator joining the branches
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UNION", "UNION ALL", "INTERSECT", "EXCEPT"
  })
  void existsSetOperationsPreserveTemporalDifferenceOutput(String operation) throws Exception {
    String difference = "TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String predicate : List.of("", " WHERE t.id = s.id")) {
        String branch = "SELECT " + difference + " FROM small t" + predicate;
        String sql = "SELECT id FROM small s WHERE EXISTS (" + branch + " " + operation + " " + branch
            + ") ORDER BY id";
        try (var result = statement.executeQuery(sql)) {
          if ("EXCEPT".equals(operation)) {
            assertFalse(result.next(), sql);
          } else {
            assertTrue(result.next(), sql);
            assertEquals(0, result.getInt(1), sql);
            assertTrue(result.next(), sql);
            assertEquals(1, result.getInt(1), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Preserves interval output when an interval literal ties for MIN/MAX,
   * regardless of input order, in grouped aggregates and window frames.
   *
   * @param function
   *          aggregate function to evaluate
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "MIN", "MAX"
  })
  void temporalDifferenceExtremumTiesRetainProvenance(String function) throws Exception {
    String difference = "TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (int differenceId : List.of(0, 1)) {
        String argument = "CASE WHEN id = " + differenceId + " THEN " + difference + " ELSE INTERVAL '1' DAY END";
        for (String window : List.of("", " OVER ()",
            " OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)")) {
          String expression = function + "(" + argument + ")" + window;
          String sql = "SELECT " + expression + " FROM small";
          try (var result = statement.executeQuery(sql)) {
            assertTrue(result.next(), sql);
            assertEquals(Duration.ofDays(1),
                assertInstanceOf(TemporalInterval.class, result.getObject(1)).toDuration());
            assertEquals("+1 00:00:00", result.getString(1), sql);
          }
          try (var result = statement.executeQuery("SELECT EXTRACT(DAY FROM " + expression + ") FROM small")) {
            assertTrue(result.next());
            assertEquals(1, result.getInt(1));
          }
        }
      }
      try (var result = statement
          .executeQuery("SELECT EXTRACT(DAY FROM " + function + "(INTERVAL '1' DAY)) FROM small")) {
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Retains NULL propagation for non-DATE temporal subtraction.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void nullTemporalDifferencesRemainNull() throws Exception {
    String projection = "SELECT CAST(NULL AS TIMESTAMP) - TIMESTAMP '2024-01-01 00:00:00' AS a, "
        + "TIME '12:00:00' - CAST(NULL AS TIME) AS b";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of(projection, "SELECT * FROM (" + projection + ") t",
          "WITH t AS (" + projection + ") SELECT * FROM t")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next());
          assertNull(result.getObject(1));
          assertTrue(result.wasNull());
          assertNull(result.getObject(2));
          assertTrue(result.wasNull());
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Preserves signed calendar-day counts through derived tables and CTEs.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void dateDifferencesRemainNumeric() throws Exception {
    String projection = "SELECT d - DATE '2024-02-28' AS forward_days, "
        + "DATE '2024-02-28' - d AS backward_days, d - d AS zero_days FROM small";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of(projection, "SELECT * FROM (" + projection + ") t",
          "WITH t AS (" + projection + ") SELECT * FROM t")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next());
          assertEquals(2L, result.getObject(1));
          assertEquals(-2L, result.getObject(2));
          assertEquals(0L, result.getObject(3));
          assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
          assertTrue(result.next());
          assertNull(result.getObject(1));
          assertTrue(result.wasNull());
          assertNull(result.getObject(2));
          assertNull(result.getObject(3));
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Reports day-count metadata before reading rows, including empty results and
   * aliases that shadow a source date.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void dayCountMetadataDoesNotDependOnRows() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT d - d AS d FROM small WHERE id = 99",
          "SELECT (d) - CAST('2024-03-01' AS DATE) AS d FROM small WHERE id = 99",
          "SELECT * FROM (SELECT d - d AS d FROM small WHERE id = 99) t",
          "SELECT DATE '2024-03-01' - DATE '2024-02-28' AS d")) {
        try (var result = statement.executeQuery(sql)) {
          assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1), sql);
          assertEquals(Long.class.getName(), result.getMetaData().getColumnClassName(1), sql);
          if (result.next()) {
            assertEquals(2L, result.getObject(1));
          }
        }
      }
    }
  }

  /**
   * Rejects interval comparisons with numeric day counts in either operand order.
   *
   * @param operator
   *          comparison operator
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      ">", "<", "=", "<>", ">=", "<="
  })
  void dayCountsCannotBeComparedWithIntervals(String operator) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String comparison : List.of("d - DATE '2024-02-01' " + operator + " INTERVAL '7 days'",
          "INTERVAL '7 days' " + operator + " (d - DATE '2024-02-01')")) {
        for (String sql : List.of("SELECT id FROM small WHERE id = 0 AND " + comparison,
            "SELECT " + comparison + " FROM small WHERE id = 0",
            "SELECT CASE d - DATE '2024-02-01' WHEN INTERVAL '29 days' THEN 'y' ELSE 'n' END "
                + "FROM small WHERE id = 0",
            "SELECT CASE INTERVAL '29 days' WHEN d - DATE '2024-02-01' THEN 'y' ELSE 'n' END "
                + "FROM small WHERE id = 0")) {
          var failure = assertThrows(SQLException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              result.next();
              result.getObject(1);
            }
          }, sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("compare"), failure::getMessage);
        }
      }
      try (var result = statement.executeQuery("SELECT id FROM small WHERE d - DATE '2024-02-01' > 7")) {
        assertTrue(result.next());
        assertEquals(0, result.getInt(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Preserves metadata and values for parenthesized, current-date, and bound-date
   * differences through materialization and shadowed aliases.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void dayCountShapesPreserveTypes() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String expression : List.of("(d - DATE '2024-02-28')", "((d - DATE '2024-02-28'))", "CURRENT_DATE - d",
          "CURRENT_DATE() - d", "? - d")) {
        for (String filter : List.of("id = 0", "id = 99")) {
          String inner = "SELECT " + expression + " AS d FROM small WHERE " + filter;
          for (String sql : List.of(inner, "SELECT * FROM (" + inner + ") t",
              "WITH t AS (" + inner + ") SELECT * FROM t")) {
            try (var statement = connection.prepareStatement(sql)) {
              if (expression.contains("?")) {
                statement.setDate(1, Date.valueOf("2024-03-03"));
              }
              LocalDate before = LocalDate.now();
              try (var result = statement.executeQuery()) {
                assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1), sql);
                assertEquals(Long.class.getName(), result.getMetaData().getColumnClassName(1), sql);
                if (filter.equals("id = 0")) {
                  assertTrue(result.next(), sql);
                  Object actual = result.getObject(1);
                  if (expression.contains("CURRENT_DATE")) {
                    LocalDate after = LocalDate.now();
                    long beforeDays = java.time.temporal.ChronoUnit.DAYS.between(LocalDate.of(2024, 3, 1), before);
                    long afterDays = java.time.temporal.ChronoUnit.DAYS.between(LocalDate.of(2024, 3, 1), after);
                    assertTrue(Long.valueOf(beforeDays).equals(actual) || Long.valueOf(afterDays).equals(actual), sql);
                  } else {
                    assertEquals(2L, actual, sql);
                  }
                }
                assertFalse(result.next(), sql);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Reports arithmetic types independently of matching source aliases, even for
   * empty inputs and materialization boundaries.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void numericSubtractionUsesExpressionMetadata() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String field : List.of("amt", "id")) {
        for (String expression : List.of(field + " - 1", "(" + field + " - 1)")) {
          for (String filter : List.of("id = 0", "id = 99")) {
            String inner = "SELECT " + expression + " AS " + field + " FROM small WHERE " + filter;
            for (String sql : List.of(inner, "SELECT * FROM (" + inner + ") t",
                "WITH t AS (" + inner + ") SELECT * FROM t")) {
              try (var result = statement.executeQuery(sql)) {
                var metadata = result.getMetaData();
                assertEquals(field.equals("amt") ? Types.DECIMAL : Types.BIGINT, metadata.getColumnType(1), sql);
                if (field.equals("amt")) {
                  assertEquals(11, metadata.getPrecision(1), sql);
                  assertEquals(2, metadata.getScale(1), sql);
                }
                if (sql.equals(inner)) {
                  assertEquals(java.sql.ResultSetMetaData.columnNullableUnknown, metadata.isNullable(1), sql);
                }
                if (filter.equals("id = 0")) {
                  assertTrue(result.next(), sql);
                  if (field.equals("amt")) {
                    assertEquals(new BigDecimal("122.45"), result.getBigDecimal(1), sql);
                  } else {
                    assertEquals(-1, result.getInt(1), sql);
                  }
                }
                assertFalse(result.next(), sql);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Preserves the documented DATE behavior of both JDBC date binding APIs.
   *
   * @param localDate
   *          whether to bind LocalDate using setObject
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void dateParametersRetainDateValues(boolean localDate) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT ?, ? + INTERVAL '1 day', EXTRACT(YEAR FROM ?)")) {
      for (int parameter = 1; parameter <= 3; parameter++) {
        if (localDate) {
          statement.setObject(parameter, LocalDate.of(2024, 3, 1));
        } else {
          statement.setDate(parameter, Date.valueOf("2024-03-01"));
        }
      }
      try (var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals(Date.valueOf("2024-03-01"), result.getObject(1));
        assertEquals(Date.valueOf("2024-03-02"), result.getObject(2));
        assertEquals(2024, result.getInt(3));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Uses SQL operators and type names with an appropriate conversion hint.
   *
   * @throws Exception
   *           if opening the connection fails
   */
  @Test
  void arithmeticDiagnosticsUseSqlTypes() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("d + 1", "d - '2024-01-01'")) {
        var failure = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery("SELECT " + expression + " FROM small WHERE id = 0")) {
            result.next();
            result.getObject(1);
          }
        });
        assertTrue(
            failure.getMessage()
                .contains(expression.contains("+") ? "+ between DATE and INTEGER" : "- between DATE and VARCHAR"),
            failure::getMessage);
        if (expression.contains("'")) {
          assertTrue(failure.getMessage().contains("CAST(... AS DATE)"), failure::getMessage);
        }
      }
    }
  }

  /**
   * Preserves explicit interval offsets and NULL propagation.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void intervalOffsetsAndNullsStillWork() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT d + INTERVAL '1 day', d - INTERVAL '1 day', "
            + "INTERVAL '1 day' + d, d + NULL, d + 1 FROM small WHERE id = 1")) {
      assertTrue(result.next());
      for (int column = 1; column <= 5; column++) {
        assertNull(result.getObject(column));
      }
    }
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(
            "SELECT d + INTERVAL '1 day', d - INTERVAL '1 day', " + "INTERVAL '1 day' + d FROM small WHERE id = 0")) {
      assertTrue(result.next());
      assertEquals(Date.valueOf("2024-03-02"), result.getObject(1));
      assertEquals(Date.valueOf("2024-02-29"), result.getObject(2));
      assertEquals(Date.valueOf("2024-03-02"), result.getObject(3));
    }
  }
}
