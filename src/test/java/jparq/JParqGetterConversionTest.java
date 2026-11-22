package jparq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies that getBigDecimal, getBoolean, and getBytes convert compatible
 * values and classify invalid ones, and that the Boolean and byte[] typed
 * getObject targets use the same conversion rules.
 */
class JParqGetterConversionTest {
  @TempDir
  Path directory;

  /**
   * Converts numeric text to decimal by both column index and label.
   *
   * @param text
   *          the numeric text stored in the column
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "1", " +12 ", "-12.75", "1.25e1", "1E+2"
  })
  void convertsNumericTextToBigDecimal(String text) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("'" + text + "' AS v", "CAST('" + text + "' AS VARCHAR) AS v")) {
        try (var result = statement.executeQuery("SELECT " + expression)) {
          assertTrue(result.next(), expression);
          assertEquals(text, result.getObject(1), expression);
          for (boolean byLabel : List.of(false, true)) {
            assertEquals(0, read(result, byLabel).compareTo(new BigDecimal(text.trim())), expression);
            assertFalse(result.wasNull(), expression);
          }
        }
      }
    }
  }

  /**
   * Converts numbers, booleans, and SQL NULL consistently across both overloads.
   *
   * @param expression
   *          the expression producing the column value
   * @param expected
   *          the expected decimal digits, or null for SQL NULL
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(value = {
      "42 | 42", "CAST(42 AS BIGINT) | 42", "CAST(42 AS REAL) | 42", "CAST(42 AS DOUBLE) | 42",
      "CAST(42 AS DECIMAL(5,2)) | 42", "TRUE | 1", "FALSE | 0", "CAST(NULL AS VARCHAR) | ",
      "CAST(NULL AS DECIMAL(5,2)) | "
  }, delimiter = '|')
  void convertsNativeValuesToBigDecimal(String expression, String expected) throws SQLException {
    BigDecimal expectedValue = expected == null ? null : new BigDecimal(expected);
    boolean isNull = expectedValue == null;
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression + " AS v")) {
      assertTrue(result.next(), expression);
      for (boolean byLabel : List.of(false, true)) {
        BigDecimal actual = read(result, byLabel);
        if (isNull) {
          assertNull(actual, expression);
        } else {
          assertEquals(0, actual.compareTo(expectedValue), expression);
        }
        assertEquals(isNull, result.wasNull(), expression);
      }
    }
  }

  /**
   * Reports invalid and incompatible values through checked data exceptions
   * carrying SQLState 22018.
   *
   * @param expression
   *          the expression producing the column value
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'not a number'", "''", "'   '", "'NaN'", "'Infinity'", "'-Infinity'", "DATE '2024-01-01'", "X'7879'"
  })
  void rejectsInvalidBigDecimalValues(String expression) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression + " AS v")) {
      assertTrue(result.next(), expression);
      for (boolean byLabel : List.of(false, true)) {
        var failure = assertThrows(SQLDataException.class, () -> read(result, byLabel), expression);
        assertEquals("22018", failure.getSQLState(), expression);
        assertTrue(failure.getMessage().contains("column 1 ('v') as DECIMAL"), failure::getMessage);
        assertCausedBy(failure, NumberFormatException.class, expression);
        assertFalse(result.wasNull(), expression);
      }
    }
  }

  /**
   * Converts booleans, boolean text using the shared CAST text forms plus the
   * JDBC '1' and '0' forms, and numbers by their decimal sign.
   *
   * @param expression
   *          the expression producing the column value
   * @param expected
   *          the expected boolean result
   * @param isNull
   *          whether the column value is SQL NULL
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(value = {
      "TRUE | true | false", "FALSE | false | false", "1 | true | false", "0 | false | false", "42 | true | false",
      "-1 | true | false", "\"'true'\" | true | false", "\"'false'\" | false | false", "\"'True'\" | true | false",
      "\"'FALSE'\" | false | false", "\"' true '\" | true | false", "\"'1'\" | true | false", "\"'0'\" | false | false",
      "\"'t'\" | true | false", "\"'T'\" | true | false", "\"'yes'\" | true | false", "\"'y'\" | true | false",
      "\"'f'\" | false | false", "\"'no'\" | false | false", "\"'n'\" | false | false", "\"''\" | false | false",
      "\"'   '\" | false | false", "CAST(42 AS BIGINT) | true | false", "CAST(0 AS BIGINT) | false | false",
      "CAST(0.5 AS DOUBLE) | true | false", "CAST(0 AS DECIMAL(5,2)) | false | false",
      "CAST(NULL AS VARCHAR) | false | true", "CAST(NULL AS BOOLEAN) | false | true"
  }, delimiter = '|', quoteCharacter = '"')
  void convertsBooleanValues(String expression, boolean expected, boolean isNull) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression + " AS v")) {
      assertTrue(result.next(), expression);
      for (boolean byLabel : List.of(false, true)) {
        assertEquals(expected, byLabel ? result.getBoolean("v") : result.getBoolean(1), expression);
        assertEquals(isNull, result.wasNull(), expression);
      }
    }
  }

  /**
   * Reports text and values that JDBC cannot map to boolean as SQLDataException
   * 22018 instead of leaking unchecked exceptions.
   *
   * @param expression
   *          the expression producing the column value
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'abc'", "'2'", "'1.5'", "'NaN'", "DATE '2024-01-01'", "X'7879'"
  })
  void rejectsInvalidBooleanValues(String expression) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression + " AS v")) {
      assertTrue(result.next(), expression);
      for (boolean byLabel : List.of(false, true)) {
        var failure = assertThrows(SQLDataException.class, () -> readBoolean(result, byLabel), expression);
        assertEquals("22018", failure.getSQLState(), expression);
        assertTrue(failure.getMessage().contains("column 1 ('v') as BOOLEAN"), failure::getMessage);
        assertFalse(result.wasNull(), expression);
      }
    }
  }

  /**
   * Returns binary values unchanged, converts text to UTF-8 bytes, and reports
   * incompatible values with SQLState 22018.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void convertsBytesValues() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (var caseRow : List.of(new CaseRow("X'7879'", new byte[]{
          (byte) 0x78, (byte) 0x79
      }), new CaseRow("CAST('xy' AS VARBINARY)", new byte[]{
          120, 121
      }), new CaseRow("'abc'", "abc".getBytes(StandardCharsets.UTF_8)),
          new CaseRow("'å🙂'", "å🙂".getBytes(StandardCharsets.UTF_8)))) {
        try (var result = statement.executeQuery("SELECT " + caseRow.expression() + " AS v")) {
          assertTrue(result.next(), caseRow.expression());
          for (boolean byLabel : List.of(false, true)) {
            assertArrayEquals(caseRow.expected(), byLabel ? result.getBytes("v") : result.getBytes(1),
                caseRow.expression());
            assertFalse(result.wasNull(), caseRow.expression());
          }
        }
      }
    }
  }

  /**
   * Reports numeric, boolean, and temporal values as SQLDataException 22018 from
   * getBytes instead of leaking unchecked exceptions.
   *
   * @param expression
   *          the expression producing the column value
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "42", "CAST(42 AS BIGINT)", "TRUE", "CAST(1.5 AS DOUBLE)", "DATE '2024-01-01'"
  })
  void rejectsInvalidBytesValues(String expression) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression + " AS v")) {
      assertTrue(result.next(), expression);
      for (boolean byLabel : List.of(false, true)) {
        var failure = assertThrows(SQLDataException.class, () -> readBytes(result, byLabel), expression);
        assertEquals("22018", failure.getSQLState(), expression);
        assertTrue(failure.getMessage().contains("column 1 ('v') as BINARY"), failure::getMessage);
        assertFalse(result.wasNull(), expression);
      }
    }
  }

  /**
   * Returns null and false for SQL NULL across all three getters, with wasNull()
   * true.
   *
   * @param expression
   *          the expression producing the SQL NULL column
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CAST(NULL AS VARBINARY)", "CAST(NULL AS VARCHAR)", "CAST(NULL AS BOOLEAN)", "CAST(NULL AS DECIMAL(5,2))"
  })
  void returnsNullsForSqlNull(String expression) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression + " AS v")) {
      assertTrue(result.next(), expression);
      for (boolean byLabel : List.of(false, true)) {
        assertNull(byLabel ? result.getBytes("v") : result.getBytes(1), expression);
        assertNull(byLabel ? result.getBigDecimal("v") : result.getBigDecimal(1), expression);
        assertFalse(byLabel ? result.getBoolean("v") : result.getBoolean(1), expression);
        assertTrue(result.wasNull(), expression);
      }
    }
  }

  /**
   * Converts and classifies LATERAL derived tables that materialize numeric
   * expressions as VARCHAR.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void convertsLateralMaterializedText() throws SQLException {
    String sql = "SELECT x.v FROM (VALUES (1), (2)) AS a(n) "
        + "CROSS JOIN LATERAL (SELECT a.n / 1 AS v FROM (VALUES (1)) AS seed(z)) x ORDER BY x.v";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      for (int expected = 1; expected <= 2; expected++) {
        assertTrue(result.next());
        assertInstanceOf(String.class, result.getObject(1));
        for (boolean byLabel : List.of(false, true)) {
          assertEquals(0,
              (byLabel ? result.getBigDecimal("v") : result.getBigDecimal(1)).compareTo(BigDecimal.valueOf(expected)));
          if (expected == 1) {
            assertTrue(byLabel ? result.getBoolean("v") : result.getBoolean(1));
          } else {
            // #475: the LATERAL table materializes the DECIMAL quotient as
            // VARCHAR text '2', which is not a JDBC boolean form; the same
            // expression outside LATERAL is DECIMAL and converts to true.
            var failure = assertThrows(SQLDataException.class, () -> readBoolean(result, byLabel));
            assertEquals("22018", failure.getSQLState());
            assertTrue(failure.getMessage().contains("as BOOLEAN"), failure::getMessage);
          }
          assertFalse(result.wasNull());
        }
      }
      assertFalse(result.next());
    }
    String failing = "SELECT x.v FROM (VALUES (1)) AS a(n) "
        + "CROSS JOIN LATERAL (SELECT 'abc' AS v FROM (VALUES (1)) AS seed(z)) x";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(failing)) {
      assertTrue(result.next());
      for (boolean byLabel : List.of(false, true)) {
        var failure = assertThrows(SQLDataException.class, () -> read(result, byLabel));
        assertEquals("22018", failure.getSQLState());
        assertTrue(failure.getMessage().contains("as DECIMAL"), failure::getMessage);
      }
    }
  }

  /**
   * Pins the bytes getBytes returns for an outer binary value that a LATERAL
   * derived table stringifies to its Java identity text. The binary-to-VARCHAR
   * materialization is the defect tracked by issue 599; until it is fixed this
   * test keeps the corrupted result from changing.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void pinsLateralStringifiedBinaryBytes() throws SQLException {
    String sql = "SELECT x.v FROM (VALUES (X'FF00')) AS a(n) "
        + "CROSS JOIN LATERAL (SELECT a.n AS v FROM (VALUES (1)) AS seed(z)) x";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertTrue(result.next());
      // #599: the outer binary value is stringified to its Java identity
      // instead of being decoded to VARCHAR.
      String identity = assertInstanceOf(String.class, result.getObject(1));
      assertTrue(identity.startsWith("[B@"), identity);
      for (boolean byLabel : List.of(false, true)) {
        assertArrayEquals(identity.getBytes(StandardCharsets.UTF_8),
            byLabel ? result.getBytes("v") : result.getBytes(1),
            "#599: the identity text's UTF-8 bytes, not the original binary");
      }
      assertFalse(result.next());
    }
  }

  /**
   * Converts Boolean and byte[] typed getObject targets with the same rules as
   * getBoolean and getBytes, and classifies invalid values with SQLState 22018.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void convertsTypedObjectBooleanAndBinary() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT '1' AS v")) {
        assertTrue(result.next());
        assertEquals(true, result.getObject(1, Boolean.class));
        assertFalse(result.wasNull());
      }
      try (var result = statement.executeQuery("SELECT 42 AS v")) {
        assertTrue(result.next());
        assertEquals(true, result.getObject(1, Boolean.class));
        assertFalse(result.wasNull());
      }
      try (var result = statement.executeQuery("SELECT 'abc' AS v")) {
        assertTrue(result.next());
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), result.getObject(1, byte[].class));
        assertFalse(result.wasNull());
      }
      try (var result = statement.executeQuery("SELECT X'7879' AS v")) {
        assertTrue(result.next());
        assertArrayEquals(new byte[]{
            (byte) 0x78, (byte) 0x79
        }, result.getObject(1, byte[].class));
        assertFalse(result.wasNull());
      }
      try (var result = statement.executeQuery("SELECT 'abc' AS v")) {
        assertTrue(result.next());
        var failure = assertThrows(SQLDataException.class, () -> result.getObject(1, Boolean.class));
        assertEquals("22018", failure.getSQLState());
      }
      try (var result = statement.executeQuery("SELECT 42 AS v")) {
        assertTrue(result.next());
        var failure = assertThrows(SQLDataException.class, () -> result.getObject(1, byte[].class));
        assertEquals("22018", failure.getSQLState());
      }
      try (var result = statement.executeQuery("SELECT CAST(NULL AS VARCHAR) AS v")) {
        assertTrue(result.next());
        assertNull(result.getObject(1, Boolean.class));
        assertNull(result.getObject(1, byte[].class));
        assertTrue(result.wasNull());
      }
    }
  }

  /**
   * Rejects non-finite stored doubles from getBoolean, the typed Boolean
   * getObject target, and CAST AS BOOLEAN with SQLState 22018, while finite
   * doubles keep their sign.
   *
   * @throws Exception
   *           if the fixture or a query fails
   */
  @Test
  void rejectsNonFiniteStoredDoublesAsBoolean() throws Exception {
    Schema schema = SchemaBuilder.record("doubles").fields().requiredInt("id").name("d")
        .type(Schema.create(Schema.Type.DOUBLE)).noDefault().endRecord();
    var file = new org.apache.hadoop.fs.Path(directory.resolve("doubles.parquet").toUri());
    try (
        var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, new Configuration(false)))
            .withSchema(schema).withConf(new Configuration(false)).build()) {
      int id = 1;
      for (double value : List.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0d, 42.0d)) {
        var row = new GenericData.Record(schema);
        row.put("id", id++);
        row.put("d", value);
        writer.write(row);
      }
    }
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (int id = 1; id <= 3; id++) {
        try (var result = statement.executeQuery("SELECT d FROM doubles WHERE id = " + id)) {
          assertTrue(result.next(), "id " + id);
          var failure = assertThrows(SQLDataException.class, () -> result.getBoolean(1), "getBoolean id " + id);
          assertEquals("22018", failure.getSQLState(), "getBoolean id " + id);
          var typed = assertThrows(SQLDataException.class, () -> result.getObject(1, Boolean.class), "typed id " + id);
          assertEquals("22018", typed.getSQLState(), "typed id " + id);
        }
        try (var result = statement.executeQuery("SELECT CAST(d AS BOOLEAN) AS v FROM doubles WHERE id = " + id)) {
          assertTrue(result.next(), "cast id " + id);
          var failure = assertThrows(SQLDataException.class, () -> result.getObject(1), "cast id " + id);
          assertEquals("22018", failure.getSQLState(), "cast id " + id);
        }
      }
      try (var result = statement.executeQuery("SELECT d FROM doubles WHERE id = 4")) {
        assertTrue(result.next());
        assertFalse(result.getBoolean(1));
      }
      try (var result = statement.executeQuery("SELECT d FROM doubles WHERE id = 5")) {
        assertTrue(result.next());
        assertTrue(result.getBoolean(1));
      }
    }
  }

  /**
   * Calls getBigDecimal using the requested access mode.
   *
   * @param result
   *          the positioned result set
   * @param byLabel
   *          whether to use the column label instead of its index
   * @return the decimal value
   * @throws SQLException
   *           if retrieval or conversion fails
   */
  private static BigDecimal read(ResultSet result, boolean byLabel) throws SQLException {
    return byLabel ? result.getBigDecimal("v") : result.getBigDecimal(1);
  }

  /**
   * Calls getBoolean using the requested access mode.
   *
   * @param result
   *          the positioned result set
   * @param byLabel
   *          whether to use the column label instead of its index
   * @return the boolean value
   * @throws SQLException
   *           if retrieval or conversion fails
   */
  private static boolean readBoolean(ResultSet result, boolean byLabel) throws SQLException {
    return byLabel ? result.getBoolean("v") : result.getBoolean(1);
  }

  /**
   * Calls getBytes using the requested access mode.
   *
   * @param result
   *          the positioned result set
   * @param byLabel
   *          whether to use the column label instead of its index
   * @return the byte array
   * @throws SQLException
   *           if retrieval or conversion fails
   */
  private static byte[] readBytes(ResultSet result, boolean byLabel) throws SQLException {
    return byLabel ? result.getBytes("v") : result.getBytes(1);
  }

  /**
   * Asserts that a failure's cause chain carries the expected runtime failure.
   *
   * @param failure
   *          the thrown failure
   * @param type
   *          the expected cause type
   * @param context
   *          the diagnostic for the assertion
   */
  private static void assertCausedBy(SQLDataException failure, Class<? extends Throwable> type, String context) {
    Throwable cause = failure.getCause();
    while (cause != null && !type.isInstance(cause)) {
      cause = cause.getCause();
    }
    assertInstanceOf(type, cause, context);
  }

  /** Pairs a SQL expression with its expected byte representation. */
  private record CaseRow(String expression, byte[] expected) {
  }
}
