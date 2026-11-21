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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies that getBigDecimal, getBoolean, and getBytes convert compatible
 * values and classify invalid ones.
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
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "42", "CAST(42 AS BIGINT)", "CAST(42 AS REAL)", "CAST(42 AS DOUBLE)", "CAST(42 AS DECIMAL(5,2))", "TRUE", "FALSE",
      "CAST(NULL AS VARCHAR)", "CAST(NULL AS DECIMAL(5,2))"
  })
  void convertsNativeValuesToBigDecimal(String expression) throws SQLException {
    BigDecimal expected;
    if (expression.contains("NULL")) {
      expected = null;
    } else if (expression.contains("FALSE")) {
      expected = BigDecimal.ZERO;
    } else if (expression.contains("TRUE")) {
      expected = BigDecimal.ONE;
    } else {
      expected = BigDecimal.valueOf(42);
    }
    boolean isNull = expected == null;
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression + " AS v")) {
      assertTrue(result.next(), expression);
      for (boolean byLabel : List.of(false, true)) {
        BigDecimal actual = read(result, byLabel);
        if (isNull) {
          assertNull(actual, expression);
        } else {
          assertEquals(0, actual.compareTo(expected), expression);
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
   * Converts booleans, boolean text, and numbers by their decimal sign.
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
      "TRUE | true | false", "FALSE | false | false", "'true' | true | false", "'false' | false | false",
      "'True' | true | false", "'FALSE' | false | false", "' true ' | true | false", "'1' | true | false",
      "'0' | false | false", "1 | true | false", "0 | false | false", "42 | true | false", "-1 | true | false",
      "CAST(0.5 AS DOUBLE) | true | false", "CAST(0 AS DECIMAL(5,2)) | false | false",
      "CAST(NULL AS VARCHAR) | false | true", "CAST(NULL AS BOOLEAN) | false | true"
  }, delimiter = '|')
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
      "'abc'", "''", "'   '", "'2'", "'1.5'", "'NaN'", "DATE '2024-01-01'", "X'7879'"
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
        assertEquals(false, byLabel ? result.getBoolean("v") : result.getBoolean(1), expression);
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
