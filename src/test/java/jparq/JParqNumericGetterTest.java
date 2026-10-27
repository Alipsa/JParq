package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import se.alipsa.jparq.engine.NumericCastTarget;

/** Verifies numeric ResultSet getters for both column indexes and labels. */
class JParqNumericGetterTest {
  @TempDir
  Path directory;

  /**
   * Converts numeric text, preserving the original JDBC object and NULL status.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void convertsNumericText(NumericCastTarget target) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String text : List.of("1", " +12 ", "-12.75", "1.25e1")) {
        try (var result = statement.executeQuery("SELECT CAST('" + text + "' AS VARCHAR) AS v")) {
          assertTrue(result.next());
          assertEquals(text, result.getObject(1));
          double expected = Double.parseDouble(text);
          if (target != NumericCastTarget.REAL && target != NumericCastTarget.DOUBLE) {
            expected = (long) expected;
          }
          for (boolean byLabel : List.of(false, true)) {
            assertEquals(expected, read(result, target, byLabel).doubleValue());
            assertFalse(result.wasNull());
          }
          assertEquals(text, result.getObject("v"));
        }
      }
    }
  }

  /**
   * Keeps numbers, booleans, and SQL NULL consistent across both overloads.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void handlesNumbersAndNulls(NumericCastTarget target) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("42", "CAST(42 AS BIGINT)", "CAST(42 AS REAL)", "CAST(42 AS DOUBLE)",
          "CAST(42 AS DECIMAL(5,2))", "TRUE", "FALSE", "CAST(TRUE AS BOOLEAN)", "CAST(FALSE AS BOOLEAN)",
          "CAST(NULL AS VARCHAR)", "CAST(NULL AS INTEGER)", "CAST(NULL AS BOOLEAN)")) {
        boolean isNull = expression.contains("NULL");
        double expected = 42d;
        if (isNull || expression.contains("FALSE")) {
          expected = 0d;
        } else if (expression.contains("TRUE")) {
          expected = 1d;
        }
        try (var result = statement.executeQuery("SELECT " + expression + " AS v")) {
          assertTrue(result.next());
          for (boolean byLabel : List.of(false, true)) {
            assertEquals(expected, read(result, target, byLabel).doubleValue(), expression);
            assertEquals(isNull, result.wasNull(), expression);
          }
        }
      }
      try (var result = statement.executeQuery("SELECT v FROM (VALUES (NULL), ('7')) AS t(v)")) {
        assertTrue(result.next());
        assertEquals(0d, read(result, target, false).doubleValue());
        assertTrue(result.wasNull());
        assertTrue(result.next());
        assertEquals(7d, read(result, target, true).doubleValue());
        assertFalse(result.wasNull());
      }
    }
  }

  /**
   * Reports invalid text and incompatible values through checked data exceptions.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void rejectsInvalidValues(NumericCastTarget target) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("'not a number'", "''", "'   '", "'NaN'", "'Infinity'", "'-Infinity'",
          "DATE '2024-01-01'")) {
        try (var result = statement.executeQuery("SELECT " + expression + " AS v")) {
          assertTrue(result.next());
          for (boolean byLabel : List.of(false, true)) {
            var failure = assertThrows(SQLDataException.class, () -> read(result, target, byLabel), expression);
            assertEquals("22018", failure.getSQLState(), expression);
            assertTrue(failure.getMessage().contains("column 1 ('v')"), failure::getMessage);
            Throwable cause = failure.getCause();
            while (cause != null && !(cause instanceof NumberFormatException)) {
              cause = cause.getCause();
            }
            assertInstanceOf(NumberFormatException.class, cause);
            assertFalse(result.wasNull());
          }
        }
      }
    }
  }

  /**
   * Names the column label and position when either overload fails conversion.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void conversionDiagnosticsIncludeColumnLabel() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 1 AS id, 'bad' AS price")) {
      assertTrue(result.next());
      for (boolean byLabel : List.of(false, true)) {
        var failure = assertThrows(SQLDataException.class, () -> {
          if (byLabel) {
            result.getInt("price");
          } else {
            result.getInt(2);
          }
        });
        assertEquals("22018", failure.getSQLState());
        assertTrue(failure.getMessage().contains("column 2 ('price') as INTEGER"), failure::getMessage);
      }
    }
  }

  /**
   * Rejects text overflow instead of wrapping integers or producing infinity.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void rejectsOverflow(NumericCastTarget target) throws SQLException {
    String overflow = switch (target) {
      case TINYINT -> "128";
      case SMALLINT -> "32768";
      case INTEGER -> "2147483648";
      case BIGINT -> "9223372036854775808";
      case REAL -> "1e39";
      case DOUBLE -> "1e309";
    };
    String negativeOverflow = switch (target) {
      case REAL, DOUBLE -> "-" + overflow;
      default -> new BigInteger(overflow).negate().subtract(BigInteger.ONE).toString();
    };
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("'" + overflow + "'", "'" + negativeOverflow + "'")) {
        try (var result = statement.executeQuery("SELECT " + expression + " AS v")) {
          assertTrue(result.next());
          for (boolean byLabel : List.of(false, true)) {
            var failure = assertThrows(SQLDataException.class, () -> read(result, target, byLabel), expression);
            assertEquals("22003", failure.getSQLState(), expression);
          }
        }
      }
    }
  }

  /**
   * Accepts integer boundary values without losing precision during conversion.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(value = NumericCastTarget.class, names = {
      "TINYINT", "SMALLINT", "INTEGER", "BIGINT"
  })
  void acceptsIntegerBoundaries(NumericCastTarget target) throws SQLException {
    long maximum = switch (target) {
      case TINYINT -> Byte.MAX_VALUE;
      case SMALLINT -> Short.MAX_VALUE;
      case INTEGER -> Integer.MAX_VALUE;
      default -> Long.MAX_VALUE;
    };
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (long boundary : List.of(maximum, -maximum - 1)) {
        String text = boundary + (boundary < 0 ? ".9" : ".0");
        try (var result = statement.executeQuery("SELECT '" + text + "' AS v")) {
          assertTrue(result.next());
          for (boolean byLabel : List.of(false, true)) {
            assertEquals(boundary, read(result, target, byLabel).longValue());
          }
        }
      }
    }
  }

  /**
   * Converts numeric VARCHAR values produced by a correlated LATERAL query.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void convertsLateralNumericText(NumericCastTarget target) throws SQLException {
    String sql = "SELECT x.v FROM (VALUES (1), (2)) AS a(n) "
        + "CROSS JOIN LATERAL (SELECT a.n / 1 AS v FROM (VALUES (1)) AS seed(z)) x ORDER BY x.v";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      for (int expected = 1; expected <= 2; expected++) {
        assertTrue(result.next());
        assertInstanceOf(String.class, result.getObject(1));
        for (boolean byLabel : List.of(false, true)) {
          assertEquals((double) expected, read(result, target, byLabel).doubleValue());
          assertFalse(result.wasNull());
        }
      }
      assertFalse(result.next());
    }
  }

  /**
   * Calls the getter overload matching the target and access mode.
   *
   * @param result
   *          the positioned result set
   * @param target
   *          the getter's numeric target
   * @param byLabel
   *          whether to use the column label instead of its index
   * @return the boxed primitive result
   * @throws SQLException
   *           if retrieval or conversion fails
   */
  private static Number read(ResultSet result, NumericCastTarget target, boolean byLabel) throws SQLException {
    return switch (target) {
      case TINYINT -> byLabel ? result.getByte("v") : result.getByte(1);
      case SMALLINT -> byLabel ? result.getShort("v") : result.getShort(1);
      case INTEGER -> byLabel ? result.getInt("v") : result.getInt(1);
      case BIGINT -> byLabel ? result.getLong("v") : result.getLong(1);
      case REAL -> byLabel ? result.getFloat("v") : result.getFloat(1);
      case DOUBLE -> byLabel ? result.getDouble("v") : result.getDouble(1);
    };
  }
}
