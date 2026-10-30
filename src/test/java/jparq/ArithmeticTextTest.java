package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies text conversion in arithmetic across JDBC evaluation boundaries. */
class ArithmeticTextTest {
  /**
   * Classifies invalid text in either operand and either numeric family.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture lookup or JDBC setup fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void invalidText(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown="
        + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String text : List.of("'abc'", "''", "'   '", "'NaN'", "'Infinity'", "model")) {
        for (String operator : List.of("+", "-", "*", "/", "%")) {
          for (String number : List.of("1", "CAST(1 AS DOUBLE)")) {
            for (String expression : List.of(text + operator + number, number + operator + text)) {
              assertInvalid(statement, expression, "22018");
            }
          }
        }
        for (String expression : List.of("-" + text, "+" + text)) {
          assertInvalid(statement, expression, "22018");
        }
      }
      for (String text : List.of("'1e400'", "'-1e400'")) {
        for (String operator : List.of("+", "-", "*", "/", "%")) {
          String number = "CAST(1 AS DOUBLE)";
          for (String expression : List.of(text + operator + number, number + operator + text)) {
            assertInvalid(statement, expression, "22003");
          }
        }
      }
    }
  }

  /**
   * Trims numeric text consistently with CAST and preserves arithmetic results.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture lookup or JDBC execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void numericText(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown="
        + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String text : List.of("5", " 5", "5 ", " 5 ", "\t5\n")) {
        String value = "'" + text + "'";
        for (String expression : List.of(value + " + 1", "1 + " + value, value + " - 1", "10 - " + value,
            value + " * 2", "2 * " + value, value + " / 2", "10 / " + value, value + " % 2", "12 % " + value,
            "-" + value, "+" + value, value + " + CAST(1 AS DOUBLE)", "CAST(1 AS DOUBLE) + " + value)) {
          String expectedExpression = expression.replace(value, "5");
          BigDecimal expected;
          try (var result = statement.executeQuery("SELECT " + expectedExpression)) {
            assertTrue(result.next());
            expected = result.getBigDecimal(1);
          }
          for (String sql : List.of("SELECT " + expression, "SELECT " + expression + " FROM mtcars LIMIT 1",
              "SELECT cyl FROM mtcars WHERE " + expression + " = " + expected + " LIMIT 1")) {
            try (var result = statement.executeQuery(sql)) {
              assertTrue(result.next(), sql);
              if (!sql.contains("WHERE")) {
                assertEquals(0, expected.compareTo(result.getBigDecimal(1)), sql);
              }
              assertFalse(result.next(), sql);
            }
          }
        }
      }
    }
  }

  /**
   * Checks direct, materialized, and predicate evaluation of one invalid operand.
   *
   * @param statement
   *          the JDBC statement
   * @param expression
   *          arithmetic containing invalid or out-of-range numeric text
   * @param sqlState
   *          expected conversion failure state
   */
  private void assertInvalid(Statement statement, String expression, String sqlState) {
    String body = "SELECT " + expression + " AS x FROM mtcars LIMIT 1";
    for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c",
        "SELECT cyl FROM mtcars WHERE " + expression + " > 0 LIMIT 1",
        "SELECT cyl FROM mtcars WHERE cyl > " + expression + " LIMIT 1")) {
      var failure = assertThrows(SQLDataException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          result.getObject(1);
        }
      }, sql);
      assertEquals(sqlState, failure.getSQLState(), sql);
      boolean hasParseCause = false;
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        hasParseCause |= "22018".equals(sqlState)
            ? cause instanceof NumberFormatException
            : cause instanceof ArithmeticException;
      }
      assertTrue(hasParseCause, sql);
    }
    if (!expression.contains("model")) {
      String sql = "SELECT " + expression;
      var failure = assertThrows(SQLDataException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          result.getObject(1);
        }
      }, sql);
      assertEquals(sqlState, failure.getSQLState(), sql);
    }
  }
}
