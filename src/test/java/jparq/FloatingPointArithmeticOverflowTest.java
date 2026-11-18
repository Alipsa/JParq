package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies that floating-point arithmetic overflow reports SQLState 22003
 * (#323).
 */
class FloatingPointArithmeticOverflowTest {
  /** Expected value of an arithmetic or SUM expression. */
  private record Expectation(String expression, Double expected) {
  }

  /**
   * Rejects arithmetic on finite approximate operands that produces a non-finite
   * result, consistently through materialization.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void finiteOperandOverflowReportsSqlState(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = jdbcUrl() + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String expression : List.of("CAST(1e300 AS DOUBLE) * CAST(1e300 AS DOUBLE)",
          "CAST(-1e300 AS DOUBLE) * CAST(1e300 AS DOUBLE)",
          "CAST(1.7976931348623157e308 AS DOUBLE) + CAST(1.7976931348623157e308 AS DOUBLE)",
          "CAST(-1.7976931348623157e308 AS DOUBLE) - CAST(1.7976931348623157e308 AS DOUBLE)",
          "CAST(1e300 AS DOUBLE) / CAST(1e-300 AS DOUBLE)", "CAST(-1e300 AS DOUBLE) / CAST(1e-300 AS DOUBLE)",
          "CAST(1e38 AS REAL) * CAST(1e300 AS DOUBLE)")) {
        String body = "SELECT " + expression + " AS x FROM mtcars WHERE cyl = 6 LIMIT 1";
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c",
            "SELECT (" + body + ") AS x", "SELECT " + expression + " AS x",
            "SELECT cyl FROM mtcars WHERE " + expression + " > 0 LIMIT 1",
            "SELECT cyl FROM mtcars WHERE mpg > " + expression + " LIMIT 1")) {
          SQLDataException failure = assertThrows(SQLDataException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              assertTrue(result.next(), sql);
              result.getObject(1);
            }
          }, sql);
          assertEquals("22003", failure.getSQLState(), sql);
        }
      }
    }
  }

  /**
   * Rejects sums of finite DOUBLE values that exceed the DOUBLE range in
   * aggregate and window forms.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void sumOverflowReportsSqlState(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = jdbcUrl() + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      String sum = "SUM(CAST(1e308 AS DOUBLE))";
      for (String sql : List.of("SELECT " + sum + " AS x FROM mtcars",
          "SELECT x FROM (SELECT " + sum + " AS x FROM mtcars) d",
          "WITH c AS (SELECT " + sum + " AS x FROM mtcars) SELECT x FROM c",
          "SELECT cyl, " + sum + " AS x FROM mtcars GROUP BY cyl",
          "SELECT cyl FROM mtcars GROUP BY cyl HAVING " + sum + " > 0",
          "SELECT cyl, " + sum + " AS x FROM mtcars GROUP BY cyl ORDER BY x",
          "SELECT " + sum + " OVER () AS x FROM mtcars")) {
        SQLDataException failure = assertThrows(SQLDataException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals("22003", failure.getSQLState(), sql);
      }
    }
  }

  /**
   * Keeps finite approximate results, NULL zero-divisor results, and in-range
   * sums flowing through unchanged.
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
  void finiteResultsAndNullDivisorsArePreserved(boolean predicatePushdown, boolean projectionPushdown)
      throws Exception {
    String url = jdbcUrl() + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (Expectation expectation : List.of(new Expectation("CAST(1e300 AS DOUBLE) + CAST(1e300 AS DOUBLE)", 2.0E300),
          new Expectation("CAST(1.7976931348623157e308 AS DOUBLE) / CAST(1e300 AS DOUBLE)", 1.7976931348623157E8),
          new Expectation("CAST(10.5 AS DOUBLE) % CAST(2.0 AS DOUBLE)", 0.5),
          new Expectation("CAST(1 AS DOUBLE) / CAST(0 AS DOUBLE)", null),
          new Expectation("CAST(1 AS DOUBLE) % CAST(0 AS DOUBLE)", null))) {
        for (String sql : List.of("SELECT " + expectation.expression() + " AS x FROM mtcars WHERE cyl = 6 LIMIT 1",
            "SELECT " + expectation.expression() + " AS x")) {
          try (var result = statement.executeQuery(sql)) {
            assertTrue(result.next(), sql);
            Object value = result.getObject(1);
            if (expectation.expected() == null) {
              assertNull(value, sql);
            } else {
              assertEquals(expectation.expected(), assertInstanceOf(Double.class, value, sql), sql);
            }
            assertFalse(result.next(), sql);
          }
        }
      }
      double sum = BigDecimal.valueOf(1.0E300).multiply(BigDecimal.valueOf(32L)).doubleValue();
      for (String expression : List.of("SUM(CAST(1e300 AS DOUBLE))", "SUM(CAST(1e300 AS DOUBLE)) OVER ()")) {
        for (String sql : List.of("SELECT " + expression + " AS x FROM mtcars",
            "SELECT x FROM (SELECT " + expression + " AS x FROM mtcars) d",
            "WITH c AS (SELECT " + expression + " AS x FROM mtcars) SELECT x FROM c")) {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              assertEquals(sum, assertInstanceOf(Double.class, result.getObject(1), sql), sql);
            }
          }
        }
      }
    }
  }

  /**
   * Locates the committed Parquet fixture.
   *
   * @return the fixture directory's JDBC URL
   * @throws Exception
   *           if fixture lookup fails
   */
  private static String jdbcUrl() throws Exception {
    return "jdbc:jparq:"
        + Path.of(FloatingPointArithmeticOverflowTest.class.getResource("/mtcars.parquet").toURI()).getParent();
  }
}
