package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.function.NumericFunctions;

/**
 * Regression coverage for grouped scalar scope, diagnostics, and sort
 * evaluation.
 */
class AggregateExpressionReviewTest {
  /**
   * Rejects ungrouped HAVING operands before scanning even an empty input.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if fixture setup or connection creation fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void rejectsUngroupedHaving(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String where : List.of("", " WHERE cyl < 0")) {
        for (String predicate : List.of("COALESCE(mpg, 1) > 0", "ABS(mpg) > 0", "mpg > 0",
            "CASE WHEN mpg IS NULL THEN 1 ELSE 0 END > 0")) {
          assertSyntax(connection, "SELECT cyl FROM mtcars" + where + " GROUP BY cyl HAVING " + predicate);
        }
      }
      assertCylinderOrder(connection,
          "SELECT cyl, COUNT(*) AS n FROM mtcars GROUP BY cyl HAVING COALESCE(n, 0) > 10 ORDER BY cyl", List.of(4, 8));
      assertCylinderOrder(connection, "SELECT cyl FROM mtcars GROUP BY cyl HAVING COALESCE(cyl, 0) > 4 ORDER BY cyl",
          List.of(6, 8));
      assertCylinderOrder(connection, "SELECT cyl FROM mtcars GROUP BY cyl HAVING EXISTS "
          + "(SELECT 1 FROM mtcars m WHERE m.cyl = cyl) ORDER BY cyl", List.of(4, 6, 8));
      assertCylinderOrder(connection,
          "SELECT cyl FROM mtcars GROUP BY cyl HAVING COUNT(*) = ANY(SELECT 11) ORDER BY cyl", List.of(4));
    }
  }

  /**
   * Preserves SQLState 42000 for grouped planner rejections and unsupported
   * windows.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if fixture setup or connection creation fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void classifiesPlannerFailures(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String sql : List.of("SELECT SUM(cyl) + mpg FROM mtcars", "SELECT SUM(SUM(cyl)) FROM mtcars",
          "SELECT 1 FROM mtcars ORDER BY mpg + COUNT(*)", "SELECT mpg FROM mtcars ORDER BY COUNT(*)",
          "SELECT cyl, SUM(COUNT(*)) OVER () FROM mtcars GROUP BY cyl",
          "SELECT cyl FROM mtcars GROUP BY cyl HAVING SUM(COUNT(*)) OVER () > 0",
          "SELECT cyl FROM mtcars GROUP BY cyl ORDER BY SUM(COUNT(*)) OVER ()",
          "SELECT cyl, ABS(SUM(COUNT(*)) OVER ()) FROM mtcars GROUP BY cyl")) {
        assertSyntax(connection, sql);
      }
    }
  }

  /**
   * Supports grouped ORDER BY subqueries and validates their outer references.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if fixture setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void ordersBySubqueries(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertCylinderOrder(connection, "SELECT cyl, COUNT(*) FROM mtcars GROUP BY cyl ORDER BY COUNT(*) + (SELECT 1)",
          List.of(6, 4, 8));
      assertCylinderOrder(connection, "SELECT m.cyl FROM mtcars m GROUP BY m.cyl ORDER BY COUNT(*) + "
          + "(SELECT MAX(n.cyl) FROM mtcars n WHERE n.cyl = m.cyl) DESC", List.of(8, 4, 6));
      for (String where : List.of("", " WHERE m.cyl < 0")) {
        String sql = "SELECT m.cyl FROM mtcars m" + where + " GROUP BY m.cyl ORDER BY COUNT(*) + "
            + "(SELECT MAX(n.cyl) FROM mtcars n WHERE n.mpg = m.mpg)";
        try (var statement = connection.createStatement()) {
          SQLException failure = assertThrows(SQLException.class, () -> statement.executeQuery(sql));
          assertEquals("42000", failure.getSQLState(), sql);
        }
      }
    }
  }

  /**
   * Keeps correlated predicates inside aggregate arguments in the input-row
   * scope.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if fixture setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void keepsAggregateArgumentScope(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT m.cyl, SUM(CASE WHEN m.gear IN "
            + "(SELECT n.gear FROM mtcars n WHERE n.mpg = m.mpg) THEN 1 ELSE 0 END) + 1 "
            + "FROM mtcars m GROUP BY m.cyl ORDER BY m.cyl")) {
      for (int[] row : List.of(new int[]{
          4, 12
      }, new int[]{
          6, 8
      }, new int[]{
          8, 15
      })) {
        assertTrue(result.next());
        assertEquals(row[0], result.getInt(1));
        assertEquals(row[1], result.getInt(2));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Evaluates each scalar sort key once per group instead of once per comparison.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if fixture setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void evaluatesSortKeysOnce(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown); var numeric = mockStatic(NumericFunctions.class, CALLS_REAL_METHODS)) {
      assertCylinderOrder(connection, "SELECT cyl FROM mtcars GROUP BY cyl ORDER BY ABS(COUNT(*)) DESC",
          List.of(8, 4, 6));
      numeric.verify(() -> NumericFunctions.evaluate(eq("ABS"), anyList()), times(3));
    }
  }

  /**
   * Asserts that a syntax failure occurs during preparation without exposing
   * bindings.
   *
   * @param connection
   *          fixture connection
   * @param sql
   *          query to prepare
   */
  private static void assertSyntax(Connection connection, String sql) {
    SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
    assertEquals("42000", failure.getSQLState(), sql);
    assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure.getMessage());
  }

  /**
   * Compares the ordered cylinder values from a query.
   *
   * @param connection
   *          fixture connection
   * @param sql
   *          query to execute
   * @param expected
   *          expected cylinder values
   * @throws SQLException
   *           if query execution fails
   */
  private static void assertCylinderOrder(Connection connection, String sql, List<Integer> expected)
      throws SQLException {
    List<Integer> actual = new ArrayList<>();
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        actual.add(result.getInt(1));
      }
    }
    assertEquals(expected, actual, sql);
  }

  /**
   * Opens the fixture database with both pushdown options set consistently.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @return fixture connection
   * @throws Exception
   *           if fixture setup or connection creation fails
   */
  private static Connection connect(boolean pushdown) throws Exception {
    return DriverManager.getConnection(
        "jdbc:jparq:" + Path.of(AggregateExpressionReviewTest.class.getResource("/mtcars.parquet").toURI()).getParent()
            + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
  }
}
