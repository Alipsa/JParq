package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies predicate subqueries retain set and truth semantics inside grouped
 * scalars.
 */
class GroupedPredicateSubqueryTest {
  private static final List<Integer> CYLINDERS = List.of(4, 6, 8);
  private static final Map<Integer, Long> COUNTS = Map.of(4, 11L, 6, 7L, 8, 14L);

  /**
   * Supplies true, false, unknown, empty, multirow, negated, and correlated
   * predicates.
   *
   * @return predicate cases with both pushdown settings
   */
  static Stream<Arguments> predicates() {
    Map<String, List<Integer>> cases = new java.util.LinkedHashMap<>();
    cases.put("m.cyl IN (SELECT 4)", List.of(4));
    cases.put("COUNT(*) IN (SELECT 11 UNION ALL SELECT 14)", List.of(4, 8));
    cases.put("m.cyl IN (SELECT n.cyl FROM mtcars n WHERE n.cyl = m.cyl AND n.cyl = 8)", List.of(8));
    cases.put("m.cyl NOT IN (SELECT 4)", List.of(6, 8));
    cases.put("EXISTS (SELECT 1)", CYLINDERS);
    cases.put("EXISTS (SELECT 1 FROM mtcars n WHERE n.cyl = m.cyl AND n.cyl = 8)", List.of(8));
    cases.put("NOT EXISTS (SELECT 1 FROM mtcars n WHERE n.cyl = m.cyl AND n.cyl = 8)", List.of(4, 6));
    cases.put("COUNT(*) = ANY (SELECT 11)", List.of(4));
    cases.put("COUNT(*) > ALL (SELECT 10)", List.of(4, 8));
    cases.put("COUNT(*) = SOME (SELECT 7)", List.of(6));
    cases.put("COUNT(*) = ANY (SELECT COUNT(*) FROM mtcars n WHERE n.cyl = m.cyl)", CYLINDERS);
    cases.put("m.cyl = ALL (SELECT n.cyl FROM mtcars n WHERE n.cyl = m.cyl)", CYLINDERS);
    cases.put("COUNT(*) = ANY (SELECT CAST(NULL AS BIGINT))", List.of());
    cases.put("COUNT(*) > ALL (SELECT CAST(NULL AS BIGINT))", List.of());
    cases.put("COUNT(*) NOT IN (SELECT CAST(NULL AS BIGINT))", List.of());
    cases.put("m.cyl IN (SELECT 4 WHERE 1 = 0)", List.of());
    cases.put("EXISTS (SELECT 1 WHERE 1 = 0)", List.of());
    cases.put("COUNT(*) = ANY (SELECT 11 WHERE 1 = 0)", List.of());
    cases.put("COUNT(*) > ALL (SELECT 10 WHERE 1 = 0)", CYLINDERS);
    return Stream.of(true, false).flatMap(
        pushdown -> cases.entrySet().stream().map(entry -> Arguments.of(pushdown, entry.getKey(), entry.getValue())));
  }

  /**
   * Evaluates predicate subqueries within CASE in all grouped scalar contexts.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @param predicate
   *          condition containing a predicate subquery
   * @param matches
   *          cylinder groups where the condition is SQL TRUE
   * @throws Exception
   *           if fixture setup or query execution fails
   */
  @ParameterizedTest(name = "{index}: pushdown={0}, predicate={1}")
  @MethodSource("predicates")
  void evaluatesCasePredicates(boolean pushdown, String predicate, List<Integer> matches) throws Exception {
    try (var connection = connect(pushdown)) {
      String sql = "SELECT m.cyl, CASE WHEN " + predicate + " THEN COUNT(*) ELSE -1 END "
          + "FROM mtcars m GROUP BY m.cyl ORDER BY m.cyl";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
        for (int cylinder : CYLINDERS) {
          assertTrue(result.next(), sql);
          assertEquals(cylinder, result.getInt(1), sql);
          assertEquals(matches.contains(cylinder) ? COUNTS.get(cylinder) : -1L, result.getLong(2), sql);
        }
        assertFalse(result.next(), sql);
      }
      assertOrder(connection, "SELECT m.cyl FROM mtcars m GROUP BY m.cyl HAVING CASE WHEN " + predicate
          + " THEN 1 ELSE 0 END = 1 ORDER BY m.cyl", matches);
      List<Integer> order = new ArrayList<>(matches);
      CYLINDERS.stream().filter(cylinder -> !matches.contains(cylinder)).forEach(order::add);
      assertOrder(connection,
          "SELECT m.cyl FROM mtcars m GROUP BY m.cyl ORDER BY CASE WHEN " + predicate + " THEN 0 ELSE 1 END, m.cyl",
          order);
    }
  }

  /**
   * Preserves Boolean and NULL values for directly projected quantified
   * comparisons.
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
  void projectsThreeValuedPredicates(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT m.cyl, COUNT(*) > ALL (SELECT 10), "
            + "COUNT(*) = ANY (SELECT CAST(NULL AS BIGINT)), " + "COUNT(*) IN (SELECT CAST(NULL AS BIGINT)), "
            + "COUNT(*) > ALL (SELECT CAST(NULL AS BIGINT)), " + "NOT (COUNT(*) = ANY (SELECT CAST(NULL AS BIGINT))), "
            + "COALESCE(COUNT(*) IN (SELECT CAST(NULL AS BIGINT)), FALSE) "
            + "FROM mtcars m GROUP BY m.cyl ORDER BY m.cyl")) {
      for (int cylinder : CYLINDERS) {
        assertTrue(result.next());
        assertEquals(cylinder, result.getInt(1));
        assertEquals(cylinder != 6, result.getObject(2));
        assertNull(result.getObject(3));
        assertNull(result.getObject(4));
        assertNull(result.getObject(5));
        assertNull(result.getObject(6));
        assertEquals(Boolean.FALSE, result.getObject(7));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Supports predicate subqueries around a global aggregate without GROUP BY.
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
  void evaluatesGlobalAggregatePredicates(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CASE WHEN COUNT(*) IN (SELECT 32) THEN COUNT(*) ELSE -1 END, "
            + "CASE WHEN EXISTS (SELECT 1) THEN COUNT(*) ELSE -1 END, "
            + "COUNT(*) = ANY (SELECT 32), COUNT(*) > ALL (SELECT 31) FROM mtcars "
            + "HAVING CASE WHEN COUNT(*) IN (SELECT 32) THEN 1 ELSE 0 END = 1 "
            + "ORDER BY CASE WHEN EXISTS (SELECT 1) THEN 0 ELSE 1 END")) {
      assertTrue(result.next());
      assertEquals(32L, result.getLong(1));
      assertEquals(32L, result.getLong(2));
      assertEquals(Boolean.TRUE, result.getObject(3));
      assertEquals(Boolean.TRUE, result.getObject(4));
      assertFalse(result.next());
    }
  }

  /**
   * Keeps grouped-column checks active when a whole predicate is bound for
   * execution.
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
  void validatesPredicateOperands(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String where : List.of("", " WHERE m.cyl < 0")) {
        for (String predicate : List.of("m.mpg IN (SELECT 4)", "m.mpg = ANY (SELECT 4)", "m.mpg > ALL (SELECT 4)")) {
          String from = " FROM mtcars m" + where + " GROUP BY m.cyl";
          for (String sql : List.of("SELECT m.cyl, CASE WHEN " + predicate + " THEN COUNT(*) ELSE 0 END" + from,
              "SELECT m.cyl" + from + " HAVING CASE WHEN " + predicate + " THEN 1 ELSE 0 END = 1",
              "SELECT m.cyl" + from + " ORDER BY CASE WHEN " + predicate + " THEN 0 ELSE 1 END")) {
            SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
            assertEquals("42000", failure.getSQLState(), sql);
            assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure.getMessage());
          }
        }
      }
    }
  }

  /**
   * Rejects ungrouped correlations consistently before reading groups, including
   * empty input and predicates nested inside HAVING expressions.
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
  void validatesSubqueryCorrelationsWithoutGroups(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown); var statement = connection.createStatement()) {
      for (String where : List.of(" WHERE m.cyl < 0", "")) {
        String from = " FROM mtcars m" + where + " GROUP BY m.cyl";
        String subquery = "(SELECT n.gear FROM mtcars n WHERE n.gear = m.gear)";
        for (String predicate : List.of("EXISTS " + subquery, "m.cyl IN " + subquery, "COUNT(*) = ANY " + subquery,
            "COUNT(*) > ALL " + subquery, "COUNT(*) = (SELECT MAX(n.gear) FROM mtcars n WHERE n.gear = m.gear)")) {
          for (String sql : List.of("SELECT m.cyl, CASE WHEN " + predicate + " THEN 1 ELSE 0 END + COUNT(*)" + from,
              "SELECT m.cyl" + from + " HAVING " + predicate,
              "SELECT m.cyl" + from + " HAVING CASE WHEN " + predicate + " THEN 1 ELSE 0 END = 1",
              "SELECT m.cyl" + from + " ORDER BY CASE WHEN " + predicate + " THEN 0 ELSE 1 END")) {
            SQLException failure = assertThrows(SQLException.class, () -> statement.executeQuery(sql), sql);
            assertEquals("42000", failure.getSQLState(), sql);
            assertEquals("Failed to compute aggregate query: Subquery references ungrouped outer column 'gear'; "
                + "outer columns must appear in the GROUP BY clause", failure.getMessage(), sql);
          }
        }
      }
    }
  }

  /**
   * Compares cylinder values returned by a query in order.
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
  private static void assertOrder(Connection connection, String sql, List<Integer> expected) throws SQLException {
    List<Integer> actual = new ArrayList<>();
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        actual.add(result.getInt(1));
      }
    }
    assertEquals(expected, actual, sql);
  }

  /**
   * Opens the fixture database with the requested pushdown settings.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @return fixture connection
   * @throws Exception
   *           if fixture setup or connection creation fails
   */
  private static Connection connect(boolean pushdown) throws Exception {
    return DriverManager.getConnection(
        "jdbc:jparq:" + Path.of(GroupedPredicateSubqueryTest.class.getResource("/mtcars.parquet").toURI()).getParent()
            + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
  }
}
