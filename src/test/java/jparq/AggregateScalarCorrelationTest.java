package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies that scalar subqueries in aggregate arguments use input-row
 * bindings.
 */
class AggregateScalarCorrelationTest {
  /**
   * Compares direct and wrapped aggregates against independently verified fixture
   * totals, with and without a colliding projection alias.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture resolution or query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void aggregatesUseEachInputRow(boolean predicate, boolean projection) throws Exception {
    var directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String scalar = "(SELECT MAX(n.gear) FROM mtcars n WHERE n.mpg = m.mpg)";
    try (
        var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
        var statement = connection.createStatement()) {
      Map<Integer, Long> expected = new TreeMap<>(Map.of(4, 46L, 6, 28L, 8, 47L));
      for (String input : List.of("mtcars m", "(SELECT cyl, mpg FROM mtcars) m")) {
        for (String group : List.of("m.cyl", "m.cyl AS mpg")) {
          for (int multiplier : List.of(1, 2)) {
            String sql = "SELECT " + group + ", SUM(" + scalar + ")" + (multiplier == 1 ? "" : " * 2") + " FROM "
                + input + " GROUP BY m.cyl ORDER BY m.cyl";
            try (var result = statement.executeQuery(sql)) {
              for (var entry : expected.entrySet()) {
                assertTrue(result.next(), sql);
                assertEquals(entry.getKey().intValue(), result.getInt(1), sql);
                assertEquals(entry.getValue() * multiplier, result.getLong(2), sql);
              }
              assertFalse(result.next(), sql);
            }
          }
        }
      }
    }
  }

  /**
   * Preserves row bindings through aliases, joins, NULLs, HAVING, and ORDER BY.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rowScopeSurvivesGroupedExpressions(boolean predicate, boolean projection) throws Exception {
    try (
        var connection = DriverManager
            .getConnection("jdbc:jparq:.?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
        var statement = connection.createStatement()) {
      String scalar = "(SELECT o.v + x.v)";
      String from = " FROM (VALUES (1, 2), (1, 5), (2, 1), (2, NULL), (3, NULL)) o(g, v)"
          + " CROSS JOIN (VALUES (10)) x(v)";
      try (var result = statement.executeQuery("SELECT o.g, SUM(" + scalar + ") AS v, COUNT(" + scalar + "), MIN("
          + scalar + "), MAX(" + scalar + "), AVG(" + scalar + ")" + from + " GROUP BY o.g HAVING COUNT(" + scalar
          + ") > 0 ORDER BY SUM(" + scalar + ") + 1")) {
        assertTrue(result.next());
        assertEquals(2, result.getInt(1));
        assertEquals(11, result.getLong(2));
        assertEquals(1, result.getLong(3));
        assertEquals(11, result.getInt(4));
        assertEquals(11, result.getInt(5));
        assertEquals(11.0, result.getDouble(6));
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
        assertEquals(27, result.getLong(2));
        assertEquals(2, result.getLong(3));
        assertEquals(12, result.getInt(4));
        assertEquals(15, result.getInt(5));
        assertEquals(13.5, result.getDouble(6));
        assertFalse(result.next());
      }
      try (var result = statement
          .executeQuery("SELECT SUM((SELECT o.v)), COUNT((SELECT o.v))" + " FROM (VALUES (NULL), (2), (5)) o(v)")) {
        assertTrue(result.next());
        assertEquals(7, result.getLong(1));
        assertEquals(2, result.getLong(2));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Keeps scalar cardinality and syntax diagnostics at the aggregate JDBC
   * boundary.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void failuresKeepSqlState(boolean predicate, boolean projection) throws Exception {
    try (
        var connection = DriverManager
            .getConnection("jdbc:jparq:.?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
        var statement = connection.createStatement()) {
      for (String scalar : List.of("SELECT x.v FROM (VALUES (1), (2)) x(v) WHERE x.v >= o.v",
          "SELECT x.v, x.v FROM (VALUES (1)) x(v) WHERE x.v = o.v")) {
        for (String suffix : List.of("", " * 2")) {
          String sql = "SELECT SUM((" + scalar + "))" + suffix + " FROM (VALUES (1)) o(v)";
          SQLException failure = assertThrows(SQLException.class, () -> statement.executeQuery(sql), sql);
          assertEquals(scalar.contains("x.v, x.v") ? "42000" : "21000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains(scalar.contains("x.v, x.v") ? "one column" : "one row"),
              failure.getMessage());
        }
      }
    }
  }
}
