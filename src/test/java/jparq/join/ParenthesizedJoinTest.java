package jparq.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies row counts, name binding, and grouping for parenthesized joins. */
class ParenthesizedJoinTest {

  /**
   * Keeps every input and projected column from the issue's physical-table joins.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void physicalJoinGroupsRetainInputs(boolean pushdown) throws SQLException {
    assertRows(pushdown, "SELECT COUNT(*) FROM (mtcars m JOIN (SELECT 1 AS a UNION SELECT 2) x ON TRUE)",
        List.of(List.of(64)));
    String url = jdbcUrl(pushdown);
    try (var connection = DriverManager.getConnection(url);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT * FROM (mtcars m JOIN (SELECT 1 AS a) x ON TRUE)")) {
      assertEquals(13, result.getMetaData().getColumnCount());
      assertEquals("a", result.getMetaData().getColumnLabel(13));
      int count = 0;
      while (result.next()) {
        assertEquals(1, result.getInt("a"));
        count++;
      }
      assertEquals(32, count);
    }
    assertRows(pushdown, "SELECT COUNT(*) FROM (mtcars m JOIN (SELECT 1 AS a) x ON TRUE) WHERE m.cyl = 4 AND x.a = 1",
        List.of(List.of(11)));
    assertRows(pushdown,
        "SELECT COUNT(*) FROM mtcars m JOIN (mtcars n JOIN (SELECT 1 AS a) x ON TRUE) ON m.model = n.model",
        List.of(List.of(32)));
  }

  /**
   * Resolves qualified columns and wildcards across nesting and group aliases.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void groupsPreserveNamesAndScopes(boolean pushdown) throws SQLException {
    String group = "((VALUES (1), (2)) m(a) JOIN (VALUES (2), (3)) n(b) ON m.a + 1 = n.b)";
    List<List<Integer>> rows = List.of(List.of(1, 2), List.of(2, 3));
    assertRows(pushdown, "SELECT m.a, n.b FROM " + group + " ORDER BY n.b DESC", List.of(List.of(2, 3), List.of(1, 2)));
    assertRows(pushdown, "SELECT m.*, n.* FROM " + group + " ORDER BY m.a", rows);
    assertRows(pushdown, "SELECT g.a, g.b FROM " + group + " g ORDER BY g.a", rows);
    assertRows(pushdown, "SELECT g.x, g.y FROM " + group + " g(x, y) ORDER BY g.x", rows);
    assertRows(pushdown, "SELECT * FROM (SELECT m.a, n.b FROM " + group + ") d ORDER BY a", rows);
    assertRows(pushdown, "WITH c AS (SELECT m.a, n.b FROM " + group + ") SELECT * FROM c ORDER BY a", rows);
    assertRows(pushdown, "SELECT m.a, n.b, p.c FROM (" + group + " CROSS JOIN (VALUES (4)) p(c)) ORDER BY m.a",
        List.of(List.of(1, 2, 4), List.of(2, 3, 4)));
    assertRows(pushdown, "SELECT m.a, n.a FROM ((VALUES (1)) m(a) JOIN (VALUES (2)) n(a) ON TRUE)",
        List.of(List.of(1, 2)));
    assertRows(pushdown,
        "SELECT \"Left input\".*, \"Right input\".* FROM "
            + "((VALUES (1)) \"Left input\"(\"A value\") JOIN (VALUES (2)) \"Right input\"(\"B value\") ON TRUE)",
        List.of(List.of(1, 2)));
    assertRows(pushdown, "SELECT * FROM ((VALUES (1)) a(x), (VALUES (2)) b(y))", List.of(List.of(1, 2)));
    assertRows(pushdown, "WITH c(x) AS (SELECT 1) SELECT a.x, b.x FROM (c a CROSS JOIN c b)", List.of(List.of(1, 1)));
  }

  /**
   * Retains nested qualifiers when scalar and LATERAL queries bind outer values.
   *
   * @param pushdown
   *          whether both pushdown options are enabled
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void groupedInputsSupportCorrelation(boolean pushdown) throws SQLException {
    for (String condition : List.of("a.x = b.y", "x = y")) {
      assertRows(pushdown, "SELECT a.x, (SELECT COUNT(*) FROM ((VALUES (1), (2)) a(x) JOIN (VALUES (2)) b(y) ON "
          + condition + ") g(px, py) WHERE g.px = 2) FROM (VALUES (8)) a(x)", List.of(List.of(8, 1)));
    }
    assertRows(pushdown, "SELECT a.x, (SELECT COUNT(*) FROM ((VALUES (1), (2)) a(x) CROSS JOIN (VALUES (3)) b(y))"
        + " WHERE a.x = 2) FROM (VALUES (8)) a(x)", List.of(List.of(8, 1)));
    assertRows(pushdown,
        "SELECT o.x, (SELECT COUNT(*) FROM ((VALUES (1), (2)) a(y) JOIN (VALUES (1)) b(z) ON a.y = b.z)"
            + " WHERE a.y = o.x) FROM (VALUES (1), (2)) o(x) ORDER BY o.x",
        List.of(List.of(1, 1), List.of(2, 0)));
    assertRows(pushdown,
        "SELECT a.x, (SELECT a.x + b.y) FROM ((VALUES (1), (2)) a(x) CROSS JOIN (VALUES (3)) b(y)) ORDER BY a.x",
        List.of(List.of(1, 4), List.of(2, 5)));
    assertRows(pushdown,
        "SELECT a.x, c.z FROM ((VALUES (1), (2)) a(x) CROSS JOIN (VALUES (3)) b(y))"
            + " CROSS JOIN LATERAL (SELECT CAST(a.x + b.y AS INTEGER) AS z FROM (VALUES (1)) v(k)) c ORDER BY a.x",
        List.of(List.of(1, 4), List.of(2, 5)));
    assertRows(pushdown,
        "SELECT a.x, c.z FROM ((VALUES (1), (2)) a(x)"
            + " CROSS JOIN LATERAL (SELECT CAST(a.x + 1 AS INTEGER) AS z FROM (VALUES (1)) v(k)) c) ORDER BY a.x",
        List.of(List.of(1, 2), List.of(2, 3)));
    assertRows(pushdown, "SELECT u.x FROM ((SELECT ARRAY[1, 2] AS a) m CROSS JOIN (SELECT ARRAY[3] AS a) n)"
        + " CROSS JOIN UNNEST(m.a) u(x) ORDER BY u.x", List.of(List.of(1), List.of(2)));
  }

  /**
   * Reports invalid aliases and names instead of returning partial join results.
   *
   * @param sql
   *          invalid grouped join query
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT * FROM ((VALUES (1)) a(x) JOIN (VALUES (2)) b(y) ON a.missing = b.y)",
      "SELECT * FROM ((VALUES (1)) a(x) CROSS JOIN (VALUES (2)) b(y)) g(p)",
      "SELECT * FROM ((VALUES (1)) a(x) CROSS JOIN (VALUES (2)) b(y)) g(p, p)",
      "SELECT * FROM ((VALUES (1)) a(x) CROSS JOIN (VALUES (2)) b(y)) g JOIN (VALUES (3)) c(z) ON a.x = c.z",
      "SELECT a.y FROM ((VALUES (1)) a(x) CROSS JOIN (VALUES (2)) b(y))"
  })
  void invalidGroupedInputsReportSyntaxErrors(String sql) throws SQLException {
    try (var connection = DriverManager.getConnection(jdbcUrl(true)); var statement = connection.createStatement()) {
      SQLException error = assertThrows(SQLException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      }, sql);
      assertEquals("42000", error.getSQLState(), sql);
    }
  }

  /**
   * Preserves the grouping boundaries that determine outer-join null extension.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void outerJoinGroupingControlsNullExtension(boolean pushdown) throws SQLException {
    assertRows(pushdown, "SELECT a.x, b.y, c.z FROM (VALUES (1)) a(x) LEFT JOIN "
        + "((VALUES (2)) b(y) JOIN (VALUES (3)) c(z) ON b.y = c.z) ON TRUE", List.of(Arrays.asList(1, null, null)));
    assertRows(pushdown, "SELECT a.x, b.y, c.z FROM ((VALUES (1)) a(x) LEFT JOIN (VALUES (2)) b(y) ON FALSE)"
        + " JOIN (VALUES (3)) c(z) ON TRUE", List.of(Arrays.asList(1, null, 3)));
    assertRows(pushdown, "SELECT a.x, b.y FROM ((VALUES (1)) a(x) RIGHT JOIN (VALUES (2)) b(y) ON FALSE)",
        List.of(Arrays.asList(null, 2)));
    assertRows(pushdown,
        "SELECT a.x, b.y FROM ((VALUES (1)) a(x) FULL JOIN (VALUES (2)) b(y) ON FALSE) ORDER BY a.x NULLS LAST",
        List.of(Arrays.asList(1, null), Arrays.asList(null, 2)));
    assertRows(pushdown, "SELECT * FROM ((VALUES (1), (2)) a(x) FULL JOIN (VALUES (2), (3)) b(x) USING (x)) ORDER BY x",
        List.of(List.of(1), List.of(2), List.of(3)));
    assertRows(pushdown, "SELECT a.x FROM ((VALUES (1)) a(x) JOIN (SELECT 2 AS y WHERE FALSE) b ON TRUE)", List.of());
  }

  /**
   * Preserves parentheses around a single input and bound parameters in groups.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void singleInputsAndParametersRemainSupported(boolean pushdown) throws SQLException {
    for (String sql : List.of("SELECT COUNT(*) FROM (mtcars)", "SELECT COUNT(*) FROM ((mtcars)) m",
        "SELECT COUNT(*) FROM (mtcars m) JOIN (SELECT 1 AS a) x ON TRUE",
        "SELECT COUNT(*) FROM mtcars m JOIN (mtcars n) ON m.model = n.model")) {
      assertRows(pushdown, sql, List.of(List.of(32)));
    }
    try (var connection = DriverManager.getConnection(jdbcUrl(pushdown));
        var statement = connection
            .prepareStatement("SELECT x.a FROM (mtcars m JOIN (SELECT ? AS a) x ON TRUE) LIMIT 1")) {
      for (int value : List.of(1, 2)) {
        statement.setInt(1, value);
        try (var result = statement.executeQuery()) {
          assertTrue(result.next());
          assertEquals(value, result.getInt(1));
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Compares numeric rows while retaining SQL NULL values.
   *
   * @param pushdown
   *          whether both pushdown options are enabled
   * @param sql
   *          query to execute
   * @param expected
   *          expected result rows
   * @throws SQLException
   *           if query execution fails
   */
  private static void assertRows(boolean pushdown, String sql, List<List<Integer>> expected) throws SQLException {
    try (var connection = DriverManager.getConnection(jdbcUrl(pushdown));
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      List<List<Integer>> actual = new ArrayList<>();
      while (result.next()) {
        List<Integer> row = new ArrayList<>();
        for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
          String value = result.getString(column);
          row.add(value == null ? null : Integer.valueOf(value));
        }
        actual.add(row);
      }
      assertEquals(expected, actual, sql);
    }
  }

  /**
   * Builds the fixture URL with matching pushdown settings.
   *
   * @param pushdown
   *          whether both pushdown options are enabled
   * @return fixture JDBC URL
   */
  private static String jdbcUrl(boolean pushdown) {
    return "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown=" + pushdown
        + "&projectionPushdown=" + pushdown;
  }
}
