package jparq.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies row counts, name binding, and grouping for parenthesized joins. */
class ParenthesizedJoinTest {

  /**
   * Resolves independent VALUES aliases alongside a named derived table (#284).
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
  void mixedDerivedAndValuesInputsRetainIndependentAliases(boolean pushdown) throws SQLException {
    assertRows(pushdown, "SELECT a, k, z FROM (SELECT 1 AS a) named, (VALUES (2)) first_input(k),"
        + " (VALUES (3)) second_input(z) WHERE a = 1 AND k = 2", List.of(List.of(1, 2, 3)));
  }

  /**
   * Keeps reused VALUES aliases independent when an empty derived input reports
   * types.
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
  void valuesAliasesRetainIndependentSchemas(boolean pushdown) throws SQLException {
    String sql = "SELECT a.y, d.y FROM ((VALUES ('s')) a(y) CROSS JOIN (VALUES (2)) b(z))"
        + " CROSS JOIN (SELECT a.y FROM (VALUES (1)) a(y) WHERE FALSE) d";
    try (var connection = DriverManager.getConnection(jdbcUrl(pushdown));
        var statement = connection.prepareStatement(sql)) {
      for (int execution = 0; execution < 2; execution++) {
        try (var result = statement.executeQuery()) {
          assertEquals(Types.VARCHAR, result.getMetaData().getColumnType(1));
          assertEquals(Types.INTEGER, result.getMetaData().getColumnType(2));
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Applies lowercase quoted-name fallback to qualifiers, columns, and ordering.
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
  void quotedLowercaseNamesResolveWithinGroups(boolean pushdown) throws SQLException {
    assertRows(pushdown, "SELECT \"m\".cyl FROM (mtcars m JOIN (SELECT 1 AS a) x ON TRUE) LIMIT 1",
        List.of(List.of(6)));
    assertRows(pushdown, "SELECT \"m\".\"cyl\" FROM (mtcars m JOIN (SELECT 1 AS a) x ON TRUE) LIMIT 1",
        List.of(List.of(6)));
    assertRows(pushdown, "SELECT \"m\".\"x\", n.x FROM ((VALUES (1), (2)) m(x) CROSS JOIN (VALUES (3)) n(x))"
        + " WHERE \"m\".\"x\" > 1 ORDER BY \"m\".\"x\" DESC", List.of(List.of(2, 3)));
    assertRows(pushdown, "SELECT COUNT(*) FROM (mtcars m JOIN (SELECT 1 AS a) x ON TRUE) WHERE \"m\".cyl = 4",
        List.of(List.of(11)));
    assertRows(pushdown, "SELECT \"g\".\"x\" FROM ((VALUES (1)) a(x) CROSS JOIN (VALUES (2)) b(y)) g",
        List.of(List.of(1)));
  }

  /**
   * Applies quoted lowercase residual predicates to physical and joined inputs.
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
  void quotedQualifiersRetainResidualPredicates(boolean pushdown) throws SQLException {
    for (String input : List.of("mtcars m", "mtcars m JOIN (SELECT 1 AS a) x ON TRUE",
        "(mtcars m JOIN (SELECT 1 AS a) x ON TRUE)")) {
      assertRows(pushdown, "SELECT COUNT(*) FROM " + input + " WHERE \"m\".cyl = 4", List.of(List.of(11)));
      assertRows(pushdown, "SELECT COUNT(*) FROM " + input + " WHERE (\"m\".cyl = 4) AND \"m\".hp > 100",
          List.of(List.of(2)));
      assertRows(pushdown, "SELECT COUNT(*) FROM " + input + " WHERE \"m\".cyl = 4 OR \"m\".cyl = 6",
          List.of(List.of(18)));
    }
    String input = "((VALUES (1), (1), (2)) m(x) CROSS JOIN (VALUES (3)) n(y))";
    assertRows(pushdown, "SELECT DISTINCT m.x AS renamed FROM " + input + " ORDER BY m.x DESC",
        List.of(List.of(2), List.of(1)));
    assertRows(pushdown, "SELECT m.x AS renamed FROM " + input + " ORDER BY m.x DESC",
        List.of(List.of(2), List.of(1), List.of(1)));
  }

  /**
   * Resolves quoted lowercase ON references against the inputs visible at each
   * join.
   *
   * @param pushdown
   *          whether both pushdown options are enabled
   * @throws SQLException
   *           if a supported query fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void quotedQualifiersResolveInJoinConditions(boolean pushdown) throws SQLException {
    assertRows(pushdown, "SELECT COUNT(*) FROM mtcars m JOIN (SELECT 4 AS a) x ON \"m\".cyl = x.a",
        List.of(List.of(11)));
    assertRows(pushdown,
        "SELECT COUNT(*) FROM mtcars o" + " JOIN (mtcars m JOIN (SELECT 1 AS a) x ON TRUE) ON o.model = \"m\".model",
        List.of(List.of(32)));
    assertRows(pushdown,
        "SELECT m.x, n.x FROM ((VALUES (1), (2)) m(x)" + " JOIN (VALUES (2)) n(x) ON \"m\".\"x\" = \"n\".\"x\")",
        List.of(List.of(2, 2)));
    assertRows(pushdown, "SELECT COUNT(*) FROM ((VALUES (1), (2)) m(x)"
        + " LEFT JOIN (VALUES (2)) n(y) ON \"m\".x = n.y) WHERE n.y IS NULL", List.of(List.of(1)));
    assertRows(pushdown, "SELECT COUNT(*) FROM (VALUES (1), (2)) \"M\"(x)" + " JOIN (VALUES (2)) n(y) ON \"M\".x = n.y",
        List.of(List.of(1)));
    try (var connection = DriverManager.getConnection(jdbcUrl(pushdown));
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT * FROM (VALUES (1)) m(x) JOIN (VALUES (1)) n(y) ON \"M\".x = n.y",
          "SELECT * FROM (VALUES (1)) m(x) JOIN (VALUES (1)) n(y) ON zz.x = n.y",
          "SELECT * FROM (VALUES (1)) m(x) JOIN (VALUES (1)) n(y) ON \"m\".missing = n.y",
          "SELECT * FROM (VALUES (1)) m(x) JOIN (VALUES (1)) n(y) ON \"p\".z = n.y"
              + " JOIN (VALUES (1)) p(z) ON TRUE")) {
        SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql), sql);
        assertEquals("42000", error.getSQLState(), sql);
        String diagnostic = sql.contains("ON \"M\".x") || sql.contains("ON zz.x")
            ? "Unknown table qualifier"
            : "Missing columns in JOIN condition";
        assertTrue(error.getMessage().contains(diagnostic), error.getMessage());
      }
    }
  }

  /**
   * Preserves quoted aliases whose text resembles an internal lookup prefix.
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
  void quotedPrefixNamesResolve(boolean pushdown) throws SQLException {
    for (String alias : List.of("u:x", "q:x", "U:x", "Q:x")) {
      String qualifier = "\"" + alias + "\"";
      String inputs = "(VALUES (1), (2)) " + qualifier + "(a) CROSS JOIN (VALUES (3)) b(c)";
      assertRows(pushdown, "SELECT " + qualifier + ".* FROM " + inputs, List.of(List.of(1), List.of(2)));
      assertRows(pushdown, "SELECT " + qualifier + ".a FROM (" + inputs + ") WHERE " + qualifier + ".a > 1",
          List.of(List.of(2)));
    }
  }

  /**
   * Preserves real table names for correlations when an outer table has an alias.
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
  void aliasedOuterTablesRetainRealNameCorrelations(boolean pushdown) throws SQLException {
    for (String input : List.of("mtcars m", "mtcars m JOIN (SELECT 1 AS a) x ON TRUE")) {
      for (String inner : List.of("mtcars i WHERE i.cyl = mtcars.cyl",
          "(VALUES (4),(6),(6)) v(c) WHERE v.c = mtcars.cyl")) {
        String sql = "SELECT m.model, (SELECT COUNT(*) FROM " + inner + ") AS c FROM " + input + " LIMIT 2";
        try (var connection = DriverManager.getConnection(jdbcUrl(pushdown));
            var statement = connection.createStatement();
            var result = statement.executeQuery(sql)) {
          for (String model : List.of("Mazda RX4", "Mazda RX4 Wag")) {
            assertTrue(result.next(), sql);
            assertEquals(model, result.getString(1), sql);
            assertEquals(inner.startsWith("mtcars") ? 7 : 2, result.getInt(2), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Exposes only the alias for ordinary aliased joined-input wildcards.
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
  void aliasedJoinTablesHideRealNameWildcards(boolean pushdown) throws SQLException {
    try (var connection = DriverManager.getConnection(jdbcUrl(pushdown));
        var statement = connection.createStatement()) {
      SQLException error = assertThrows(SQLException.class,
          () -> statement.executeQuery("SELECT mtcars.* FROM mtcars m JOIN (SELECT 1 AS a) x ON TRUE LIMIT 2"));
      assertEquals("42000", error.getSQLState());
      String sql = "SELECT m.* FROM mtcars m JOIN (SELECT 1 AS a) x ON TRUE LIMIT 2";
      try (var result = statement.executeQuery(sql)) {
        assertEquals(12, result.getMetaData().getColumnCount(), sql);
        for (String model : List.of("Mazda RX4", "Mazda RX4 Wag")) {
          assertTrue(result.next(), sql);
          assertEquals(model, result.getString("model"), sql);
          assertEquals(6, result.getInt("cyl"), sql);
        }
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Classifies duplicate-label group wildcards and accepts positional renaming.
   *
   * @param pushdown
   *          whether both pushdown options are enabled
   * @throws SQLException
   *           if connection setup or a supported query fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void duplicateGroupWildcardLabelsReportAmbiguity(boolean pushdown) throws SQLException {
    String group = "((VALUES (1)) a(x) JOIN (VALUES (2)) b(x) ON TRUE) g";
    try (var connection = DriverManager.getConnection(jdbcUrl(pushdown));
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT g.* FROM " + group,
          "SELECT g.* FROM (" + group + " CROSS JOIN (VALUES (3)) c(z))",
          "SELECT g.x FROM (" + group + " CROSS JOIN (VALUES (3)) c(z))",
          "SELECT g.* FROM ((" + group + " CROSS JOIN (VALUES (3)) c(z)) CROSS JOIN (VALUES (4)) e(w))",
          "SELECT d.* FROM ((SELECT 1 AS x, 2 AS x) d CROSS JOIN (VALUES (3)) b(y))",
          "WITH c AS (SELECT 1 AS x, 2 AS x) SELECT d.* FROM (c d CROSS JOIN (VALUES (3)) b(y))",
          "SELECT g.* FROM " + group + " CROSS JOIN (VALUES (3)) c(y)",
          "SELECT g.* FROM ((VALUES (1)) a(x) JOIN (VALUES (2, 3)) b(x, y) ON TRUE) g",
          "SELECT g.* FROM (mtcars m JOIN mtcars n ON m.model = n.model) g",
          "SELECT d.* FROM (SELECT 1 AS x, 2 AS x) d CROSS JOIN (VALUES (3)) b(y)",
          "WITH c AS (SELECT 1 AS x, 2 AS x) SELECT d.* FROM c d CROSS JOIN (VALUES (3)) b(y)")) {
        SQLException error = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals(sql.startsWith("SELECT g.x") ? "42702" : "42000", error.getSQLState(), sql);
        String diagnostic = sql.startsWith("SELECT g.x")
            ? "Ambiguous column reference"
            : "Ambiguous column in join alias";
        assertTrue(error.getMessage().contains(diagnostic), error.getMessage());
      }
    }
    assertRows(pushdown, "SELECT * FROM " + group, List.of(List.of(1, 2)));
    assertRows(pushdown, "SELECT g.* FROM " + group + "(left_x, right_x)", List.of(List.of(1, 2)));
    assertRows(pushdown, "SELECT h.* FROM " + group + " CROSS JOIN (VALUES (3)) h(y)", List.of(List.of(3)));
  }

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
      "SELECT a.y FROM ((VALUES (1)) a(x) CROSS JOIN (VALUES (2)) b(y))",
      "SELECT \"M\".cyl FROM (mtcars m JOIN (SELECT 1 AS a) x ON TRUE)",
      "SELECT m.\"Cyl\" FROM (mtcars m JOIN (SELECT 1 AS a) x ON TRUE)"
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
    for (String input : List.of("(VALUES (1)) a(x) FULL JOIN (VALUES (2)) b(x) USING (x)",
        "((VALUES (1)) a(x) FULL JOIN (VALUES (2)) b(x) USING (x))")) {
      assertRows(pushdown, "SELECT * FROM " + input + " ORDER BY x", List.of(List.of(1), List.of(2)));
    }
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
