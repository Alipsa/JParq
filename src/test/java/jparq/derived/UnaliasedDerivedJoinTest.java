package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies anonymous derived inputs in joins without physical source tables.
 */
class UnaliasedDerivedJoinTest {

  /**
   * Checks both join positions, multiple anonymous inputs, and alias collisions.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void anonymousInputsJoinByVisibleColumns(boolean pushdown) throws SQLException {
    for (String source : List.of("(SELECT 1 AS a) JOIN (VALUES (2)) j(k) ON a + 1 = k",
        "(VALUES (2)) j(k) JOIN (SELECT 1 AS a) ON a + 1 = k",
        "(SELECT 1 AS a UNION SELECT 2) JOIN (VALUES (2)) j(k) ON a + 1 = k",
        "(VALUES (2)) j(k) JOIN (SELECT 1 AS a UNION SELECT 2) ON a + 1 = k", "(SELECT 1 AS a), (SELECT 2 AS k)",
        "(SELECT 1 AS a UNION SELECT 2), (SELECT 2 AS k UNION SELECT 3)",
        "(SELECT 1 AS a) JOIN (VALUES (2)) derived_0(k) ON a + 1 = derived_0.k",
        "(VALUES (2)) derived_1(k) JOIN (SELECT 1 AS a) ON a + 1 = derived_1.k",
        "(SELECT 1 AS a) JOIN (VALUES (2)) DERIVED_0(k) ON a + 1 = DERIVED_0.k",
        "(SELECT 1 AS a) JOIN (VALUES (2)) \"derived_0\"(k) ON a + 1 = \"derived_0\".k",
        "(SELECT 1 AS a), (SELECT 2 AS k) derived_0, (SELECT 3 AS z) _derived_0",
        "(SELECT 1 AS a), (SELECT 1 AS b), (VALUES (2)) j(k)")) {
      assertRows(pushdown, "SELECT a, k FROM " + source + " WHERE a = 1 AND k = 2", List.of(List.of(1, 2)));
    }
    assertRows(pushdown, "SELECT * FROM (SELECT 1 AS a UNION SELECT 2), (SELECT 3 AS b UNION SELECT 4) ORDER BY a, b",
        List.of(List.of(1, 3), List.of(1, 4), List.of(2, 3), List.of(2, 4)));
    assertRows(pushdown, "SELECT * FROM (SELECT 1 AS a) JOIN (VALUES (2)) j(k) ON a + 1 = k", List.of(List.of(1, 2)));
    assertRows(pushdown, "SELECT * FROM (SELECT 1 AS a UNION SELECT 2) JOIN (VALUES (2)) j(k) ON a = k",
        List.of(List.of(2, 2)));
    assertRows(pushdown, "SELECT * FROM (SELECT 1 AS a), (SELECT 1 AS a)", List.of(List.of(1, 1)));
    assertRows(pushdown, "SELECT x, x + 1 FROM (SELECT ARRAY[1, 2] AS a), UNNEST(a) AS u(x) ORDER BY x",
        List.of(List.of(1, 2), List.of(2, 3)));
  }

  /**
   * Keeps anonymous LATERAL inputs bound to each outer row, including set inputs.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void anonymousLateralInputsUseCurrentRow(boolean pushdown) throws SQLException {
    for (String body : List.of("SELECT v.n AS a FROM (VALUES (1), (2)) v(n) WHERE v.n = s.id",
        "SELECT v.n AS a FROM (VALUES (1), (2)) v(n) WHERE v.n = s.id"
            + " UNION SELECT v.n FROM (VALUES (9)) v(n) WHERE FALSE")) {
      assertRows(pushdown, "SELECT id, a FROM (VALUES (1), (2)) s(id) CROSS JOIN LATERAL (" + body + ") ORDER BY id",
          List.of(List.of(1, 1), List.of(2, 2)));
    }
    assertRows(pushdown,
        "SELECT * FROM (SELECT 1 AS a),"
            + " LATERAL (SELECT v.n AS b FROM (VALUES (2)) v(n) UNION SELECT v.n FROM (VALUES (2)) v(n)),"
            + " LATERAL (SELECT v.n AS c FROM (VALUES (3)) v(n) UNION SELECT v.n FROM (VALUES (3)) v(n))",
        List.of(List.of(1, 2, 3)));
  }

  /**
   * Retains visible output columns when an anonymous join input is empty.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void emptyAnonymousInputsRetainMetadata(boolean pushdown) throws SQLException {
    for (String body : List.of("SELECT CAST(1 AS INTEGER) AS a WHERE FALSE",
        "SELECT CAST(1 AS INTEGER) AS a WHERE FALSE UNION SELECT CAST(2 AS INTEGER) WHERE FALSE")) {
      assertRows(pushdown, "SELECT a, k FROM (" + body + ") JOIN (VALUES (2)) j(k) ON TRUE", List.of());
    }
  }

  /**
   * Prevents generated names from qualifying otherwise ambiguous output columns.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if JDBC setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void anonymousInputsDoNotExposeQualifiers(boolean pushdown) throws SQLException {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown=" + pushdown
        + "&projectionPushdown=" + pushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT derived_1.a FROM (SELECT 1 AS a) JOIN (SELECT 2 AS a) ON TRUE",
          "SELECT t1.a FROM (SELECT 1 AS a) JOIN (SELECT 2 AS a) ON TRUE",
          "SELECT derived_0.* FROM (SELECT 1 AS a) JOIN (SELECT 2 AS b) ON TRUE",
          "SELECT * FROM (SELECT ARRAY[1] AS a) JOIN UNNEST(derived_0.a) AS u(x) ON TRUE")) {
        assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
      }
      for (String qualifier : List.of("derived_0", "t0")) {
        String sql = "SELECT * FROM (SELECT 1 AS a) JOIN (SELECT 2 AS b) ON " + qualifier + ".a = b";
        SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql), sql);
        assertEquals("42000", error.getSQLState(), sql);
      }
    }
  }

  /**
   * Checks column labels, numeric values, and row counts through JDBC.
   *
   * @param pushdown
   *          whether both pushdown options are enabled
   * @param sql
   *          query to execute
   * @param expected
   *          expected rows in result order
   * @throws SQLException
   *           if JDBC execution fails
   */
  private static void assertRows(boolean pushdown, String sql, List<List<Integer>> expected) throws SQLException {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown=" + pushdown
        + "&projectionPushdown=" + pushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.prepareStatement(sql)) {
      for (int execution = 0; execution < 2; execution++) {
        try (var result = statement.executeQuery()) {
          var metadata = result.getMetaData();
          assertEquals(expected.isEmpty() ? 2 : expected.getFirst().size(), metadata.getColumnCount(), sql);
          if (expected.isEmpty()) {
            assertEquals("a", metadata.getColumnLabel(1), sql);
            assertEquals("k", metadata.getColumnLabel(2), sql);
          }
          for (int column = 1; column <= metadata.getColumnCount(); column++) {
            assertFalse(metadata.getColumnLabel(column).startsWith("derived_"), sql);
          }
          for (List<Integer> row : expected) {
            assertTrue(result.next(), sql);
            for (int column = 1; column <= row.size(); column++) {
              assertInstanceOf(Number.class, result.getObject(column), sql);
              assertEquals(row.get(column - 1).intValue(), result.getInt(column), sql);
            }
          }
          assertFalse(result.next(), sql);
        }
      }
    } catch (SQLException e) {
      throw new SQLException("Query failed: " + sql, e);
    }
  }
}
