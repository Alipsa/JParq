package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies positional alias lists on ordinary and parenthesized CTE references.
 */
class CteReferenceAliasTest {

  /**
   * Checks projections, wildcard labels, grouping, and the exact issue 211 join.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void aliasesExposeOnlyRenamedColumns(boolean predicate, boolean projection) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown=" + predicate
        + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String declaration : List.of("c", "c(k, n)")) {
        String prefix = "WITH " + declaration + " AS (SELECT id, id + 1 AS n FROM optimized WHERE id <= 2) ";
        for (String source : List.of("c x(a, b)", "c AS x(a, b)", "(c) x(a, b)", "((c)) x(a, b)")) {
          for (String query : List.of("SELECT a, b FROM " + source + " ORDER BY a",
              "SELECT * FROM " + source + " ORDER BY a",
              "SELECT MIN(a) AS a, b FROM " + source + " GROUP BY b ORDER BY a",
              "SELECT x.a, x.b FROM " + source + " JOIN (VALUES (1)) s(z) ON x.a >= s.z ORDER BY x.a")) {
            String sql = prefix + query;
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              assertEquals("a", result.getMetaData().getColumnLabel(1), sql);
              assertEquals("b", result.getMetaData().getColumnLabel(2), sql);
              for (int id = 1; id <= 2; id++) {
                assertTrue(result.next(), sql);
                assertEquals(id, result.getInt(1), sql);
                assertEquals(id + 1, result.getInt(2), sql);
              }
              assertFalse(result.next(), sql);
            }
          }
        }
      }
      for (String source : List.of("r x(m)", "r AS x(m)", "(r) x(m)")) {
        String sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 2) " + "SELECT m FROM "
            + source + " ORDER BY m";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          for (int n = 1; n <= 2; n++) {
            assertTrue(result.next(), sql);
            assertEquals(n, result.getInt(1), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
      // Both references reuse one immutable CTE materialization with distinct alias
      // lists.
      String sql = "WITH c AS (SELECT id FROM optimized WHERE id <= 2) "
          + "SELECT x.a, y.b FROM c x(a) JOIN c y(b) ON x.a = y.b ORDER BY x.a";
      try (var statement = connection.prepareStatement(sql)) {
        for (int execution = 0; execution < 2; execution++) {
          try (var result = statement.executeQuery()) {
            for (int id = 1; id <= 2; id++) {
              assertTrue(result.next());
              assertEquals(id, result.getInt(1));
              assertEquals(id, result.getInt(2));
            }
            assertFalse(result.next());
          }
        }
      }
      String correlated = "SELECT o.v, (WITH c AS (SELECT o.v AS n) SELECT x.m FROM c x(m)) "
          + "FROM (VALUES (1), (2)) o(v) ORDER BY o.v";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(correlated)) {
        for (int value = 1; value <= 2; value++) {
          assertTrue(result.next());
          assertEquals(value, result.getInt(1));
          assertEquals(value, result.getInt(2));
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Rejects invalid alias lists and old column names even for empty CTEs.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @throws Exception
   *           if JDBC preparation fails unexpectedly
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void validatesDeclaredAliases(boolean predicate, boolean projection) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown=" + predicate
        + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String filter : List.of("id <= 2", "id < 0")) {
        String prefix = "WITH c AS (SELECT id, id + 1 AS n FROM optimized WHERE " + filter + ") ";
        for (String suffix : List.of("SELECT id FROM c x(a, b)", "SELECT n FROM c AS x(a, b)", "SELECT * FROM c x(a)",
            "SELECT * FROM c AS x(a, b, d)", "SELECT * FROM c x(a, a)")) {
          String sql = prefix + suffix;
          SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
          assertEquals("42000", failure.getSQLState(), sql);
        }
      }
    }
  }
}
