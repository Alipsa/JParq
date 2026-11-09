package jparq;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeIntegerFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Regression coverage for SELECT aliases used inside ORDER BY expressions. */
class OrderByAliasExpressionTest {

  /**
   * Checks computed and aggregate aliases through physical, derived, and joined
   * inputs.
   *
   * @param prepared
   *          whether to use prepared statements
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void resolvesAliasesInsideExpressions(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    for (String options : List.of("", "?projectionPushdown=false&predicatePushdown=false")) {
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + options)) {
        for (String source : List.of("o t", "(SELECT n FROM o) t", "o t JOIN (VALUES (1)) v(x) ON TRUE")) {
          for (String[] example : List.of(new String[]{
              "n * 2 AS s", "s * 2", "(n * 2) * 2"
          }, new String[]{
              "n * 2 AS s", "-s", "-(n * 2)"
          }, new String[]{
              "UPPER(CAST(n AS VARCHAR)) AS s", "LOWER(s)", "LOWER(UPPER(CAST(n AS VARCHAR)))"
          }, new String[]{
              "-n AS n", "n + 1", "(-t.n) + 1"
          }, new String[]{
              "-n AS n", "t.n + 1", "t.n + 1"
          }, new String[]{
              "n + 1 AS a, -n AS b", "a * b", "(n + 1) * (-n)"
          }, new String[]{
              "n + 1 AS renamed", "\"renamed\" * 2", "(n + 1) * 2"
          })) {
            String query = "SELECT " + example[0] + " FROM " + source + " ORDER BY ";
            assertEquals(rows(connection, query + example[2] + " DESC NULLS LAST", prepared),
                rows(connection, query + example[1] + " DESC NULLS LAST", prepared), query + example[1]);
          }
          for (String[] example : List.of(new String[]{
              "SUM(n) AS s", "s * 2", "SUM(n) * 2"
          }, new String[]{
              "COUNT(*) AS c", "COALESCE(c, 0)", "COALESCE(COUNT(*), 0)"
          }, new String[]{
              "SUM(n) + 1 AS s", "-s", "-(SUM(n) + 1)"
          })) {
            String query = "SELECT n, " + example[0] + " FROM " + source + " GROUP BY n ORDER BY ";
            assertEquals(rows(connection, query + example[2] + " DESC NULLS LAST, n", prepared),
                rows(connection, query + example[1] + " DESC NULLS LAST, n", prepared), query + example[1]);
          }
        }
        String wildcard = "SELECT t.*, -n AS s FROM o t ORDER BY ";
        assertEquals(rows(connection, wildcard + "(-n) + 1 NULLS LAST", prepared),
            rows(connection, wildcard + "s + 1 NULLS LAST", prepared));
        String scope = "SELECT -n AS s FROM o ORDER BY ";
        assertEquals(rows(connection, scope + "(-n) + (SELECT s FROM (VALUES (10)) v(s)) NULLS LAST", prepared),
            rows(connection, scope + "s + (SELECT s FROM (VALUES (10)) v(s)) NULLS LAST", prepared));
      }
    }
  }

  /**
   * Checks ambiguity and quoted-name diagnostics even when the input is empty.
   *
   * @param prepared
   *          whether to use prepared statements
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void validatesExpressionAliasNames(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String source : List.of("o", "o JOIN (VALUES (1)) v(x) ON TRUE")) {
        for (String filter : List.of("", " WHERE 1 = 0")) {
          String query = "SELECT n + 1 AS s, -n AS s FROM " + source + filter + " ORDER BY s * 2";
          SQLException ambiguous = assertThrows(SQLException.class, () -> rows(connection, query, prepared));
          assertEquals("42702", ambiguous.getSQLState(), query);
          String repeated = "SELECT n + 1 AS s, n + 1 AS s FROM " + source + filter + " ORDER BY ";
          assertEquals(rows(connection, repeated + "(n + 1) * 2", prepared),
              rows(connection, repeated + "s * 2", prepared));
          String quoted = "SELECT -n AS renamed FROM " + source + filter + " ORDER BY \"Renamed\" * 2";
          SQLException missing = assertThrows(SQLException.class, () -> rows(connection, quoted, prepared));
          assertEquals("42000", missing.getSQLState(), quoted);
        }
      }
    }
  }
}
