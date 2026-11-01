package jparq;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeIntegerFixture;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.PredicateSubqueryValidation;
import se.alipsa.jparq.engine.SqlParser;

/** Verifies single-column cardinality for predicate subqueries (issue 259). */
class PredicateSubqueryColumnCountTest {
  @TempDir
  Path directory;

  /**
   * Rejects extra columns before interpreting empty, NULL, or matching results.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture setup or querying fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rejectsMultipleColumns(boolean predicate, boolean projection) throws Exception {
    writeIntegerFixture(directory);
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String subquery : List.of("SELECT i.n, i.n FROM o i", "SELECT i.n, i.n FROM o i WHERE i.n > 10",
          "SELECT i.n, i.n FROM o i WHERE i.n IS NULL", "SELECT i.n, i.n FROM o i WHERE i.n = o.n",
          "SELECT i.n, i.n FROM o i WHERE i.n = o.n AND i.n > 10", "SELECT * FROM (SELECT 1 AS a, 2 AS b) t")) {
        for (String operator : List.of("IN", "NOT IN", "= ANY", "= SOME", "= ALL")) {
          String condition = "n " + operator + " (" + subquery + ")";
          for (String clause : List.of(" WHERE ", " GROUP BY n HAVING ")) {
            String sql = "SELECT n FROM o" + clause + condition;
            for (boolean prepared : List.of(true, false)) {
              var error = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
              assertEquals("42000", error.getSQLState(), sql);
              assertTrue(error.getMessage().contains("exactly one column"), error.getMessage());
            }
          }
        }
      }
    }
  }

  /**
   * Rejects invalid degrees during preparation even if no predicate is evaluated.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture setup or querying fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rejectsUnevaluatedSubqueries(boolean predicate, boolean projection) throws Exception {
    writeIntegerFixture(directory);
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String subquery : List.of("SELECT n, n FROM o", "SELECT i.n, o.n FROM o i WHERE i.n = o.n",
          "SELECT * FROM (SELECT n, n FROM o) t", "SELECT t.* FROM (SELECT n, n FROM o) t",
          "WITH t AS (SELECT n, n FROM o) SELECT * FROM t", "SELECT * FROM o a CROSS JOIN o b",
          "SELECT n, n FROM o UNION ALL SELECT n, n FROM o", "SELECT n FROM o WHERE n IN (SELECT n, n FROM o)",
          "SELECT n FROM (SELECT n FROM o WHERE n IN (SELECT n, n FROM o)) t", "VALUES (1, 2), (3, 4)",
          "SELECT * FROM (VALUES (1, 2), (3, 4)) t", "SELECT a.*, b.* FROM (o a JOIN o b ON a.n = b.n)")) {
        for (String operator : List.of("IN", "NOT IN", "= ANY", "= SOME", "= ALL")) {
          String condition = "n " + operator + " (" + subquery + ")";
          for (String sql : List.of("SELECT n FROM o WHERE n = 99 AND " + condition,
              "SELECT n FROM o WHERE FALSE AND " + condition, "SELECT n FROM o WHERE TRUE OR " + condition,
              "SELECT n FROM o WHERE n > 10 GROUP BY n HAVING " + condition,
              "SELECT n FROM o GROUP BY n HAVING TRUE OR " + condition)) {
            var error = assertThrows(SQLSyntaxErrorException.class, () -> {
              try (var ignored = connection.prepareStatement(sql)) {
                // Degree validation must fail before any statement is executed.
              }
            }, sql);
            assertEquals("42000", error.getSQLState(), sql);
            assertTrue(error.getMessage().contains("exactly one column"), error.getMessage());
          }
        }
      }
      assertEquals(List.of(), rows(connection, "SELECT n FROM o WHERE n = 99 AND n IN (SELECT 1 / 0)", false));
      for (String subquery : List.of("SELECT a.* FROM o a CROSS JOIN o b", "SELECT * FROM o a JOIN o b USING(n)",
          "WITH t(x) AS (SELECT n FROM o) SELECT * FROM t", "VALUES (1), (2)",
          "SELECT a.* FROM (o a JOIN o b ON a.n = b.n)")) {
        assertEquals(List.of(), rows(connection, "SELECT n FROM o WHERE n = 99 AND n IN (" + subquery + ")", false));
      }
    }
  }

  /**
   * Resolves qualifiers inside unaliased join groups and preserves source error
   * diagnostics when predicates execute.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture setup or querying fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void preservesParenthesizedJoinWildcards(boolean predicate, boolean projection) throws Exception {
    writeIntegerFixture(directory);
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String subquery : List.of("SELECT a.* FROM (o a JOIN o b ON a.n = b.n)",
          "SELECT b.* FROM (o a JOIN o b ON a.n = b.n)", "SELECT a.* FROM ((o a JOIN o b ON a.n = b.n))")) {
        assertEquals(List.of(List.of(1), List.of(1), List.of(2), List.of(3)),
            rows(connection, "SELECT n FROM o WHERE n IN (" + subquery + ") ORDER BY n", false));
      }
      for (String subquery : List.of("SELECT x.* FROM o", "SELECT * FROM o a NATURAL JOIN o b")) {
        var error = assertThrows(SQLException.class,
            () -> rows(connection, "SELECT n FROM o WHERE n IN (" + subquery + ")", false));
        assertFalse(error.getMessage().contains("Subquery must return exactly one column"), error.getMessage());
      }
    }
  }

  /**
   * Leaves unresolved wildcard shapes to source and runtime validation.
   *
   * @param subquery
   *          query with an unknown qualifier or unsupported join shape
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT x.* FROM o", "SELECT * FROM o a NATURAL JOIN o b"
  })
  void defersUnresolvedWildcardDegrees(String subquery) {
    var query = (SqlParser.Select) SqlParser.parseQuery("SELECT n FROM o WHERE n IN (" + subquery + ")");
    assertDoesNotThrow(() -> PredicateSubqueryValidation.validate(query, table -> Set.of("n")));
  }

  /**
   * Keeps one-column multirow, NULL, and empty-input semantics intact.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture setup or querying fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void preservesSingleColumnSemantics(boolean predicate, boolean projection) throws Exception {
    writeIntegerFixture(directory);
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String clause : List.of(" WHERE ", " GROUP BY n HAVING ")) {
        var matches = clause.contains("HAVING")
            ? List.of(List.of(1), List.of(2))
            : List.of(List.of(1), List.of(1), List.of(2));
        for (String operator : List.of("IN", "= ANY", "= SOME")) {
          assertEquals(matches, rows(connection,
              "SELECT n FROM o" + clause + "n " + operator + " (SELECT n FROM o WHERE n <= 2) ORDER BY n", false));
        }
        for (String operator : List.of("IN", "NOT IN", "= ANY", "= SOME", "= ALL")) {
          boolean all = operator.equals("= ALL") || operator.equals("NOT IN");
          assertEquals(List.of(List.of(all ? 1 : 0)), rows(connection,
              "SELECT CASE WHEN NULL " + operator + " (SELECT n FROM o WHERE n > 10) THEN 1 ELSE 0 END", false));
          assertEquals(List.of(), rows(connection,
              "SELECT n FROM o" + clause + "n " + operator + " (SELECT n FROM o WHERE n IS NULL)", false));
        }
        // EXISTS accepts multiple columns because it tests only row existence.
        assertEquals(List.of(List.of(3)), rows(connection,
            "SELECT n FROM o" + clause + "EXISTS (SELECT i.n, i.n FROM o i WHERE i.n = o.n) AND n = 3", false));
      }
    }
  }
}
