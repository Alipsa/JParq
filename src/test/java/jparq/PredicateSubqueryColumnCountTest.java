package jparq;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeIntegerFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
