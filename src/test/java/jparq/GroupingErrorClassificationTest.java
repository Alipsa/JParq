package jparq;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeIntegerFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks consistent grouping-error classification across clauses and input
 * kinds.
 */
class GroupingErrorClassificationTest {

  /**
   * Classifies ungrouped references without confusing unknown-column errors.
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
  void classifiesUngroupedReferencesConsistently(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    for (String options : List.of("", "?projectionPushdown=false&predicatePushdown=false")) {
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + options)) {
        for (String source : List.of("o t", "(SELECT n FROM o) t", "o t JOIN (VALUES (1)) v(x) ON TRUE")) {
          for (String filter : List.of("", " WHERE 1 = 0")) {
            String from = " FROM " + source + filter;
            String grouped = from + " GROUP BY n + 1";
            for (String sql : List.of("SELECT COUNT(*)" + grouped + " ORDER BY n", "SELECT n, COUNT(*)" + grouped,
                "SELECT SUM(n) + n" + from, "SELECT COUNT(*)" + grouped + " HAVING n > 1",
                "SELECT COUNT(*)" + grouped + " ORDER BY (SELECT MAX(i.n) FROM o i WHERE i.n > t.n)",
                "SELECT (SELECT t.n)" + grouped, "SELECT COUNT(*)" + grouped + " HAVING (SELECT t.n) > 1",
                "SELECT GROUPING(n)" + grouped, "SELECT GROUPING(t.n)" + grouped, "SELECT GROUPING((n))" + grouped)) {
              var failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
              assertEquals("42803", failure.getSQLState(), sql);
            }
            for (String sql : List.of("SELECT absent, COUNT(*)" + grouped,
                "SELECT COUNT(*)" + grouped + " HAVING absent > 1",
                "SELECT COUNT(*)" + grouped + " ORDER BY absent + 1", "SELECT GROUPING(absent)" + grouped,
                "SELECT GROUPING(t.absent)" + grouped)) {
              var failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
              assertEquals("42000", failure.getSQLState(), sql);
            }
          }
        }
        for (String filter : List.of("", " WHERE 1 = 0")) {
          String sql = "SELECT n AS x, COUNT(*) FROM (SELECT n, n + 10 AS x FROM o) t" + filter
              + " GROUP BY n ORDER BY x * -1";
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
          assertEquals("42803", failure.getSQLState(), sql);
        }
      }
    }
  }
}
