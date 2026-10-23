package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies that select-list subquery failures cross JDBC boundaries as SQL
 * exceptions.
 */
class ScalarSubqueryFailureTest {

  /**
   * Checks the streaming iteration boundary independently of projection getters.
   *
   * @param inner
   *          the failing scalar subquery
   * @throws Exception
   *           if connecting or reading the first row fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT COUNT(*) FROM optimized WHERE zz = 1",
      "SELECT COUNT(*) FROM (SELECT id FROM optimized WHERE id <= 2) x(a, a)"
  })
  void nextRetainsSubqueryFailure(String inner) throws Exception {
    var fixture = getClass().getResource("/hf/optimized.parquet");
    assertNotNull(fixture);
    String url = "jdbc:jparq:" + Path.of(fixture.toURI()).getParent();
    try (var connection = DriverManager.getConnection(url);
        var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT DISTINCT CASE WHEN id = 1 THEN 0 ELSE (" + inner + ") END AS c FROM optimized")) {
      assertTrue(result.next());
      var failure = assertThrows(SQLSyntaxErrorException.class, result::next);
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains("Failed to execute sub query"), failure.getMessage());
      assertNotNull(failure.getCause());
      assertTrue(result.isClosed());
    }
  }

  /**
   * Exercises the issue's queries with and without a physical outer input.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if connecting to the fixture fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void selectListFailuresRetainSyntaxState(boolean predicate, boolean projection) throws Exception {
    var fixture = getClass().getResource("/hf/optimized.parquet");
    assertNotNull(fixture);
    Path directory = Path.of(fixture.toURI()).getParent();
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String inner : new String[]{
          "SELECT COUNT(*) FROM optimized WHERE zz = 1",
          "SELECT COUNT(*) FROM (SELECT id FROM optimized WHERE id <= 2) x(a, a)"
      }) {
        for (String outer : new String[]{
            "", " FROM optimized", " FROM optimized ORDER BY id"
        }) {
          String sql = "SELECT (" + inner + ") AS c" + outer;
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("Failed to execute sub query"), failure.getMessage());
          assertNotNull(failure.getCause(), sql);
        }
      }
    }
  }
}
