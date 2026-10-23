package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that select-list subquery failures cross JDBC boundaries as SQL
 * exceptions.
 */
class ScalarSubqueryFailureTest {
  private static final Map<String, String> SUBQUERY_FAILURES = Map.of("SELECT COUNT(*) FROM optimized WHERE zz = 1",
      "Missing columns for table 'optimized': zz",
      "SELECT COUNT(*) FROM (SELECT id FROM optimized WHERE id <= 2) x(a, a)",
      "Duplicate derived table column alias: a");

  /**
   * Checks the streaming iteration boundary independently of projection getters.
   *
   * @param inner
   *          the failing scalar subquery
   * @param diagnostic
   *          the original syntax error expected in the cause chain
   * @throws Exception
   *           if connecting or reading the first row fails
   */
  @ParameterizedTest
  @MethodSource("subqueryFailures")
  void nextRetainsSubqueryFailure(String inner, String diagnostic) throws Exception {
    String url = "jdbc:jparq:" + fixtureDirectory();
    // The fixture starts with id = 1. DISTINCT currently streams its input;
    // CASE lets the first row succeed before a later row evaluates the subquery.
    try (var connection = DriverManager.getConnection(url);
        var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT DISTINCT CASE WHEN id = 1 THEN 0 ELSE (" + inner + ") END AS c FROM optimized")) {
      assertTrue(result.next());
      assertEquals(0, result.getInt(1), "The first fixture row must take the successful CASE branch");
      var failure = assertThrows(SQLSyntaxErrorException.class, result::next);
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains("Failed to execute sub query"), failure.getMessage());
      assertNestedSyntaxFailure(failure, diagnostic);
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
    String url = "jdbc:jparq:" + fixtureDirectory() + "?predicatePushdown=" + predicate + "&projectionPushdown="
        + projection;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (var entry : SUBQUERY_FAILURES.entrySet()) {
        for (String outer : new String[]{
            "", " FROM optimized", " FROM optimized ORDER BY id"
        }) {
          String sql = "SELECT (" + entry.getKey() + ") AS c" + outer;
          try (var result = statement.executeQuery(sql)) {
            assertTrue(result.next(), sql);
            var failure = assertThrows(SQLSyntaxErrorException.class, () -> result.getObject(1), sql);
            assertEquals("42000", failure.getSQLState(), sql);
            assertTrue(failure.getMessage().contains("Failed to execute sub query"), failure.getMessage());
            assertNestedSyntaxFailure(failure, entry.getValue());
          }
        }
      }
    }
  }

  /**
   * Supplies each failing subquery and its original diagnostic.
   *
   * @return subqueries paired with their expected diagnostics
   */
  private static Stream<Arguments> subqueryFailures() {
    return SUBQUERY_FAILURES.entrySet().stream().map(entry -> Arguments.of(entry.getKey(), entry.getValue()));
  }

  /**
   * Resolves the committed Parquet fixture directory.
   *
   * @return the fixture's parent directory
   * @throws URISyntaxException
   *           if the fixture URL cannot be converted to a URI
   */
  private static Path fixtureDirectory() throws URISyntaxException {
    var fixture = ScalarSubqueryFailureTest.class.getResource("/hf/optimized.parquet");
    assertNotNull(fixture);
    return Path.of(fixture.toURI()).getParent();
  }

  /**
   * Checks that an inner SQL exception retains the original diagnostic and state.
   *
   * @param failure
   *          the JDBC boundary exception
   * @param diagnostic
   *          the expected inner error message
   */
  private static void assertNestedSyntaxFailure(SQLException failure, String diagnostic) {
    for (Throwable cause = failure.getCause(); cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException sql && "42000".equals(sql.getSQLState()) && sql.getMessage() != null
          && sql.getMessage().contains(diagnostic)) {
        return;
      }
    }
    fail("Missing nested SQLState 42000 diagnostic: " + diagnostic);
  }
}
