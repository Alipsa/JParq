package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies unknown scalar functions fail instead of returning their SQL text.
 */
class UnsupportedFunctionTest {
  /**
   * Preserves the function name and syntax classification across JDBC boundaries.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rejectsUnknownFunctions(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown="
        + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String function : List.of("NO_SUCH_FUNCTION(cyl)", "DATEADD('DAY', 1, DATE '2024-01-01')",
          "cardinality(ARRAY[4,6])", "no_such_function()")) {
        String name = function.substring(0, function.indexOf('('));
        String body = "SELECT " + function + " AS x FROM mtcars WHERE cyl = 6 LIMIT 1";
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c",
            "SELECT (" + body + ") AS x", "SELECT cyl FROM mtcars WHERE " + function + " > 0",
            "SELECT cyl FROM mtcars WHERE mpg > " + function,
            "SELECT a.cyl FROM mtcars a JOIN mtcars b ON a.cyl = b.cyl WHERE b.mpg > " + function,
            "SELECT COUNT(" + function + ") FROM mtcars", "SELECT cyl FROM mtcars ORDER BY " + function + " LIMIT 1",
            "SELECT ABS(" + function + ") FROM mtcars LIMIT 1")) {
          SQLSyntaxErrorException failure = assertThrows(SQLSyntaxErrorException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("Unsupported function: " + name), failure::getMessage);
        }
      }
    }
  }
  /**
   * Rejects table-free calls while preserving selective expression evaluation.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void rejectsEvaluatedCallsOnly() throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT NO_SUCH_FUNCTION(1)", "SELECT no_such_function()")) {
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            result.getString(1);
          }
        }, sql);
        assertEquals("42000", failure.getSQLState());
        assertTrue(failure.getMessage().contains("Unsupported function:"), failure::getMessage);
      }
      try (var result = statement.executeQuery("SELECT CASE WHEN cyl = 6 THEN 1 ELSE NO_SUCH_FUNCTION(cyl) END AS x "
          + "FROM mtcars WHERE cyl = 6 LIMIT 1")) {
        assertTrue(result.next());
        assertEquals(1, result.getInt("x"));
        assertFalse(result.next());
      }
      try (var result = statement.executeQuery("SELECT NO_SUCH_FUNCTION(cyl) FROM mtcars WHERE 1 = 0")) {
        assertFalse(result.next());
      }
    }
  }

}
