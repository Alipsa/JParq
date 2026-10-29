package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies unsupported scalar and window functions report syntax errors.
 */
class UnsupportedFunctionTest {
  /**
   * Identifies unsupported ordered-set aggregates without calling them windows.
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
  void rejectsUnsupportedWithinGroup(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown="
        + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String expression : List.of("LISTAGG(model, ',') WITHIN GROUP (ORDER BY model)",
          "STRING_AGG(model, ',') WITHIN GROUP (ORDER BY model)", "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mpg)")) {
        String name = expression.substring(0, expression.indexOf('('));
        for (String suffix : List.of("", " WHERE 1 = 0")) {
          String sql = "SELECT " + expression + " FROM mtcars" + suffix;
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("WITHIN GROUP is not supported: " + name), failure::getMessage);
          assertFalse(failure.getMessage().contains("Unsupported window function"), failure::getMessage);
        }
      }
    }
  }

  /**
   * Rejects unsupported window functions with their names and SQL syntax state.
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
  void rejectsUnsupportedWindowFunctions(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown="
        + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String name : List.of("NO_SUCH", "no_such", "ABS")) {
        String expression = name + "(cyl) OVER ()";
        String body = "SELECT " + expression + " AS x FROM mtcars";
        for (String sql : List.of(body, body + " WHERE 1 = 0", "SELECT ABS(" + expression + ") FROM mtcars",
            "SELECT ROW_NUMBER() OVER (), " + expression + " FROM mtcars", "SELECT x FROM (" + body + ") d",
            "WITH c AS (" + body + ") SELECT x FROM c", "SELECT (" + body + " LIMIT 1) AS x")) {
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("Unsupported window function: " + name), failure::getMessage);
        }
      }
    }
  }

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
        String joinFunction = function.replace("(cyl)", "(a.cyl)");
        String body = "SELECT " + function + " AS x FROM mtcars WHERE cyl = 6 LIMIT 1";
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c",
            "SELECT (" + body + ") AS x", "SELECT cyl FROM mtcars WHERE " + function + " > 0",
            "SELECT cyl FROM mtcars WHERE mpg > " + function,
            "SELECT a.cyl FROM mtcars a JOIN mtcars b ON a.cyl = b.cyl WHERE b.mpg > " + joinFunction,
            "SELECT a.cyl FROM mtcars a JOIN mtcars b ON " + joinFunction + " > b.mpg",
            "SELECT DISTINCT " + function + " FROM mtcars",
            "SELECT " + function + " FROM mtcars UNION SELECT cyl FROM mtcars",
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
   * Rejects aggregate calls in predicates evaluated before grouping.
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
  void rejectsAggregatesInRowPredicates(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown="
        + projectionPushdown;
    Map<String, String> queries = Map.of("COALESCE(MAX(cyl), 0) > 0", "MAX", "UPPER(MAX(model)) = 'X'", "MAX",
        "ROUND(AVG(mpg), 1) > 0", "AVG", "SUM(cyl) + 1 > 0", "SUM", "COUNT(*) > 1", "COUNT", "NULLIF(MIN(cyl), 0) > 0",
        "MIN", "UPPER(STRING_AGG(model, ',')) = 'X'", "STRING_AGG", "ROUND(average(mpg), 1) > 0", "average");
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (var query : queries.entrySet()) {
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var result = statement.executeQuery("SELECT cyl FROM mtcars WHERE " + query.getKey())) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, query.getKey());
        assertEquals("42000", failure.getSQLState(), query.getKey());
        String message = "Aggregate function " + query.getValue() + " requires an aggregate query context";
        assertTrue(failure.getMessage().contains(message), failure::getMessage);
      }
    }
  }

  /**
   * Rejects unsupported parser nodes and invalid unary signs at JDBC boundaries.
   *
   * @param sql
   *          the query containing an unsupported expression
   * @param diagnostic
   *          expected error detail
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SELECT JSON_ARRAYAGG(model) FROM mtcars LIMIT 1|Unsupported expression:",
      "SELECT TIMESTAMP '2024-01-01 00:00:00' AT TIME ZONE 'UTC'|Unsupported expression:",
      "SELECT -TRUE|Unary sign requires", "SELECT +TRUE|Unary sign requires", "SELECT 7 DIV 2|Unsupported expression:",
      "SELECT 6 & 3|Unsupported expression:", "SELECT (ARRAY[1,2])[1]|Unsupported expression:",
      "SELECT JSON_EXISTS('{}', '$.a')|Unsupported JSON function:"
  })
  void rejectsUnsupportedExpressions(String sql, String diagnostic) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      }, sql);
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains(diagnostic), failure::getMessage);
    }
  }

  /**
   * Explains why window-only function names cannot be evaluated without OVER.
   *
   * @param expression
   *          a window-only function without its window specification
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "ROW_NUMBER()", "RANK()", "DENSE_RANK()", "PERCENT_RANK()", "CUME_DIST()", "NTILE(2)", "LAG(cyl)", "LEAD(cyl)",
      "FIRST_VALUE(cyl)", "LAST_VALUE(cyl)", "NTH_VALUE(cyl, 2)"
  })
  void explainsMissingOverClause(String expression) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
        try (var result = statement.executeQuery("SELECT " + expression + " FROM mtcars LIMIT 1")) {
          assertTrue(result.next());
          result.getObject(1);
        }
      });
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains("requires an OVER clause"), failure::getMessage);
    }
  }

  /**
   * Specifies the desired grouped scalar behavior tracked in issue 247.
   *
   * @param sql
   *          grouped query with a supported scalar function
   * @param expectedRows
   *          expected number of groups
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @Disabled("#247: scalar functions in HAVING and grouped select lists")
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SELECT cyl, COUNT(*) FROM mtcars GROUP BY cyl HAVING NULLIF(cyl, 4) IS NOT NULL|2",
      "SELECT cyl FROM mtcars GROUP BY cyl HAVING COALESCE(MAX(mpg), 0) > 1|3",
      "SELECT NULLIF(cyl, 4) FROM mtcars GROUP BY cyl|3"
  })
  void evaluatesGroupedScalarFunctions(String sql, int expectedRows) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      int rows = 0;
      while (result.next()) {
        result.getObject(1);
        rows++;
      }
      assertEquals(expectedRows, rows);
    }
  }

  /**
   * Specifies the desired syntax classification for unknown functions in HAVING.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Disabled("#247: syntax classification in HAVING")
  @Test
  void rejectsUnknownHavingFunction() throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
        try (var result = statement.executeQuery("SELECT cyl FROM mtcars GROUP BY cyl HAVING NO_SUCH(MAX(mpg)) > 1")) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      });
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains("Unsupported function: NO_SUCH"), failure::getMessage);
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
