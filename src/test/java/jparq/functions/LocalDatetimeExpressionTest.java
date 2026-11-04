package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression tests for local datetime value functions in nested expressions.
 */
class LocalDatetimeExpressionTest {

  /**
   * Evaluates local datetime keywords inside constructors and scalar functions
   * over physical tables, same-named derived columns, and grouped inputs.
   *
   * @param expression
   *          expression containing a local datetime keyword
   * @throws Exception
   *           if the query fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LOCALTIMESTAMP", "LOCALTIME", "ARRAY[LOCALTIMESTAMP]", "ARRAY[LOCALTIME]",
      "COALESCE(LOCALTIMESTAMP, LOCALTIMESTAMP)", "COALESCE(LOCALTIME, LOCALTIME)", "COALESCE(NULL, localtimestamp)",
      "COALESCE(NULL, localtime)", "ARRAY[COALESCE(NULL, LOCALTIMESTAMP)]", "ARRAY[COALESCE(NULL, LOCALTIME)]"
  })
  void evaluatesNestedLocalDatetime(String expression) throws Exception {
    Path directory = Path.of(LocalDatetimeExpressionTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    for (String source : List.of("mtcars LIMIT 1", "(SELECT 42 AS \"LOCALTIME\", 42 AS \"LOCALTIMESTAMP\") t",
        "mtcars GROUP BY cyl LIMIT 1")) {
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
          var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT " + expression + " FROM " + source)) {
        assertTrue(result.next());
        Object value = result.getObject(1);
        if (expression.startsWith("ARRAY[")) {
          Object[] values = assertInstanceOf(Object[].class, value);
          value = values[0];
        }
        if (expression.toUpperCase(Locale.ROOT).contains("LOCALTIMESTAMP")) {
          assertTrue(value instanceof Timestamp || value instanceof LocalDateTime);
        } else {
          assertTrue(value instanceof Time || value instanceof LocalTime);
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Evaluates datetime keywords in predicate operands and CASE conditions.
   *
   * @param sql
   *          query counting rows that satisfy the datetime predicate
   * @param expected
   *          expected row count
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SELECT COUNT(*) FROM mtcars WHERE LOCALTIMESTAMP IS NOT NULL|32",
      "SELECT COUNT(*) FROM mtcars WHERE LOCALTIME IS NOT NULL|32",
      "SELECT COUNT(*) FROM mtcars WHERE NOT (LOCALTIMESTAMP IS NULL)|32",
      "SELECT COUNT(*) FROM mtcars WHERE NOT (LOCALTIMESTAMP IS NULL) AND mpg > 20|14",
      "SELECT COUNT(*) FROM mtcars WHERE LOCALTIMESTAMP > TIMESTAMP '2000-01-01 00:00:00'|32",
      "SELECT SUM(CASE WHEN LOCALTIMESTAMP IS NULL THEN 0 ELSE 1 END) FROM mtcars|32",
      "SELECT SUM(CASE WHEN LOCALTIME IS NULL THEN 0 ELSE 1 END) FROM mtcars|32",
      "SELECT COUNT(*) FROM mtcars WHERE LOCALTIME BETWEEN TIME '00:00:00' AND TIME '23:59:59.999999999'|32",
      "SELECT COUNT(*) FROM mtcars HAVING LOCALTIMESTAMP IS NOT NULL|32",
      "SELECT COUNT(*) FROM mtcars HAVING LOCALTIME IS NOT NULL|32",
      "SELECT COUNT(*) FROM mtcars HAVING EXTRACT(HOUR FROM LOCALTIME) IS NOT NULL|32",
      "SELECT COUNT(*) FROM mtcars HAVING TRIM(CAST(LOCALTIME AS VARCHAR(64))) IS NOT NULL|32",
      "SELECT COUNT(*) FROM mtcars HAVING "
          + "(CAST(LOCALTIME AS VARCHAR(64)) COLLATE latin1_general_ci) IS NOT NULL|32",
      "SELECT COUNT(*) FROM mtcars HAVING CONVERT(CAST(LOCALTIME AS VARCHAR(64)) USING UTF8) IS NOT NULL|32",
      "SELECT COUNT(*) FROM mtcars HAVING POSITION(':' IN CAST(LOCALTIME AS VARCHAR(64))) > 0|32",
      "SELECT SUM(CASE WHEN LOCALTIME IS NULL THEN 0 ELSE 1 END) FROM (SELECT 42 AS \"LOCALTIME\") t|1",
      "SELECT COUNT(*) FROM (SELECT 42 AS \"LOCALTIMESTAMP\") t "
          + "WHERE LOCALTIMESTAMP > TIMESTAMP '2000-01-01 00:00:00'|1",
      "SELECT COUNT(*) FROM mtcars t JOIN (VALUES (1)) v(x) ON LOCALTIME IS NOT NULL|32"
  })
  void evaluatesPredicates(String sql, int expected) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown=" + pushdown
          + "&projectionPushdown=" + pushdown;
      try (var connection = DriverManager.getConnection(url);
          var statement = connection.createStatement();
          var result = statement.executeQuery(sql)) {
        assertTrue(result.next(), sql);
        assertEquals(expected, result.getInt(1), sql);
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Normalizes standalone and lateral UNNEST arguments before column discovery.
   *
   * @param sql
   *          query expanding a local datetime array
   * @param expected
   *          expected number of rows
   * @throws Exception
   *           if the query fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SELECT x FROM UNNEST(ARRAY[LOCALTIME]) u(x)|1", "SELECT x FROM UNNEST(ARRAY[LOCALTIMESTAMP]) u(x)|1",
      "SELECT u.x FROM mtcars m CROSS JOIN LATERAL UNNEST(ARRAY[LOCALTIMESTAMP]) u(x)|32",
      "SELECT u.x FROM mtcars m CROSS JOIN LATERAL UNNEST(ARRAY[LOCALTIME]) u(x)|32",
      "SELECT u.x FROM mtcars m CROSS JOIN UNNEST(ARRAY[LOCALTIME]) u(x)|32"
  })
  void evaluatesUnnestArguments(String sql, int expected) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown=" + pushdown
          + "&projectionPushdown=" + pushdown;
      try (var connection = DriverManager.getConnection(url);
          var statement = connection.createStatement();
          var result = statement.executeQuery(sql)) {
        int count = 0;
        while (result.next()) {
          Object value = result.getObject(1);
          if (sql.contains("LOCALTIMESTAMP")) {
            assertTrue(value instanceof Timestamp || value instanceof LocalDateTime, sql);
          } else {
            assertTrue(value instanceof Time || value instanceof LocalTime, sql);
          }
          count++;
        }
        assertEquals(expected, count, sql);
      }
    }
  }

  /**
   * Rejects numeric arithmetic on a bare datetime keyword even when the input
   * exposes a same-named numeric column.
   *
   * @param keyword
   *          local datetime value function
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if JDBC setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LOCALTIME", "localtime", "LOCALTIMESTAMP", "localtimestamp"
  })
  void rejectsNumericDatetimeArithmetic(String keyword, @TempDir Path directory) throws Exception {
    String sql = "SELECT " + keyword + " + 0 FROM (SELECT 42 AS \"LOCALTIME\", 42 AS \"LOCALTIMESTAMP\") t";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      SQLException failure = assertThrows(SQLException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          if (result.next()) {
            result.getObject(1);
          }
        }
      });
      assertEquals("42000", failure.getSQLState());
    }
  }

  /**
   * Rejects reserved keywords used as unquoted qualified identifiers, whether the
   * field exists or is absent, instead of evaluating them as value functions.
   *
   * @param expression
   *          qualified column reference in a projection or predicate
   * @throws Exception
   *           if JDBC setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT COALESCE(t.LOCALTIME, 0) FROM mtcars t LIMIT 1",
      "SELECT COALESCE(t.LOCALTIMESTAMP, 0) FROM mtcars t LIMIT 1", "SELECT ARRAY[t.LOCALTIME] FROM mtcars t LIMIT 1",
      "SELECT t.LOCALTIMESTAMP FROM mtcars t LIMIT 1", "SELECT 1 FROM mtcars t WHERE t.LOCALTIME IS NOT NULL LIMIT 1",
      "SELECT 1 FROM mtcars t WHERE t.LOCALTIMESTAMP IS NOT NULL LIMIT 1",
      "SELECT COALESCE(t.LOCALTIME, 0) FROM (SELECT 42 AS \"LOCALTIME\") t",
      "SELECT COALESCE(t.LOCALTIMESTAMP, 0) FROM (SELECT 42 AS \"LOCALTIMESTAMP\") t",
      "SELECT COALESCE(t.LOCALTIME, 0) FROM mtcars t JOIN (VALUES (1)) v(x) ON TRUE LIMIT 1"
  })
  void rejectsUnquotedQualifiedKeywords(String expression) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown=" + pushdown
          + "&projectionPushdown=" + pushdown;
      try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
        SQLException failure = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery(expression)) {
            if (result.next()) {
              result.getObject(1);
            }
          }
        }, expression);
        assertEquals("42000", failure.getSQLState(), expression);
        assertTrue(failure.getMessage().contains("LOCALTIME"), failure::getMessage);
      }
    }
  }

  /**
   * Keeps delimited and qualified keyword-shaped names bound to their columns.
   *
   * @param expression
   *          column reference nested in a scalar function
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if the query fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "\"LOCALTIME\"", "\"LOCALTIMESTAMP\"", "t.\"LOCALTIME\"", "t.\"LOCALTIMESTAMP\""
  })
  void preservesColumnReferences(String expression, @TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(
            "SELECT COALESCE(" + expression + ", 0) FROM (SELECT 42 AS \"LOCALTIME\", 42 AS \"LOCALTIMESTAMP\") t")) {
      assertTrue(result.next());
      assertEquals(42, result.getInt(1));
      assertFalse(result.next());
    }
  }
}
