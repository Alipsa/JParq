package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.format.DateTimeParseException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies temporal and predicate classifications across JDBC query boundaries.
 */
class TemporalPredicateFailureTest {
  /**
   * Preserves temporal and predicate states through preparation and evaluation.
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
  void classifiedFailuresSurviveQueryBoundaries(boolean predicatePushdown, boolean projectionPushdown)
      throws Exception {
    String url = "jdbc:jparq:" + Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent()
        + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String expression : List.of("DATE '2024-02-30'", "CAST('25:00:00' AS TIME)",
          "TIMESTAMP '2024-01-01 12:60:00'", "DATE '2024-13-01'", "CAST('2024-02-30T10:00:00+01:00' AS DATE)",
          "CAST('2024-01-01T25:00:00+01:00' AS TIME)", "CAST('2024-02-30T10:00:00+01:00' AS TIMESTAMP)",
          "CAST('2024-02-30' AS TIMESTAMP)", "CAST('not a date' AS DATE)", "CAST('bad' AS TIME)",
          "CAST('bad' AS TIMESTAMP)", "CASE WHEN cyl THEN 1 ELSE 0 END", "NOT model", "CASE WHEN 1 THEN 1 ELSE 0 END",
          "NOT (cyl + 1)")) {
        boolean predicate = expression.startsWith("CASE") || expression.startsWith("NOT");
        String state = predicate
            ? "42000"
            : expression.contains("bad") || expression.contains("not a date") ? "22007" : "22008";
        String body = "SELECT " + expression + " AS x FROM mtcars LIMIT 1";
        String condition = predicate ? expression.startsWith("NOT") ? expression : "cyl" : expression + " IS NOT NULL";
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c",
            "SELECT (" + body + ") AS x", "SELECT cyl FROM mtcars WHERE " + condition + " LIMIT 1",
            "SELECT a.cyl FROM mtcars a JOIN mtcars b ON a.cyl = b.cyl WHERE "
                + condition.replace("model", "b.model").replace("cyl", "b.cyl") + " LIMIT 1")) {
          SQLException failure = assertThrows(SQLException.class, () -> {
            try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
              assertTrue(result.next(), sql);
              result.getObject(1);
            }
          }, sql);
          assertEquals(state, failure.getSQLState(), sql);
          if (predicate) {
            assertInstanceOf(SQLSyntaxErrorException.class, failure, sql);
          } else {
            assertInstanceOf(SQLDataException.class, failure, sql);
          }
          assertTrue(failure.getMessage().contains(predicate ? "predicate" : "value"), failure::getMessage);
          if (!predicate) {
            Throwable root = failure;
            while (root.getCause() != null) {
              root = root.getCause();
            }
            assertTrue(hasParseCause(failure), sql + ": " + root);
          }
        }
      }
    }
  }

  /**
   * Keeps expression errors classified regardless of the JDBC getter used.
   *
   * @param getter
   *          the value accessor
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "object", "string", "date", "time", "timestamp"
  })
  void gettersRetainTemporalState(String getter) throws Exception {
    String url = "jdbc:jparq:" + Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection(url);
        var statement = connection.prepareStatement("SELECT DATE '2024-02-30' AS x FROM mtcars LIMIT 1");
        var result = statement.executeQuery()) {
      assertTrue(result.next());
      var failure = assertThrows(SQLDataException.class, () -> {
        switch (getter) {
          case "object" -> result.getObject("x");
          case "string" -> result.getString("x");
          case "date" -> result.getDate("x");
          case "time" -> result.getTime("x");
          case "timestamp" -> result.getTimestamp("x");
          default -> throw new AssertionError(getter);
        }
      });
      assertEquals("22008", failure.getSQLState());
      assertTrue(failure.getMessage().contains("2024-02-30"), failure::getMessage);
      assertTrue(hasParseCause(failure));
    }
  }

  /**
   * Retains valid alternate temporal formats and SQL UNKNOWN predicate semantics.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void validTemporalAndBooleanValuesRemainSupported() throws Exception {
    String url = "jdbc:jparq:" + Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection(url);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT DATE '2024-2-29', CAST('1:2:3' AS TIME), "
            + "CAST('2024-02-29T01:02:03+01:00' AS TIMESTAMP), CAST(NULL AS DATE), "
            + "NOT NULL, NOT FALSE, CASE WHEN TRUE THEN 1 ELSE 0 END FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals(Date.valueOf("2024-02-29"), result.getDate(1));
      assertEquals(Time.valueOf("01:02:03"), result.getTime(2));
      assertEquals(Timestamp.from(java.time.Instant.parse("2024-02-29T00:02:03Z")), result.getTimestamp(3));
      assertNull(result.getObject(4));
      assertNull(result.getObject(5));
      assertEquals(true, result.getBoolean(6));
      assertEquals(1, result.getInt(7));
    }
  }

  /**
   * Checks that a temporal parser's original failure remains reachable.
   *
   * @param failure
   *          the JDBC failure
   * @return whether a parser failure exists in the cause chain
   */
  private static boolean hasParseCause(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof DateTimeParseException) {
        return true;
      }
    }
    return false;
  }
}
