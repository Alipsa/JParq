package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies ambiguity diagnostics and valid ordering over duplicate names. */
class AmbiguousColumnTest {

  /**
   * Rejects ambiguous references independently of row counts and pushdown.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if JDBC setup fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rejectsAmbiguousColumns(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String sql : List.of("SELECT id FROM acme.employees a JOIN acme.employees b ON a.id = b.id WHERE a.id = 1",
          "SELECT x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE", "SELECT x FROM (SELECT 1 AS x, 2 AS x) t",
          "WITH t AS (SELECT 1 AS x, 2 AS x) SELECT x FROM t", "SELECT * FROM (SELECT 1 AS x, 2 AS x) t ORDER BY x",
          "SELECT * FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY x",
          "SELECT a.x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE WHERE x = 1",
          "SELECT * FROM (SELECT 1 AS x WHERE FALSE) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY x",
          "SELECT * FROM (SELECT 1 AS x, 2 AS x) t ORDER BY t.x", "SELECT 1 AS x, 2 AS x ORDER BY x",
          "SELECT a.x, b.x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY x",
          "SELECT * FROM (SELECT 1 AS x, 2 AS x) t ORDER BY x + 1",
          "SELECT * FROM (SELECT 1 AS x, 2 AS x) t WHERE x = 1",
          "SELECT * FROM (SELECT 1 AS x, 2 AS x) t WHERE FALSE ORDER BY x",
          "WITH t AS (SELECT 1 AS x, 2 AS x) SELECT * FROM t ORDER BY x",
          "SELECT * FROM (SELECT id AS x, id AS x FROM hf.optimized WHERE FALSE) t ORDER BY x")) {
        SQLException failure = assertThrows(SQLException.class, () -> {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals("42702", failure.getSQLState(), sql + ": " + failure);
        assertTrue(failure.getMessage().contains("Ambiguous column reference"), failure::getMessage);
        assertTrue(failure.getMessage().contains(sql.contains("SELECT id FROM") ? "id" : "x"), failure::getMessage);
      }
    }
  }

  /**
   * Allows qualified, positional, and unrelated alias ordering.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void permitsUnambiguousOrdering(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT * FROM (SELECT 1 AS x, 2 AS x) t",
          "SELECT * FROM (SELECT 1 AS x, 2 AS x) t ORDER BY 1",
          "SELECT * FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY a.x",
          "SELECT a.x, b.x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY a.x",
          "SELECT a.x, b.x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY 1",
          "SELECT a.x AS y, b.x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY y",
          "SELECT a.*, b.x AS y FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY y",
          "SELECT a.x AS x, b.x AS x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY a.x",
          "SELECT a.x AS x, b.x AS y FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE ORDER BY y")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertEquals(2, result.getInt(2), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Sorts by the chosen join input without binding unrelated duplicate labels.
   *
   * @param order
   *          the ordering reference
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "a.x", "1", "chosen"
  })
  void sortsChosenJoinColumn(String order) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
    String sql = "SELECT a.x AS chosen, b.x AS x FROM (VALUES (3), (1), (2)) a(x) "
        + "CROSS JOIN (VALUES (7)) b(x) ORDER BY " + order;
    try (var connection = DriverManager.getConnection(url);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      for (int expected = 1; expected <= 3; expected++) {
        assertTrue(result.next(), sql);
        assertEquals(expected, result.getInt(1), sql);
        assertEquals(7, result.getInt(2), sql);
      }
      assertFalse(result.next(), sql);
    }
  }

}
