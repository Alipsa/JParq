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

/** Verifies missing-column validation at derived-table and CTE boundaries. */
class UnknownMaterializedColumnTest {

  /**
   * Rejects unknown names independently of row availability and pushdown
   * settings.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rejectsUnknownColumns(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String filter : List.of("id <= 1", "id < 0")) {
        for (String source : List.of("(SELECT id FROM optimized WHERE " + filter + ") x",
            "(SELECT id FROM optimized WHERE " + filter + ") x(a)", "c")) {
          String prefix = source.equals("c") ? "WITH c AS (SELECT id FROM optimized WHERE " + filter + ") " : "";
          String qualifier = source.equals("c") ? "c" : "x";
          for (String query : List.of("SELECT zzz FROM " + source, "SELECT " + qualifier + ".zzz FROM " + source,
              "SELECT zzz + 1 AS computed FROM " + source, "SELECT * FROM " + source + " WHERE zzz = 1",
              "SELECT * FROM " + source + " ORDER BY zzz", "SELECT COUNT(zzz) FROM " + source,
              "SELECT COUNT(*) AS c FROM " + source + " HAVING zzz = 1",
              "SELECT COUNT(*) FROM " + source + " GROUP BY zzz")) {
            String sql = prefix + query;
            SQLException exception = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
            assertTrue(exception.getMessage().contains("zzz"), sql + ": " + exception);
            assertTrue(exception.getMessage().contains("Missing columns"), sql + ": " + exception);
            assertEquals("42000", exception.getSQLState(), sql);
          }
        }
        for (String sql : List.of("SELECT x.id FROM (SELECT id FROM optimized WHERE " + filter + ") x(a)",
            "WITH c(a) AS (SELECT id FROM optimized WHERE " + filter + ") SELECT c.id FROM c",
            "WITH c AS (SELECT id FROM optimized WHERE " + filter + ") SELECT x.id FROM (c) x(a)")) {
          SQLException exception = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
          assertTrue(exception.getMessage().contains("Missing columns"), sql + ": " + exception);
          assertTrue(exception.getMessage().contains("id"), sql + ": " + exception);
        }
      }
      for (String filter : List.of("id = 1", "id < 0")) {
        for (String sql : List.of(
            "SELECT zzz FROM (SELECT id FROM optimized WHERE " + filter + ") x JOIN (SELECT 1 AS a) y ON TRUE",
            "SELECT x.zzz FROM (SELECT id FROM optimized WHERE " + filter + ") x JOIN (SELECT 1 AS a) y ON TRUE",
            "WITH c AS (SELECT id FROM optimized WHERE " + filter
                + ") SELECT c.zzz FROM c JOIN (SELECT 1 AS a) y ON TRUE",
            "SELECT x.id FROM (SELECT id FROM optimized WHERE " + filter
                + ") x JOIN (VALUES (1)) y(a) ON x.id = y.a AND zzz = 1",
            "SELECT x.id FROM (SELECT id FROM optimized WHERE " + filter
                + ") x JOIN (VALUES (1)) y(a) ON zzz = 1 JOIN (VALUES (1)) z(zzz) ON TRUE",
            "SELECT x.id FROM (SELECT id FROM optimized WHERE " + filter + ") x(a) JOIN (SELECT 1 AS b) y ON TRUE")) {
          SQLException exception = assertThrows(SQLException.class, () -> {
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, sql);
          assertTrue(exception.getMessage().contains("Missing columns"), sql + ": " + exception);
          assertEquals("42000", exception.getSQLState(), sql);
          if (sql.startsWith("SELECT x.id FROM") && sql.contains("x(a)")) {
            assertTrue(exception.getMessage().contains("Missing columns for table 'x': id"), sql + ": " + exception);
          }
        }
      }
      for (String sql : List.of("SELECT a AS result FROM (SELECT id FROM optimized WHERE id = 1) x(a) ORDER BY result",
          "WITH c(a) AS (SELECT id FROM optimized WHERE id = 1) SELECT c.a AS result FROM c ORDER BY result",
          "WITH c AS (SELECT id FROM optimized WHERE id = 1) SELECT x.a FROM (c) x(a)",
          "WITH c AS (SELECT id FROM optimized WHERE id = 1) "
              + "SELECT x.a FROM (c) x(a) JOIN (SELECT 1 AS b) y ON x.a = y.b",
          "SELECT COUNT(*) AS c FROM (SELECT 1 AS a) x JOIN (SELECT 1 AS b) y ON TRUE HAVING c = 1 ORDER BY c",
          "SELECT (SELECT id FROM optimized WHERE id = 1) FROM (SELECT 1 AS a) x JOIN (SELECT 1 AS b) y ON TRUE",
          "SELECT x.* FROM (SELECT id FROM optimized WHERE id = 1) x",
          "SELECT COUNT(*) FROM (SELECT id FROM optimized WHERE id < 0) x", "SELECT a FROM (VALUES (1)) x(a)",
          "SELECT x.id FROM (SELECT id FROM optimized WHERE id = 1) x WHERE EXISTS (SELECT 1 WHERE x.id = 1)")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(sql.contains("id < 0") ? 0 : 1, result.getInt(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }
}
