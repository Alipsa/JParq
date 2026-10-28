package jparq.quoted;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies that delimited lowercase names resolve like their unquoted
 * equivalents.
 */
class LowercaseDelimitedIdentifierTest {

  /**
   * Checks physical and materialized columns and CTE names under every pushdown
   * configuration.
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
  void resolvesLowercaseNames(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String sql : List.of("SELECT \"id\" FROM optimized WHERE id = 1",
          "SELECT \"id\" + 0 AS v FROM optimized WHERE id = 1", "SELECT id FROM optimized WHERE \"id\" = 1",
          "SELECT \"x\" FROM (SELECT 1 AS x) t", "SELECT \"id\" FROM optimized WHERE \"id\" = 1",
          "SELECT \"o\".\"id\" + 0 FROM optimized o WHERE \"o\".\"id\" = 1",
          "SELECT DISTINCT \"id\" FROM optimized WHERE id = 1 ORDER BY \"id\"",
          "SELECT \"x\" + 0 FROM (SELECT 1 AS x) t WHERE \"x\" = 1",
          "SELECT \"x\" FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) t WHERE \"x\" = 1",
          "WITH c AS (SELECT 1 AS x) SELECT \"x\" + 0 FROM c WHERE \"x\" = 1",
          "WITH \"c\" AS (SELECT 1 AS x) SELECT t.x FROM (SELECT * FROM c) t",
          "WITH RECURSIVE \"c\"(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1) SELECT x FROM c",
          "SELECT \"t\".\"x\" FROM (VALUES (1)) t(x) WHERE \"x\" = 1",
          "SELECT \"t\".\"x\" FROM (SELECT 1 AS x) t JOIN (SELECT 2 AS y) u ON \"x\" + 1 = \"y\"",
          "WITH c AS (SELECT 1 AS x) SELECT \"x\" FROM \"c\"",
          "WITH c AS (SELECT 2 AS x) SELECT x FROM (WITH \"c\" AS (SELECT 1 AS x) SELECT x FROM c) t",
          "WITH \"c\" AS (SELECT 2 AS x) SELECT x FROM (WITH c AS (SELECT 1 AS x) SELECT x FROM \"c\") t",
          "WITH c AS (SELECT 2 AS x), \"C\" AS (SELECT 1 AS x) SELECT x FROM \"C\"",
          "WITH \"c\" AS (SELECT id FROM optimized WHERE id = 1) SELECT c.id FROM c",
          "WITH \"c\"(x) AS (VALUES (1)) SELECT \"x\" FROM c",
          "SELECT o.id FROM optimized o WHERE o.id = 1 AND EXISTS "
              + "(WITH optimized AS (SELECT 2 AS z) SELECT 1 FROM optimized WHERE z = 2 AND id = 1)",
          "SELECT o.id FROM optimized o WHERE o.id = 1 AND EXISTS "
              + "(WITH \"optimized\" AS (SELECT 2 AS z) SELECT 1 FROM optimized WHERE z = 2 AND id = 1)",
          "SELECT o.id FROM optimized o WHERE o.id = 1 AND EXISTS "
              + "(WITH optimized AS (SELECT 2 AS z) SELECT 1 FROM \"optimized\" WHERE z = 2 AND id = 1)")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertFalse(result.wasNull(), sql);
          assertFalse(result.next(), sql);
        }
      }
      String expectedText;
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT text || text FROM optimized WHERE id = 1")) {
        assertTrue(result.next());
        expectedText = result.getString(1);
        assertFalse(result.wasNull());
      }
      for (String sql : List.of("SELECT \"text\" || \"text\" FROM optimized WHERE id = 1",
          "SELECT * FROM (SELECT \"text\" || \"text\" FROM optimized WHERE id = 1) t",
          "WITH t AS (SELECT \"text\" || \"text\" FROM optimized WHERE id = 1) SELECT * FROM t")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(expectedText, result.getString(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      for (var query : Map.of(
          "SELECT \"o\".\"id\" + p.id FROM optimized o JOIN "
              + "(SELECT id FROM optimized WHERE id = 1) p ON o.id = p.id WHERE o.id = 1",
          2,
          "SELECT \"o\".\"id\" + \"p\".\"id\" FROM optimized o JOIN "
              + "(SELECT id FROM optimized WHERE id = 1) p ON o.id = p.id WHERE o.id = 1",
          2, "SELECT \"u\".\"x\" + 0 FROM (SELECT 1 AS x) t JOIN (SELECT 7 AS x) u ON 1 = 1", 7).entrySet()) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query.getKey())) {
          assertTrue(result.next(), query.getKey());
          assertEquals(query.getValue().intValue(), result.getInt(1), query.getKey());
          assertFalse(result.wasNull(), query.getKey());
          assertFalse(result.next(), query.getKey());
        }
      }
      for (String sql : List.of("SELECT \"id\", COUNT(*) FROM optimized WHERE id <= 2 GROUP BY \"id\" ORDER BY 1",
          "SELECT \"id\", COUNT(*) FROM optimized WHERE id <= 2 GROUP BY id ORDER BY 1",
          "SELECT id, COUNT(*) FROM optimized WHERE id <= 2 GROUP BY \"id\" ORDER BY 1",
          "SELECT \"x\", COUNT(*) FROM (SELECT id AS x FROM optimized WHERE id <= 2) t GROUP BY \"x\" ORDER BY 1")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          for (int id = 1; id <= 2; id++) {
            assertTrue(result.next(), sql);
            assertEquals(id, result.getInt(1), sql);
            assertFalse(result.wasNull(), sql);
            assertEquals(1, result.getInt(2), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
      for (String sql : List.of("WITH c AS (SELECT 1 AS x), \"c\" AS (SELECT 2 AS x) SELECT x FROM c",
          "WITH \"c\" AS (SELECT 1 AS x), c AS (SELECT 2 AS x) SELECT x FROM \"c\"",
          "WITH c AS (SELECT 1 AS x), C AS (SELECT 2 AS x) SELECT x FROM c")) {
        SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("Duplicate CTE name"), failure.getMessage());
      }
    }
  }
}
