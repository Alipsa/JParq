package jparq.quoted;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
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
          "WITH c AS (SELECT 1 AS x), \"c\" AS (SELECT x FROM c) SELECT x FROM \"c\"",
          "WITH \"c\" AS (SELECT 1 AS x), c AS (SELECT x FROM \"c\") SELECT x FROM c",
          "WITH \"c\" AS (SELECT id FROM optimized WHERE id = 1) SELECT c.id FROM c",
          "WITH \"c\"(x) AS (VALUES (1)) SELECT \"x\" FROM c")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertFalse(result.wasNull(), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }
}
