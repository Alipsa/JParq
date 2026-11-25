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
 * Verifies that unquoted references resolve explicitly quoted lowercase
 * materialized column aliases, mirroring PostgreSQL lowercase folding.
 */
class QuotedLowercaseAliasReferenceTest {

  /**
   * Checks materialized aliases in derived tables, CTEs, VALUES inputs, and
   * grouped queries under every pushdown configuration.
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
  void resolvesUnquotedReferencesToQuotedLowercaseAliases(boolean predicatePushdown, boolean projectionPushdown)
      throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String sql : List.of("SELECT x FROM (SELECT 1 AS \"x\") t",
          "SELECT x + 0 FROM (SELECT 1 AS \"x\") t WHERE x = 1", "SELECT t.x FROM (SELECT 1 AS \"x\") t",
          "SELECT x FROM (SELECT 1 AS \"x\") t ORDER BY x", "SELECT x FROM (VALUES (1)) t(\"x\")",
          "WITH c AS (SELECT 1 AS \"x\") SELECT x FROM c", "WITH c(\"x\") AS (SELECT 1) SELECT x FROM c",
          "SELECT x FROM (WITH c AS (SELECT 1 AS \"x\") SELECT x FROM c) t",
          "SELECT DISTINCT x FROM (SELECT 1 AS \"x\" UNION ALL SELECT 1) t",
          "SELECT x FROM (SELECT 1 AS \"x\") a JOIN (SELECT 1 AS y) b ON a.x = b.y",
          "SELECT a.x FROM (SELECT 1 AS \"x\") a JOIN (SELECT 2 AS y) b ON 1 = 1",
          "SELECT x FROM (SELECT id AS \"x\" FROM optimized WHERE id = 1) t",
          "SELECT x + 0 FROM (SELECT id AS \"x\" FROM optimized WHERE id = 1) t WHERE x = 1",
          "SELECT id FROM optimized WHERE id = (SELECT x FROM (SELECT 1 AS \"x\") t)",
          "SELECT id AS \"k\" FROM optimized WHERE id = 1 ORDER BY k",
          "SELECT id AS \"k\", COUNT(*) FROM optimized WHERE id = 1 GROUP BY id ORDER BY k DESC")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertFalse(result.wasNull(), sql);
          assertFalse(result.next(), sql);
        }
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT x, \"x\" FROM (SELECT 1 AS \"x\") t")) {
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
        assertEquals(1, result.getInt(2));
        assertFalse(result.next());
      }
      for (String sql : List.of("SELECT x FROM (SELECT 1 AS \"x\" UNION ALL SELECT 2 AS \"x\") t ORDER BY x",
          "SELECT 1 AS \"x\" UNION ALL SELECT 2 ORDER BY x",
          "SELECT id AS \"k\" FROM optimized WHERE id <= 2 ORDER BY k")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          for (int expected = 1; expected <= 2; expected++) {
            assertTrue(result.next(), sql);
            assertEquals(expected, result.getInt(1), sql);
            assertFalse(result.wasNull(), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
      for (String sql : List.of("SELECT x, COUNT(*) FROM (SELECT 1 AS \"x\") t GROUP BY x",
          "SELECT x, COUNT(*) FROM (SELECT 1 AS \"x\") t GROUP BY \"x\"",
          "SELECT COUNT(*) AS \"c\" FROM (SELECT 1 AS z) t HAVING c = 1")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertFalse(result.wasNull(), sql);
          assertFalse(result.next(), sql);
        }
      }
      String grouped = "SELECT \"x\", COUNT(*) FROM (SELECT id AS \"x\" FROM optimized WHERE id <= 2) t GROUP BY x "
          + "ORDER BY 1";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(grouped)) {
        for (int expected = 1; expected <= 2; expected++) {
          assertTrue(result.next(), grouped);
          assertEquals(expected, result.getInt(1), grouped);
          assertFalse(result.wasNull(), grouped);
          assertEquals(1, result.getInt(2), grouped);
        }
        assertFalse(result.next(), grouped);
      }
    }
  }

  /**
   * Keeps quoted mixed-case aliases distinct from their lowercase twins in single
   * inputs, joins, aggregates, and ORDER BY.
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
  void keepsMixedCaseTwinsDistinct(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (var query : List.of(Map.entry("SELECT \"X\", \"x\" FROM (SELECT 1 AS \"X\", 2 AS \"x\") t", "1,2"),
          Map.entry("SELECT t.\"X\", t.\"x\" FROM (SELECT 1 AS \"X\", 2 AS \"x\") t", "1,2"),
          Map.entry("SELECT \"x\", \"X\" FROM (SELECT 2 AS \"x\", 1 AS \"X\") t", "2,1"),
          Map.entry("SELECT SUM(\"X\") FROM (SELECT 1 AS \"X\", 2 AS \"x\") t", "1"),
          Map.entry("SELECT a.\"X\" FROM (SELECT 1 AS \"X\", 3 AS \"x\") a JOIN (SELECT 2 AS y) b ON 1 = 1", "1"),
          Map.entry("SELECT a.x FROM (SELECT 1 AS \"X\", 3 AS \"x\") a JOIN (SELECT 2 AS y) b ON 1 = 1", "3"),
          Map.entry("SELECT b.\"x\" FROM (SELECT 1 AS \"X\") a JOIN (SELECT 2 AS \"x\") b ON 1 = 1", "2"),
          Map.entry("SELECT b.x FROM (SELECT 1 AS \"X\") a JOIN (SELECT 2 AS \"x\") b ON 1 = 1", "2"),
          Map.entry("SELECT x FROM (SELECT 1 AS \"X\") a JOIN (SELECT 2 AS \"x\") b ON 1 = 1", "2"),
          Map.entry("SELECT a.\"X\", b.\"x\" FROM (SELECT 1 AS \"X\") a JOIN (SELECT 2 AS \"x\") b ON 1 = 1", "1,2"),
          Map.entry("SELECT b.x, a.\"X\" FROM (SELECT 1 AS \"X\") a JOIN (SELECT 2 AS \"x\") b ON 1 = 1", "2,1"),
          Map.entry("SELECT a.\"X\", b.x FROM (SELECT 1 AS \"X\") a JOIN (SELECT 2 AS \"x\") b ON 1 = 1", "1,2"))) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query.getKey())) {
          assertTrue(result.next(), query.getKey());
          StringBuilder actual = new StringBuilder();
          for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            if (i > 1) {
              actual.append(',');
            }
            actual.append(result.getInt(i));
          }
          assertEquals(query.getValue(), actual.toString(), query.getKey());
          assertFalse(result.next(), query.getKey());
        }
      }
    }
  }

  /**
   * Keeps quoted mixed-case aliases case-sensitive and rejects references that
   * bind both quoting forms.
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
  void rejectsMixedCaseAndAmbiguousReferences(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT \"Ab\" FROM (SELECT 1 AS \"Ab\") t")) {
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
        assertFalse(result.next());
      }
      for (String sql : List.of("SELECT ab FROM (SELECT 1 AS \"Ab\") t", "SELECT \"AB\" FROM (SELECT 1 AS \"Ab\") t")) {
        SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("Missing columns"), failure.getMessage());
      }
      for (String sql : List.of("SELECT x FROM (SELECT 1 AS \"x\", 2 AS x) t",
          "SELECT \"x\" FROM (SELECT 1 AS \"x\", 2 AS x) t",
          "SELECT 1 AS x, 2 AS \"x\" FROM optimized WHERE id = 1 ORDER BY x",
          "SELECT 1 AS \"x\", 2 AS x FROM optimized WHERE id = 1 ORDER BY \"x\"")) {
        SQLException failure = assertThrows(SQLException.class, () -> {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals("42702", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("Ambiguous column reference"), failure.getMessage());
      }
      String sameExpression = "SELECT id AS x, id AS \"x\" FROM optimized WHERE id = 1 ORDER BY x";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sameExpression)) {
        assertTrue(result.next(), sameExpression);
        assertEquals(1, result.getInt(1), sameExpression);
        assertFalse(result.next(), sameExpression);
      }
    }
  }
}
