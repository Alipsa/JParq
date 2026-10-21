package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies SQL name resolution independently of generated Avro identifiers. */
class MaterializedIdentifierLookupTest {

  /**
   * Checks predicate binding, ambiguity, hidden names, and delimiter decoding.
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
  void resolvesOnlyVisibleUnambiguousNames(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String sql : List.of(
          "SELECT * FROM (SELECT 1 AS \"a b\") a JOIN (SELECT 2 AS \"c d\") b ON TRUE WHERE \"a b\" = 1",
          "SELECT * FROM (SELECT 1 AS \"a b\") a JOIN (SELECT 1 AS \"c d\") b ON \"a b\" = \"c d\"",
          "SELECT * FROM (SELECT 1 AS \"a b\") a JOIN (SELECT 2 AS _column) b ON TRUE WHERE _column = 2",
          "SELECT * FROM (SELECT 1 AS \"a b\") \"My T\" JOIN (SELECT 2 AS \"a b\") \"_column_0\" ON TRUE",
          "SELECT * FROM (SELECT 1 AS \"a b\") \"My T\" JOIN (SELECT 2 AS \"a b\") _column ON TRUE")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertEquals(sql.contains("= \"c d\"") ? 1 : 2, result.getInt(2), sql);
          assertFalse(result.next(), sql);
        }
      }
      for (String sql : List.of("SELECT x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS x) b ON TRUE",
          "SELECT id FROM (SELECT id FROM optimized WHERE id = 1) a "
              + "JOIN (SELECT id FROM optimized WHERE id = 1) b ON a.id = b.id",
          "SELECT x FROM (SELECT 1 AS x, 2 AS x) t", "SELECT * FROM (SELECT 1 AS x, 2 AS x) t WHERE x = 2",
          "SELECT t.x FROM (SELECT 1 AS x, 2 AS x) t")) {
        SQLException exception = assertThrows(SQLException.class, () -> {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        Throwable cause = exception;
        while (cause.getCause() != null) {
          cause = cause.getCause();
        }
        assertTrue(cause.getMessage().contains("Ambiguous column reference"), sql + ": " + cause);
      }
      for (String sql : List.of("SELECT _column FROM (SELECT 1 AS \"a b\") t",
          "SELECT \"_column\" FROM (SELECT 1 AS \"a b\") t",
          "SELECT _column_1 FROM (SELECT 1 AS \"a b\", 2 AS \"c d\") t", "SELECT x_1 FROM (SELECT 1 AS x, 2 AS x) t",
          "SELECT Mixed FROM (SELECT 1 AS \"Mixed\") t", "SELECT t._column FROM (SELECT 1 AS \"a b\") t")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          // Unknown-column rejection is tracked separately in #209.
          assertNull(result.getObject(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      for (String sql : List.of("SELECT 1 AS \"a\"\"b\"",
          "SELECT \"a\"\"b\" FROM (SELECT 1 AS \"a\"\"b\") t WHERE \"a\"\"b\" = 1",
          "WITH t AS (SELECT 1 AS \"a\"\"b\") SELECT * FROM t")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals("a\"b", result.getMetaData().getColumnLabel(1), sql);
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt("a\"b"), sql);
          assertFalse(result.next(), sql);
        }
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT * FROM (SELECT 1 AS x, 2 AS x) t")) {
        assertEquals("x", result.getMetaData().getColumnLabel(1));
        assertEquals("x", result.getMetaData().getColumnLabel(2));
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
        assertEquals(2, result.getInt(2));
        assertFalse(result.next());
      }
    }
  }
}
