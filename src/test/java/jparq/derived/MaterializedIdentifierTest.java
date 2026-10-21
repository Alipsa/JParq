package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies SQL labels independently of materialized Avro names. */
class MaterializedIdentifierTest {

  /**
   * Checks quoted names, computed labels, and collision handling at query
   * boundaries.
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
  void preservesLabelsAndValues(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String expression : List.of("id", "id * 1")) {
        String body = "SELECT " + expression + " AS \"Mixed\" FROM optimized WHERE id = 1";
        for (String sql : List.of(body, "SELECT \"Mixed\" FROM (" + body + ") t",
            "SELECT * FROM (" + body + ") \"My T\"", "WITH t AS (" + body + ") SELECT \"Mixed\" FROM t",
            "WITH \"My T\" AS (" + body + ") SELECT * FROM \"My T\"",
            "SELECT \"My T\".\"Mixed\" FROM (" + body + ") \"My T\" JOIN (VALUES (1)) s(n) ON TRUE",
            "SELECT x.\"Mixed\" FROM (VALUES (1)) s(n) JOIN LATERAL (" + body + ") x ON TRUE")) {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertEquals("Mixed", result.getMetaData().getColumnLabel(1), sql);
            assertTrue(result.next(), sql);
            assertEquals("1", result.getString("Mixed"), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
      for (String body : List.of("SELECT 1", "SELECT 1 UNION SELECT 1")) {
        for (String sql : List.of("SELECT * FROM (" + body + ") x", "WITH x AS (" + body + ") SELECT * FROM x")) {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertEquals("1", result.getMetaData().getColumnLabel(1), sql);
            assertTrue(result.next(), sql);
            assertEquals(1, result.getInt("1"), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
      for (String source : List.of("(SELECT 1 AS \"a b\", 2 AS a_b, 3 AS _column) \"My T\"",
          "(SELECT 1, 2, 3) \"My T\"(\"a b\", a_b, _column)")) {
        String sql = "SELECT \"My T\".\"a b\", a_b, _column FROM " + source;
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          for (int i = 1; i <= 3; i++) {
            assertEquals(i, result.getInt(i), sql);
          }
          assertEquals("a b", result.getMetaData().getColumnLabel(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Checks wildcard expansion, empty schemas, expressions, and quoted names
   * containing spaces.
   *
   * @throws Exception
   *           if JDBC execution fails
   */
  @Test
  void preservesUnrestrictedLabelsAcrossBoundaries() throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath();
    try (var connection = DriverManager.getConnection(url)) {
      for (String body : List.of("SELECT 1 AS \"a b\"", "SELECT * FROM (VALUES (1)) \"My V\"(\"a b\")",
          "SELECT COUNT(*) AS \"a b\" FROM optimized WHERE id = 1", "SELECT 1 AS \"a b\" UNION SELECT 1",
          "SELECT * FROM (SELECT 1 AS \"a b\") nested")) {
        for (String source : List.of("(" + body + ") \"My T\"",
            "(" + body + ") \"My T\" JOIN (SELECT 2 AS other) \"Other T\" ON TRUE")) {
          for (String projection : List.of("\"My T\".*", "\"My T\".\"a b\"")) {
            String sql = "SELECT " + projection + " FROM " + source;
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              assertEquals("a b", result.getMetaData().getColumnLabel(1), sql);
              assertTrue(result.next(), sql);
              assertEquals(1, result.getInt("a b"), sql);
              assertFalse(result.next(), sql);
            }
          }
        }
      }
      for (String sql : List.of("SELECT * FROM (SELECT id AS \"a b\" FROM optimized WHERE id < 0) \"My T\"",
          "WITH \"My T\" AS (SELECT id AS \"a b\" FROM optimized WHERE id < 0) SELECT * FROM \"My T\"")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals("a b", result.getMetaData().getColumnLabel(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      String sql = "SELECT * FROM (SELECT id + 1 FROM optimized WHERE id = 1) t";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
        assertEquals("id + 1", result.getMetaData().getColumnLabel(1), sql);
        assertTrue(result.next(), sql);
        assertEquals(2, result.getInt(1), sql);
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Checks temporary schemas used for set ordering, USING, and correlated LATERAL
   * inputs.
   *
   * @throws Exception
   *           if JDBC execution fails
   */
  @Test
  void bindsQuotedNamesInRelationalClauses() throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath();
    try (var connection = DriverManager.getConnection(url)) {
      for (String sql : List.of("SELECT 2 AS \"a b\" UNION ALL SELECT 1 ORDER BY \"a b\" + 1",
          "SELECT * FROM (SELECT 1 AS \"a b\") a JOIN (SELECT 1 AS \"a b\") b USING (\"a b\")",
          "SELECT x.v FROM (SELECT 1 AS \"a b\") a CROSS JOIN LATERAL "
              + "(SELECT a.\"a b\" AS v FROM optimized WHERE id = 1) x",
          "WITH t AS (SELECT 1 AS \"a b\") SELECT \"a b\" FROM t " + "WHERE EXISTS (SELECT 1 WHERE t.\"a b\" = 1)")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals("1", result.getString(1), sql);
          if (sql.contains("UNION")) {
            assertTrue(result.next(), sql);
            assertEquals("2", result.getString(1), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

}
