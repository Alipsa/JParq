package jparq.cte;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Regression tests for SELECT anchors that require no physical input table. */
class SelectAnchorWithoutFromTest {

  @TempDir
  Path directory;

  /**
   * Verifies table-free recursion through JDBC in both UNION modes.
   *
   * @param union
   *          the set operation joining the anchor and recursive member
   * @throws SQLException
   *           if preparation or execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UNION ALL", "UNION"
  })
  void recursiveAnchorProducesSequence(String union) throws SQLException {
    String sql = """
        WITH RECURSIVE r(n) AS (
          SELECT 1
          %s
          SELECT n + 1 FROM r WHERE n < 3
        )
        SELECT n FROM r ORDER BY n
        """.formatted(union);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement(sql)) {
      for (int execution = 0; execution < 2; execution++) {
        try (var result = statement.executeQuery()) {
          List<Integer> values = new ArrayList<>();
          while (result.next()) {
            values.add(result.getInt("n"));
          }
          assertEquals(List.of(1, 2, 3), values);
          assertEquals(1, result.getMetaData().getColumnCount());
        }
      }
    }
  }

  /**
   * Verifies that an anchor rejected by WHERE does not start recursion.
   *
   * @throws SQLException
   *           if preparation or execution fails
   */
  @Test
  void filteredAnchorProducesNoRows() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            WITH RECURSIVE r(n) AS (
              SELECT 1 WHERE FALSE
              UNION ALL
              SELECT n + 1 FROM r WHERE n < 3
            )
            SELECT n FROM r ORDER BY n
            """)) {
      assertEquals(1, result.getMetaData().getColumnCount());
      assertFalse(result.next());
    }
  }

  /**
   * Verifies computed and nullable anchor columns with a bound JDBC parameter.
   *
   * @throws SQLException
   *           if preparation or execution fails
   */
  @Test
  void computedAnchorPreservesAliasesAndNulls() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("""
            WITH RECURSIVE r(n, label) AS (
              SELECT ? + 1, CAST(NULL AS VARCHAR)
              UNION ALL
              SELECT n + 1, label FROM r WHERE n < 3
            )
            SELECT n, label FROM r ORDER BY n
            """)) {
      statement.setInt(1, 1);
      try (var result = statement.executeQuery()) {
        for (int expected : List.of(2, 3)) {
          assertTrue(result.next());
          assertEquals(expected, result.getInt("n"));
          assertNull(result.getObject("label"));
          assertTrue(result.wasNull());
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Verifies the single-row input also supports ordinary table-free SELECTs.
   *
   * @throws SQLException
   *           if preparation or execution fails
   */
  @Test
  void selectWithoutFromEvaluatesOneRow() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 1 + 2 AS n, UPPER('ok') AS label")) {
      assertTrue(result.next());
      assertEquals(3, result.getInt("n"));
      assertEquals("OK", result.getString("label"));
      assertFalse(result.next());
    }
  }

  /**
   * Verifies that aggregation counts the implicit input row exactly once.
   *
   * @throws SQLException
   *           if preparation or execution fails
   */
  @Test
  void aggregateWithoutFromCountsOneRow() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT COUNT(*) AS count")) {
      assertTrue(result.next());
      assertEquals(1, result.getLong("count"));
      assertFalse(result.next());
    }
  }
}
