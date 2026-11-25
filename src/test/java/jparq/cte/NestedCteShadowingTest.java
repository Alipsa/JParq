package jparq.cte;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for nested non-recursive CTEs that shadow an enclosing CTE
 * with the same name. Without the RECURSIVE keyword a CTE's own name is not in
 * scope inside its body, so the reference resolves to the outer declaration
 * instead of being treated as recursion.
 */
class NestedCteShadowingTest {

  @TempDir
  Path directory;

  /**
   * Runs a single-column query and collects every row of the first column.
   *
   * @param sql
   *          the query to execute
   * @return the values of the first column in row order
   * @throws SQLException
   *           if the query cannot be prepared or executed
   */
  private List<Object> queryFirstColumn(String sql) throws SQLException {
    List<Object> values = new ArrayList<>();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement(sql);
        var rs = statement.executeQuery()) {
      while (rs.next()) {
        values.add(rs.getInt(1));
      }
    }
    return values;
  }

  /**
   * Verifies that a nested CTE referencing an outer CTE of the same name reads
   * the outer declaration instead of recursing on itself.
   *
   * @throws SQLException
   *           if the query cannot be prepared or executed
   */
  @Test
  void nestedCteReadsOuterCteWithSameName() throws SQLException {
    String sql = """
        WITH c AS (SELECT 1 AS x)
        SELECT x FROM (WITH c AS (SELECT x + 1 AS x FROM c) SELECT x FROM c) t
        """;
    assertEquals(List.of(2), queryFirstColumn(sql));
  }

  /**
   * Verifies the same shadowing resolution when the projection uses a wildcard,
   * which exercises metadata resolution of the nested CTE.
   *
   * @throws SQLException
   *           if the query cannot be prepared or executed
   */
  @Test
  void nestedCteShadowingSupportsWildcardProjection() throws SQLException {
    String sql = """
        WITH c AS (SELECT 1 AS x)
        SELECT * FROM (WITH c AS (SELECT x + 1 AS x FROM c) SELECT * FROM c) t
        """;
    assertEquals(List.of(2), queryFirstColumn(sql));
  }

  /**
   * Verifies that shadowing chains across several nesting levels, each level
   * reading the declaration of the immediately enclosing WITH clause.
   *
   * @throws SQLException
   *           if the query cannot be prepared or executed
   */
  @Test
  void shadowingResolvesAgainstImmediatelyEnclosingDeclaration() throws SQLException {
    String sql = """
        WITH c AS (SELECT 1 AS x)
        SELECT x FROM (
          WITH c AS (SELECT x + 1 AS x FROM c)
          SELECT x FROM (
            WITH c AS (SELECT x + 10 AS x FROM c)
            SELECT x FROM c
          ) t2
        ) t
        """;
    assertEquals(List.of(12), queryFirstColumn(sql));
  }

  /**
   * Verifies that shadowing also applies when the inner declaration quotes the
   * lowercase name, which is equivalent to the unquoted outer name.
   *
   * @throws SQLException
   *           if the query cannot be prepared or executed
   */
  @Test
  void quotedInnerNameShadowsUnquotedOuterName() throws SQLException {
    String sql = """
        WITH c AS (SELECT 1 AS x)
        SELECT x FROM (WITH "c" AS (SELECT x + 1 AS x FROM c) SELECT x FROM c) t
        """;
    assertEquals(List.of(2), queryFirstColumn(sql));
  }

  /**
   * Verifies that a non-recursive CTE referencing its own name without an
   * enclosing declaration of that name is rejected as an unknown table.
   *
   * @throws SQLException
   *           if the test connection cannot be opened
   */
  @Test
  void nonRecursiveSelfReferenceWithoutOuterDeclarationFails() throws SQLException {
    String sql = "WITH c AS (SELECT x + 1 AS x FROM c) SELECT x FROM c";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertThrows(SQLException.class, () -> connection.prepareStatement(sql).executeQuery());
    }
  }

  /**
   * Verifies that a self-referencing CTE declared after the first item of a WITH
   * RECURSIVE clause still iterates recursively; the keyword applies to the whole
   * clause even though JSqlParser reports it on the first item only.
   *
   * @throws SQLException
   *           if the query cannot be prepared or executed
   */
  @Test
  void recursiveKeywordAppliesToLaterItemsInTheClause() throws SQLException {
    String sql = """
        WITH RECURSIVE anchor(n) AS (SELECT 1),
        r(n) AS (SELECT n FROM anchor UNION ALL SELECT n + 1 FROM r WHERE n < 3)
        SELECT n FROM r ORDER BY n
        """;
    assertEquals(List.of(1, 2, 3), queryFirstColumn(sql));
  }
}
