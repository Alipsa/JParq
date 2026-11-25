package jparq.cte;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Files;
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
   * Verifies that a wildcard projection over a derived table reads the shadowed
   * outer CTE through the nested declaration.
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
   * enclosing declaration of that name is rejected with an error explaining that
   * self-reference requires WITH RECURSIVE. SQLState assertions are pending
   * (#594).
   *
   * @throws SQLException
   *           if the test connection cannot be opened
   */
  @Test
  void nonRecursiveSelfReferenceWithoutOuterDeclarationFails() throws SQLException {
    String sql = "WITH c AS (SELECT x + 1 AS x FROM c) SELECT x FROM c";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql).executeQuery());
      assertTrue(failure.getMessage().contains("WITH RECURSIVE"), failure::getMessage);
    }
  }

  /**
   * Verifies that a self-referencing UNION body in a nested non-recursive CTE
   * reads the outer CTE of the same name in every set component instead of
   * iterating recursively.
   *
   * @throws SQLException
   *           if the query cannot be prepared or executed
   */
  @Test
  void nestedCteWithUnionBodyReadsOuterCteInEveryComponent() throws SQLException {
    String sql = """
        WITH r AS (SELECT 1 AS n)
        SELECT n FROM (WITH r AS (SELECT n FROM r UNION ALL SELECT n + 1 FROM r) SELECT n FROM r) t ORDER BY n
        """;
    assertEquals(List.of(1, 2), queryFirstColumn(sql));
  }

  /**
   * Verifies that a self-referencing UNION body without the RECURSIVE keyword is
   * rejected: before the fix this query iterated recursively even though the
   * standard reserves that behavior for WITH RECURSIVE.
   *
   * @throws SQLException
   *           if the test connection cannot be opened
   */
  @Test
  void unionSelfReferenceWithoutRecursiveKeywordFails() throws SQLException {
    String sql = "WITH r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql).executeQuery());
      assertTrue(failure.getMessage().contains("WITH RECURSIVE"), failure::getMessage);
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

  /**
   * Copies the mtcars fixture into a second schema directory as table {@code c}.
   *
   * @throws Exception
   *           if the fixture cannot be copied
   */
  private void copyMtcarsAsOtherSchemaTableC() throws Exception {
    URL mtcarsUrl = NestedCteShadowingTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");
    Files.createDirectories(directory.resolve("other"));
    Files.copy(Path.of(mtcarsUrl.toURI()), directory.resolve("other").resolve("c.parquet"));
  }

  /**
   * Verifies that a CTE body reading a schema-qualified physical table with the
   * same name is not mistaken for a self-reference: CTE names are unqualified, so
   * the reference targets the physical table.
   *
   * @throws Exception
   *           if the fixture cannot be copied or the query fails
   */
  @Test
  void schemaQualifiedTableWithSameNameIsNotASelfReference() throws Exception {
    copyMtcarsAsOtherSchemaTableC();
    List<Object> expected = queryFirstColumn("SELECT COUNT(*) FROM other.c");
    assertEquals(expected, queryFirstColumn("WITH c AS (SELECT COUNT(*) AS n FROM other.c) SELECT n FROM c"));
  }

  /**
   * Verifies that a WITH RECURSIVE CTE whose body reads a schema-qualified
   * physical table with the same name runs as an ordinary CTE instead of failing
   * as an invalid recursive query.
   *
   * @throws Exception
   *           if the fixture cannot be copied or the query fails
   */
  @Test
  void schemaQualifiedTableWithSameNameUnderWithRecursiveIsNotRecursion() throws Exception {
    copyMtcarsAsOtherSchemaTableC();
    List<Object> expected = queryFirstColumn("SELECT COUNT(*) FROM other.c");
    assertEquals(expected, queryFirstColumn("WITH RECURSIVE c AS (SELECT COUNT(*) AS n FROM other.c) SELECT n FROM c"));
  }

  /**
   * Verifies that a self-reference inside a scalar subquery without the RECURSIVE
   * keyword is rejected with the WITH RECURSIVE explanation.
   *
   * @throws SQLException
   *           if the test connection cannot be opened
   */
  @Test
  void scalarSubquerySelfReferenceWithoutRecursiveKeywordFails() throws SQLException {
    String sql = "WITH c AS (SELECT (SELECT MAX(x) FROM c) AS x) SELECT x FROM c";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql).executeQuery());
      assertTrue(failure.getMessage().contains("WITH RECURSIVE"), failure::getMessage);
    }
  }

  /**
   * Verifies that a self-reference inside an EXISTS subquery without the
   * RECURSIVE keyword is rejected with the WITH RECURSIVE explanation.
   *
   * @throws SQLException
   *           if the test connection cannot be opened
   */
  @Test
  void existsSubquerySelfReferenceWithoutRecursiveKeywordFails() throws SQLException {
    String sql = "WITH c AS (SELECT 1 AS x WHERE EXISTS (SELECT 1 FROM c)) SELECT x FROM c";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql).executeQuery());
      assertTrue(failure.getMessage().contains("WITH RECURSIVE"), failure::getMessage);
    }
  }

  /**
   * Verifies that a nested CTE whose scalar subquery references the outer CTE of
   * the same name resolves the shadowed declaration.
   *
   * @throws SQLException
   *           if the query cannot be prepared or executed
   */
  @Test
  void nestedCteShadowingThroughScalarSubquery() throws SQLException {
    String sql = """
        WITH c AS (SELECT 1 AS x)
        SELECT x FROM (WITH c AS (SELECT (SELECT MAX(x) FROM c) + 1 AS x) SELECT x FROM c) t
        """;
    assertEquals(List.of(2), queryFirstColumn(sql));
  }
}
