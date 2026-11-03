package jparq.setops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Regression tests for row bounds on complete set-operation results. */
class SetOperationRowLimitTest {

  /**
   * Verifies that trailing clauses restrict the combined result for every
   * operator.
   *
   * @param operator
   *          the set operator
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"
  })
  void trailingBoundsApplyToCombinedResult(String operator) throws SQLException {
    String sql = "(SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) " + operator
        + " SELECT n FROM (VALUES (5), (3), (4), (6)) t(n)";
    List<Integer> combined = switch (operator) {
      case "UNION" -> List.of(1, 2, 3, 4, 5, 6);
      case "UNION ALL" -> List.of(1, 2, 3, 4, 5, 3, 4, 6);
      case "INTERSECT", "INTERSECT ALL" -> List.of(3, 4);
      case "EXCEPT", "EXCEPT ALL" -> List.of(1, 2);
      default -> throw new IllegalArgumentException(operator);
    };
    try (Connection connection = connect()) {
      assertRows(connection, sql + " LIMIT 1", combined.subList(0, 1));
      assertRows(connection, sql + " OFFSET 1 ROWS", combined.subList(1, combined.size()));
      assertRows(connection, sql + " LIMIT 1 OFFSET 1", combined.subList(1, 2));
      assertRows(connection, sql + " FETCH FIRST ROW ONLY", combined.subList(0, 1));
      assertRows(connection, sql + " OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY", combined.subList(1, 2));
      assertRows(connection, sql + " LIMIT 0", List.of());
      assertRows(connection, sql + " FETCH FIRST 0 ROWS ONLY", List.of());
      assertRows(connection, sql + " OFFSET 100 ROWS", List.of());
    }
  }

  /**
   * Verifies the reported constant-query cases and longer set-operation chains.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void constantBranchesUseSetBounds() throws SQLException {
    try (Connection connection = connect()) {
      assertRows(connection, "SELECT 1 AS n UNION ALL SELECT 2 LIMIT 1", List.of(1));
      assertRows(connection, "SELECT 1 AS n UNION ALL SELECT 2 OFFSET 1 ROWS", List.of(2));
      assertRows(connection, "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 OFFSET 1 ROWS", List.of(2, 3));
      assertRows(connection, "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n DESC LIMIT 1", List.of(2));
      assertRows(connection, "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n OFFSET 1 ROWS FETCH FIRST ROW ONLY",
          List.of(2));
      assertRows(connection, "SELECT 1 AS n UNION ALL SELECT 2 INTERSECT SELECT 2 OFFSET 1 ROWS", List.of(2));
    }
  }

  /**
   * Keeps bounds inside parentheses local while applying outer bounds separately.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void parenthesizedBranchesKeepLocalBounds() throws SQLException {
    try (Connection connection = connect()) {
      assertRows(connection, "SELECT 1 AS n UNION ALL (SELECT 2 LIMIT 0)", List.of(1));
      assertRows(connection, "SELECT 1 AS n UNION ALL (SELECT 2 OFFSET 1 ROWS)", List.of(1));
      assertRows(connection, "SELECT 1 AS n UNION ALL (SELECT 2 FETCH FIRST 0 ROWS ONLY)", List.of(1));
      assertRows(connection, "SELECT 1 AS n UNION ALL (SELECT 2 UNION ALL SELECT 3 LIMIT 1)", List.of(1, 2));
      assertRows(connection, "SELECT 1 AS n UNION ALL (SELECT 2 LIMIT 1) OFFSET 1 ROWS FETCH FIRST ROW ONLY",
          List.of(2));
    }
  }

  /**
   * Preserves bounds through CTE and derived-table materialization.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void nestedQueriesKeepSetBounds() throws SQLException {
    try (Connection connection = connect()) {
      assertRows(connection, "SELECT n FROM (SELECT 1 AS n UNION ALL SELECT 2 LIMIT 1) t", List.of(1));
      assertRows(connection, "WITH c AS (SELECT 1 AS n UNION ALL SELECT 2 OFFSET 1 ROWS) SELECT n FROM c", List.of(2));
      assertRows(connection, "SELECT 1 AS n UNION ALL (SELECT 2 UNION ALL SELECT 3 OFFSET 1 ROWS)", List.of(1, 3));
    }
  }

  /**
   * Preserves set bounds when a correlated predicate subquery is serialized and
   * reparsed for execution with each outer row's bindings.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void correlatedPredicateSubqueryKeepsSetBounds() throws SQLException {
    try (Connection connection = connect()) {
      assertRows(connection, """
          SELECT x FROM (VALUES (1), (2)) t(x)
          WHERE x IN (
            SELECT t.x FROM (VALUES (0)) w(q)
            UNION ALL SELECT q FROM (VALUES (2)) z(q) WHERE q = t.x OFFSET 1 ROWS
          )
          """, List.of(2));
    }
  }

  /**
   * Applies fresh JDBC row-count bindings to the complete set on every execution.
   *
   * @param clause
   *          the parameterized row-limiting clauses
   * @throws SQLException
   *           if preparation or execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", "LIMIT ? OFFSET ?"
  })
  void preparedBoundsApplyToCombinedResult(String clause) throws SQLException {
    int offsetParameter = clause.startsWith("LIMIT") ? 5 : 4;
    int limitParameter = clause.startsWith("LIMIT") ? 4 : 5;
    try (Connection connection = connect();
        PreparedStatement statement = connection
            .prepareStatement("SELECT ? AS n UNION ALL SELECT ? UNION ALL SELECT ? " + clause)) {
      statement.setInt(1, 10);
      statement.setInt(2, 20);
      statement.setInt(3, 30);
      statement.setInt(offsetParameter, 1);
      statement.setInt(limitParameter, 1);
      try (ResultSet result = statement.executeQuery()) {
        assertEquals(List.of(20), readRows(result));
      }
      statement.setInt(offsetParameter, 0);
      statement.setInt(limitParameter, 2);
      try (ResultSet result = statement.executeQuery()) {
        assertEquals(List.of(10, 20), readRows(result));
      }
    }
  }

  /**
   * Opens a connection using the shared fixtures directory.
   *
   * @return a JDBC connection
   * @throws SQLException
   *           if the connection cannot be opened
   */
  private Connection connect() throws SQLException {
    return DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
  }

  /**
   * Checks the rows produced by one SQL statement.
   *
   * @param connection
   *          the fixture connection
   * @param sql
   *          the statement to execute
   * @param expected
   *          the expected column values
   * @throws SQLException
   *           if query execution fails
   */
  private void assertRows(Connection connection, String sql, List<Integer> expected) throws SQLException {
    try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
      assertEquals(expected, readRows(result), sql);
    }
  }

  /**
   * Reads the first column of a result set.
   *
   * @param result
   *          the query result
   * @return the column values in encounter order
   * @throws SQLException
   *           if a row cannot be read
   */
  private List<Integer> readRows(ResultSet result) throws SQLException {
    List<Integer> values = new ArrayList<>();
    while (result.next()) {
      values.add(result.getInt(1));
    }
    return values;
  }
}
