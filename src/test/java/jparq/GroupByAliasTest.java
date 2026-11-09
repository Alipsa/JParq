package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests covering GROUP BY references to select-list aliases (issue #262).
 */
class GroupByAliasTest {

  /**
   * Single-table GROUP BY on a select-list alias must group by the aliased
   * expression rather than raising a syntax error.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void groupBySelectListAliasSingleTable() throws Exception {
    Map<String, Long> counts = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt
            .executeQuery("SELECT department AS dep, COUNT(*) AS cnt FROM acme.departments GROUP BY dep")) {
      while (rs.next()) {
        counts.put(rs.getString("dep"), rs.getLong("cnt"));
      }
    }
    assertEquals(Map.of("IT", 1L, "HR", 1L, "Sales", 1L), counts);
  }

  /**
   * Join queries must support GROUP BY on a select-list alias as well, returning
   * the same groups as grouping by the underlying column.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void groupBySelectListAliasJoin() throws Exception {
    Map<String, Long> byAlias = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT d.department AS dep, COUNT(*) AS cnt "
            + "FROM acme.employee_department ed JOIN acme.departments d ON ed.department = d.id GROUP BY dep")) {
      while (rs.next()) {
        byAlias.put(rs.getString("dep"), rs.getLong("cnt"));
      }
    }
    Map<String, Long> byColumn = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT d.department, COUNT(*) "
            + "FROM acme.employee_department ed JOIN acme.departments d ON ed.department = d.id "
            + "GROUP BY d.department")) {
      while (rs.next()) {
        byColumn.put(rs.getString(1), rs.getLong(2));
      }
    }
    assertEquals(byColumn, byAlias);
  }

  /**
   * GROUP BY on an alias of a computed expression groups by the expression value.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void groupByExpressionAlias() throws Exception {
    Map<Integer, Long> byAlias = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT gear + 1 AS g, COUNT(*) AS cnt FROM mtcars GROUP BY g")) {
      while (rs.next()) {
        byAlias.put(rs.getInt("g"), rs.getLong("cnt"));
      }
    }
    Map<Integer, Long> byExpression = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT gear + 1, COUNT(*) AS cnt FROM mtcars GROUP BY gear + 1")) {
      while (rs.next()) {
        byExpression.put(rs.getInt(1), rs.getLong(2));
      }
    }
    assertEquals(byExpression, byAlias);
    assertEquals(Map.of(4, 15L, 5, 12L, 6, 5L), byAlias);
  }

  /**
   * Alias references in GROUP BY are matched case-insensitively.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void groupByAliasIsCaseInsensitive() throws Exception {
    Map<Integer, Long> counts = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT cyl AS cylinder_count, COUNT(*) AS cnt FROM mtcars "
            + "GROUP BY CYLINDER_COUNT ORDER BY cylinder_count")) {
      while (rs.next()) {
        counts.put(rs.getInt("cylinder_count"), rs.getLong("cnt"));
      }
    }
    assertEquals(Map.of(4, 11L, 6, 7L, 8, 14L), counts);
  }

  /**
   * HAVING and ORDER BY may reference the same select-list alias used in GROUP
   * BY.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void havingAndOrderByOnAlias() throws Exception {
    Map<String, Long> counts = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT d.department AS dep, COUNT(*) AS cnt "
            + "FROM acme.employee_department ed JOIN acme.departments d ON ed.department = d.id "
            + "GROUP BY dep HAVING cnt > 1 ORDER BY dep DESC")) {
      while (rs.next()) {
        counts.put(rs.getString("dep"), rs.getLong("cnt"));
      }
    }
    assertEquals(2, counts.size());
    assertEquals(2L, counts.get("Sales"));
    assertEquals(2L, counts.get("IT"));
  }

  /**
   * GROUPING() arguments may reference a select-list alias used in GROUP BY.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void groupingFunctionAcceptsAlias() throws Exception {
    Map<String, int[]> rows = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT d.department AS dep, COUNT(*) AS cnt, GROUPING(dep) AS g "
            + "FROM acme.employee_department ed JOIN acme.departments d ON ed.department = d.id "
            + "GROUP BY ROLLUP(dep)")) {
      while (rs.next()) {
        rows.put(String.valueOf(rs.getString("dep")), new int[]{
            rs.getInt("cnt"), rs.getInt("g")
        });
      }
    }
    assertEquals(4, rows.size());
    assertEquals(0, rows.get("IT")[1]);
    assertEquals(2, rows.get("IT")[0]);
    assertEquals(0, rows.get("HR")[1]);
    assertEquals(1, rows.get("HR")[0]);
    assertEquals(5, rows.get("null")[0]);
    assertEquals(1, rows.get("null")[1]);
  }

  /**
   * Aliases of aggregate expressions cannot serve as grouping expressions; the
   * query must be rejected with a classified SQLException, not a raw runtime
   * exception.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void groupByAggregateAliasIsRejected() throws Exception {
    try (Connection connection = connection(); Statement stmt = connection.createStatement()) {
      SQLSyntaxErrorException failure = assertThrows(SQLSyntaxErrorException.class,
          () -> stmt.executeQuery("SELECT COUNT(*) AS total FROM mtcars GROUP BY total"));
      assertEquals("42000", failure.getSQLState());
    }
  }

  /**
   * An input column keeps its standard meaning when a select-list alias shares
   * its name: the query groups by the column, not by the aliased expression.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void inputColumnTakesPrecedenceOverAlias() throws Exception {
    List<String> rows = new ArrayList<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT CASE WHEN cyl > 4 THEN 'big' ELSE 'small' END AS cyl, "
            + "COUNT(*) AS n FROM mtcars GROUP BY cyl")) {
      while (rs.next()) {
        rows.add(rs.getString("cyl") + "|" + rs.getLong("n"));
      }
    }
    assertEquals(List.of("big|7", "small|11", "big|14"), rows);
  }

  /**
   * A GROUP BY name that matches an input column cannot be satisfied by an alias
   * of a different column; the query must be rejected because the select
   * expression is not grouped.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void aliasOfOtherColumnCannotSatisfyRealColumnGroupBy() throws Exception {
    try (Connection connection = connection(); Statement stmt = connection.createStatement()) {
      for (String sql : List.of("SELECT gear AS cyl, COUNT(*) AS n FROM mtcars GROUP BY cyl",
          "SELECT cyl AS gear, COUNT(*) AS n FROM mtcars GROUP BY gear")) {
        SQLSyntaxErrorException failure = assertThrows(SQLSyntaxErrorException.class, () -> stmt.executeQuery(sql));
        assertEquals("42803", failure.getSQLState(), "SQLState for " + sql);
      }
    }
  }

  /**
   * A GROUP BY name shared by more than one select-list alias is rejected as an
   * ambiguous reference with SQLState 42702.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void duplicateAliasIsRejectedAsAmbiguous() throws Exception {
    try (Connection connection = connection(); Statement stmt = connection.createStatement()) {
      SQLSyntaxErrorException failure = assertThrows(SQLSyntaxErrorException.class,
          () -> stmt.executeQuery("SELECT cyl AS x, gear AS x, COUNT(*) AS n FROM mtcars GROUP BY x"));
      assertEquals("42702", failure.getSQLState());
    }
    try (Connection connection = connection(); Statement stmt = connection.createStatement()) {
      SQLSyntaxErrorException failure = assertThrows(SQLSyntaxErrorException.class,
          () -> stmt.executeQuery("SELECT d.department AS x, ed.department AS x, COUNT(*) AS n "
              + "FROM acme.employee_department ed JOIN acme.departments d ON ed.department = d.id GROUP BY x"));
      assertEquals("42702", failure.getSQLState());
    }
  }

  /**
   * Input columns with quoted SQL names keep their standard meaning in GROUP BY,
   * even when the internal field name differs: derived tables, CTEs, and CTE
   * inputs joined with other tables must group by the column rather than by an
   * aliased expression of the same name.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void quotedInputColumnWinsOverAlias() throws Exception {
    assertCaseAliasGroupsByColumn("SELECT CASE WHEN \"Cyl\" > 4 THEN 'big' ELSE 'small' END AS \"Cyl\", "
        + "COUNT(*) AS n FROM (SELECT cyl AS \"Cyl\" FROM mtcars) t GROUP BY \"Cyl\"");

    assertCaseAliasGroupsByColumn("WITH t AS (SELECT cyl AS \"Cyl\" FROM mtcars) "
        + "SELECT CASE WHEN \"Cyl\" > 4 THEN 'big' ELSE 'small' END AS \"Cyl\", COUNT(*) AS n "
        + "FROM t GROUP BY \"Cyl\"");

    Map<String, Long> byName = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("WITH t AS (SELECT id AS \"Id\" FROM acme.employees) "
            + "SELECT CASE WHEN t.\"Id\" > 2 THEN 'big' ELSE 'small' END AS \"Id\", COUNT(*) AS n "
            + "FROM t JOIN acme.employee_department ed ON t.\"Id\" = ed.employee GROUP BY \"Id\"")) {
      while (rs.next()) {
        byName.merge(rs.getString("Id"), rs.getLong("n"), Long::sum);
      }
    }
    assertEquals(Map.of("small", 2L, "big", 3L), byName,
        "join over a quoted CTE column must group by the column, producing one row per employee");
  }

  private static void assertCaseAliasGroupsByColumn(String sql) throws Exception {
    List<String> rows = new ArrayList<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        rows.add(rs.getString(1) + "|" + rs.getLong(2));
      }
    }
    assertEquals(List.of("big|7", "small|11", "big|14"), rows, "column meaning must win for " + sql);
  }

  /**
   * A quoted-lowercase GROUP BY name falls back to the unquoted input column of
   * the same name: the column wins over a select-list alias.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void quotedLowercaseColumnWinsOverAlias() throws Exception {
    assertCaseAliasGroupsByColumn("SELECT CASE WHEN cyl > 4 THEN 'big' ELSE 'small' END AS \"cyl\", "
        + "COUNT(*) AS n FROM mtcars GROUP BY \"cyl\"");
    try (Connection connection = connection(); Statement stmt = connection.createStatement()) {
      SQLSyntaxErrorException failure = assertThrows(SQLSyntaxErrorException.class,
          () -> stmt.executeQuery("SELECT gear AS \"cyl\", COUNT(*) AS n FROM mtcars GROUP BY \"cyl\""));
      assertEquals("42803", failure.getSQLState());
    }
  }

  /**
   * Identical expressions labeling the same name are not ambiguous; the name
   * resolves to the shared expression.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void duplicateAliasOfSameExpressionIsAccepted() throws Exception {
    Map<Integer, Long> counts = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT cyl AS x, cyl AS X, COUNT(*) AS n FROM mtcars GROUP BY x")) {
      while (rs.next()) {
        counts.put(rs.getInt("x"), rs.getLong("n"));
      }
    }
    assertEquals(Map.of(4, 11L, 6, 7L, 8, 14L), counts);
  }

  /**
   * An aggregate alias counts toward alias ambiguity: a GROUP BY name shared
   * between a plain expression and an aggregate alias is rejected with SQLState
   * 42702 rather than silently grouping by the plain expression.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void aggregateAliasContributesToAmbiguity() throws Exception {
    try (Connection connection = connection(); Statement stmt = connection.createStatement()) {
      SQLSyntaxErrorException failure = assertThrows(SQLSyntaxErrorException.class,
          () -> stmt.executeQuery("SELECT cyl AS x, COUNT(*) AS x FROM mtcars GROUP BY x"));
      assertEquals("42702", failure.getSQLState());
    }
  }

  /**
   * The same expression with and without outer parentheses is one expression, not
   * an ambiguous pair.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void parenthesizedDuplicateAliasIsAccepted() throws Exception {
    Map<Integer, Long> counts = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT cyl + 1 AS x, (cyl + 1) AS x, COUNT(*) AS n FROM mtcars GROUP BY x")) {
      while (rs.next()) {
        counts.put(rs.getInt("x"), rs.getLong("n"));
      }
    }
    assertEquals(Map.of(5, 11L, 7, 7L, 9, 14L), counts);
  }

  /**
   * A row-count sample that picks no rows still reports the source schema, so
   * GROUP BY on an alias plans identically to the prepare-time pass and the query
   * simply returns no rows.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void zeroRowSampleWithAliasGroupByReturnsNoRows() throws Exception {
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT department AS dep, COUNT(*) AS n FROM acme.departments "
            + "TABLESAMPLE SYSTEM (0 ROWS) GROUP BY dep")) {
      assertFalse(rs.next(), "empty sample must return no rows");
    }
    Map<String, Long> counts = new LinkedHashMap<>();
    try (Connection connection = connection();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT department AS dep, COUNT(*) AS n FROM acme.departments "
            + "TABLESAMPLE SYSTEM (5 ROWS) GROUP BY dep")) {
      while (rs.next()) {
        counts.put(rs.getString("dep"), rs.getLong("n"));
      }
    }
    assertEquals(Map.of("IT", 1L, "HR", 1L, "Sales", 1L), counts);
  }

  private static Connection connection() throws SQLException {
    return DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
  }
}
