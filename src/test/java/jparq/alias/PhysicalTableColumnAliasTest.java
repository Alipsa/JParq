package jparq.alias;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies positional column alias lists on physical Parquet table references
 * (issue 263), e.g. {@code SELECT a FROM employees e(a, b, c)}.
 */
class PhysicalTableColumnAliasTest {

  /**
   * Renames every column positionally, hides the original names, and supports
   * wildcards, qualifiers, and parenthesized references.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void aliasesRenamePhysicalTableColumns(boolean predicate, boolean projection) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/acme").toAbsolutePath() + "?predicatePushdown=" + predicate
        + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String source : List.of("employees e(a, b, c)", "employees AS e(a, b, c)", "(employees) e(a, b, c)")) {
        assertAliasedRows(connection, "SELECT a, b, c FROM " + source + " ORDER BY a");
        assertAliasedRows(connection, "SELECT * FROM " + source + " ORDER BY a");
        assertAliasedRows(connection, "SELECT e.a, e.b, e.c FROM " + source + " WHERE e.c IS NOT NULL ORDER BY e.a");
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT * FROM " + source + " ORDER BY a")) {
          assertEquals("a", result.getMetaData().getColumnLabel(1));
          assertEquals("b", result.getMetaData().getColumnLabel(2));
          assertEquals("c", result.getMetaData().getColumnLabel(3));
        }
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT a FROM " + source + " ORDER BY a")) {
          assertEquals("a", result.getMetaData().getColumnLabel(1));
          List<String[]> expected = controlRows(connection);
          for (String[] row : expected) {
            assertTrue(result.next());
            assertEquals(row[0], result.getString(1));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Rejects references to the original column names once aliases are declared.
   *
   * @param column
   *          original column name that must no longer resolve
   * @throws Exception
   *           if the connection cannot be opened
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "id", "first_name", "last_name"
  })
  void originalNamesAreHidden(String column) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources/acme"))) {
      for (String projection : List.of(column, "e." + column)) {
        String sql = "SELECT " + projection + " FROM employees e(a, b, c)";
        try (var statement = connection.createStatement()) {
          SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql));
          assertEquals("42000", error.getSQLState(), error.getMessage());
        }
      }
    }
  }

  /**
   * Rejects alias lists whose count differs from the table width, in plain and
   * join contexts.
   *
   * @param aliases
   *          invalid column alias list
   * @throws Exception
   *           if the connection cannot be opened
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "a", "a, b", "a, b, c, d"
  })
  void aliasCountsMustMatchTableWidth(String aliases) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources/acme"))) {
      for (String source : List.of("employees e(" + aliases + ")",
          "employees e(" + aliases + ") JOIN (VALUES (1)) s(z) ON TRUE")) {
        try (var statement = connection.createStatement()) {
          SQLException error = assertThrows(SQLException.class,
              () -> statement.executeQuery("SELECT * FROM " + source));
          assertEquals("42000", error.getSQLState(), error.getMessage());
          assertTrue(error.getMessage().contains("Table alias declares"), error.getMessage());
        }
      }
    }
  }

  /**
   * Rejects duplicate alias names during parsing.
   *
   * @throws Exception
   *           if the connection cannot be opened
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "a, a, b", "a, A, b", "\"Same\", \"Same\", b"
  })
  void duplicateAliasesAreRejected(String aliases) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources/acme"))) {
      String sql = "SELECT * FROM employees e(" + aliases + ")";
      try (var statement = connection.createStatement()) {
        SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql));
        assertEquals("42000", error.getSQLState(), error.getMessage());
        assertTrue(error.getMessage().contains("Duplicate column alias"), error.getMessage());
      }
    }
  }

  /**
   * Applies aliases to physical tables on both sides of a join, including filters
   * on renamed columns.
   *
   * @param predicate
   *          predicate pushdown setting
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void aliasesWorkInJoins(boolean predicate) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/acme").toAbsolutePath() + "?predicatePushdown=" + predicate
        + "&projectionPushdown=" + predicate;
    try (var connection = DriverManager.getConnection(url)) {
      List<String[]> expected = controlRows(connection);
      String sql = "SELECT e.a, e.b FROM employees e(a, b, c) JOIN (VALUES (1)) s(z) ON e.a >= s.z"
          + " WHERE e.c IS NOT NULL ORDER BY e.a";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
        for (String[] row : expected) {
          assertTrue(result.next(), sql);
          assertEquals(row[0], result.getString(1), sql);
          assertEquals(row[1], result.getString(2), sql);
        }
        assertFalse(result.next(), sql);
      }
      sql = "SELECT s.z, e.c FROM (VALUES (1)) s(z) JOIN employees AS e(a, b, c) ON s.z <= e.a"
          + " WHERE e.a <= 2 ORDER BY e.a";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
        int observed = 0;
        for (String[] row : expected) {
          if (Integer.parseInt(row[0]) > 2) {
            break;
          }
          assertTrue(result.next(), sql);
          assertEquals("1", result.getString(1), sql);
          assertEquals(row[2], result.getString(2), sql);
          observed++;
        }
        assertTrue(observed > 0);
        assertFalse(result.next(), sql);
      }
    }
  }

  private static void assertAliasedRows(Connection connection, String sql) throws SQLException {
    List<String[]> expected = controlRows(connection);
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      for (String[] row : expected) {
        assertTrue(result.next(), sql);
        for (int i = 0; i < row.length; i++) {
          assertEquals(row[i], result.getString(i + 1), sql);
        }
      }
      assertFalse(result.next(), sql);
    }
  }

  private static List<String[]> controlRows(Connection connection) throws SQLException {
    List<String[]> rows = new ArrayList<>();
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT id, first_name, last_name FROM employees ORDER BY id")) {
      while (result.next()) {
        rows.add(new String[]{
            result.getString(1), result.getString(2), result.getString(3)
        });
      }
    }
    return rows;
  }
}
