package jparq.cte;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that native CTE parsing preserves identifier syntax and literal
 * contents.
 */
class CteIdentifierTest {

  /**
   * Supplies reserved words that cannot be regular CTE identifiers.
   *
   * @return reserved SQL keywords
   */
  private static Stream<String> reservedNames() {
    return Stream.of("select", "union", "as", "is", "cross", "full", "inner", "outer", "using", "lateral", "natural",
        "when", "else", "between", "like", "fetch", "window");
  }

  /**
   * Opens the shared Parquet fixtures.
   *
   * @return a JDBC connection
   * @throws SQLException
   *           if connection setup fails
   */
  private Connection connect() throws SQLException {
    return DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
  }

  /**
   * Rejects invalid regular identifiers without accepting SQL after a textual
   * rewrite.
   *
   * @param name
   *          the reserved word used without identifier quotes
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @MethodSource("reservedNames")
  void rejectsUnquotedReservedCteNames(String name) throws SQLException {
    try (Connection connection = connect()) {
      for (String condition : List.of("cyl = 4", "cyl = ?")) {
        for (String projection : List.of("q.model", "'from " + name + " q' AS literal_text, q.model")) {
          String sql = "WITH " + name + " AS (SELECT model FROM mtcars WHERE " + condition + ") SELECT " + projection
              + " FROM " + name + " q";
          var error = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(sql), sql);
          assertEquals("42000", error.getSQLState(), sql);
        }
      }
    }
  }

  /**
   * Allows the same words as delimited identifiers and retains the CTE's filter.
   *
   * @param name
   *          the reserved word to quote
   * @throws SQLException
   *           if execution fails
   */
  @ParameterizedTest
  @MethodSource("reservedNames")
  void acceptsQuotedReservedCteNames(String name) throws SQLException {
    String quoted = "\"" + name + "\"";
    String sql = "WITH " + quoted + " AS (SELECT model FROM mtcars WHERE cyl = ?) SELECT q.model FROM " + quoted + " q";
    try (Connection connection = connect(); PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, 4);
      try (ResultSet rows = statement.executeQuery()) {
        int count = 0;
        while (rows.next()) {
          count++;
        }
        assertEquals(11, count, sql);
      }
    }
  }

  /**
   * Keeps reserved alias rejection consistent with CTE-name rejection.
   *
   * @throws SQLException
   *           if connection setup fails
   */
  @Test
  void rejectsUnquotedReservedDerivedTableAlias() throws SQLException {
    try (Connection connection = connect()) {
      String sql = "SELECT model FROM (SELECT model FROM mtcars) select";
      assertEquals("42000",
          assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(sql)).getSQLState());
    }
  }

  /**
   * Keeps text that resembles a FROM clause separate from actual CTE references.
   *
   * @throws SQLException
   *           if execution fails
   */
  @Test
  void preservesFromTextInsideLiterals() throws SQLException {
    String sql = "WITH c AS (SELECT ? AS n) SELECT 'from c' AS s, n FROM c";
    try (Connection connection = connect(); PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, 7);
      try (ResultSet rows = statement.executeQuery()) {
        assertTrue(rows.next());
        assertEquals("from c", rows.getString("s"));
        assertEquals(7, rows.getInt("n"));
        assertFalse(rows.next());
      }
    }
  }
}
