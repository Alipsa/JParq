package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLDataException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Regression tests for checked LIMIT, OFFSET, and FETCH row counts. */
class RowCountTest {

  /**
   * Opens a connection to the shared Parquet fixtures.
   *
   * @return the fixture connection
   * @throws SQLException if connection setup fails
   */
  private Connection connect() throws SQLException {
    return DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
  }

  /**
   * Supplies plain, set-operation, derived-table, and CTE query shapes.
   *
   * @param clause the row-limiting clause
   * @return queries exercising each parser path
   */
  private List<String> queries(String clause) {
    String plain = "SELECT model FROM mtcars " + clause;
    return List.of(plain, "SELECT model FROM mtcars UNION ALL SELECT model FROM mtcars ORDER BY model " + clause,
        "SELECT * FROM (" + plain + ") t", "WITH c AS (" + plain + ") SELECT * FROM c");
  }

  /**
   * Rejects unsupported expressions and negative counts with classified syntax errors.
   *
   * @param template the clause surrounding the row count
   * @throws SQLException if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {"LIMIT %s", "OFFSET %s ROWS", "FETCH FIRST %s ROWS ONLY"})
  void rejectsInvalidRowCounts(String template) throws SQLException {
    try (Connection connection = connect()) {
      for (String value : List.of("1 + 1", "(SELECT 2)", "1.5", "'2'", "NULL", "-1")) {
        for (String sql : queries(template.formatted(value))) {
          SQLException error = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(sql), sql);
          assertEquals("42000", error.getSQLState(), sql);
          for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            assertTrue(!(cause instanceof NumberFormatException), sql);
          }
        }
      }
    }
  }

  /**
   * Rejects large counts instead of wrapping to a smaller or negative integer.
   *
   * @param template the clause surrounding the row count
   * @throws SQLException if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {"LIMIT %s", "OFFSET %s ROWS", "FETCH NEXT %s ROWS ONLY"})
  void rejectsOverflow(String template) throws SQLException {
    try (Connection connection = connect()) {
      for (String value : List.of("2147483648", "3000000000", "4294967298", "9223372036854775808")) {
        for (String sql : queries(template.formatted(value))) {
          SQLException error = assertThrows(SQLDataException.class, () -> connection.prepareStatement(sql), sql);
          assertEquals("22003", error.getSQLState(), sql);
          assertTrue(error.getMessage().contains(value), sql);
        }
      }
    }
  }

  /**
   * Verifies zero, ordinary, default, and maximum supported row counts.
   *
   * @throws SQLException if query execution fails
   */
  @Test
  void preservesLiteralSemantics() throws SQLException {
    try (Connection connection = connect()) {
      for (String sql : queries("LIMIT 0")) {
        assertEquals(0, count(connection, sql), sql);
      }
      for (String sql : queries("FETCH FIRST 0 ROWS ONLY")) {
        assertEquals(0, count(connection, sql), sql);
      }
      for (String prefix : List.of("SELECT model FROM mtcars", "SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n")) {
        int total = count(connection, prefix);
        assertEquals(total, count(connection, prefix + " LIMIT 2147483647"));
        assertEquals(total, count(connection, prefix + " FETCH FIRST 2147483647 ROWS ONLY"));
        assertEquals(0, count(connection, prefix + " OFFSET 2147483647 ROWS"));
        assertEquals(total, count(connection, prefix + " OFFSET 0 ROWS"));
        assertEquals(1, count(connection, prefix + " FETCH FIRST ROW ONLY"));
        assertEquals(1, count(connection, prefix + " OFFSET 1 ROWS FETCH NEXT 1 ROW ONLY"));
        assertEquals(1, count(connection, prefix + " LIMIT +1"));
      }
    }
  }

  /**
   * Checks valid and overflowing row counts after JDBC parameter binding.
   *
   * @param template the clause surrounding the parameter
   * @throws SQLException if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {"LIMIT %s", "OFFSET %s ROWS", "FETCH FIRST %s ROWS ONLY"})
  void checksBoundParameters(String template) throws SQLException {
    try (Connection connection = connect();
        PreparedStatement statement = connection.prepareStatement("SELECT model FROM mtcars " + template.formatted("?"))) {
      statement.setInt(1, 2);
      try (ResultSet rows = statement.executeQuery()) {
        int count = 0;
        while (rows.next()) {
          count++;
        }
        assertEquals(template.startsWith("OFFSET") ? 30 : 2, count);
      }
      statement.setLong(1, 4294967298L);
      SQLException error = assertThrows(SQLDataException.class, statement::executeQuery);
      assertEquals("22003", error.getSQLState());
      statement.setInt(1, -1);
      assertEquals("42000", assertThrows(SQLSyntaxErrorException.class, statement::executeQuery).getSQLState());
    }
  }

  /**
   * Counts the rows returned by a query.
   *
   * @param connection the fixture connection
   * @param sql the query to execute
   * @return the number of result rows
   * @throws SQLException if execution fails
   */
  private int count(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
      int count = 0;
      while (rows.next()) {
        count++;
      }
      return count;
    }
  }
}
