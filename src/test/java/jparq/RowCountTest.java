package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
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
   * @throws SQLException
   *           if connection setup fails
   */
  private Connection connect() throws SQLException {
    return DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
  }

  /**
   * Supplies plain, set-operation, derived-table, and CTE query shapes.
   *
   * @param clause
   *          the row-limiting clause
   * @return queries exercising each parser path
   */
  private List<String> queries(String clause) {
    String plain = "SELECT model FROM mtcars " + clause;
    return List.of(plain, "SELECT model FROM mtcars UNION ALL SELECT model FROM mtcars ORDER BY model " + clause,
        "SELECT * FROM (" + plain + ") t", "WITH c AS (" + plain + ") SELECT * FROM c");
  }

  /**
   * Rejects unsupported expressions with classified syntax errors.
   *
   * @param template
   *          the clause surrounding the row count
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LIMIT %s", "OFFSET %s ROWS", "FETCH FIRST %s ROWS ONLY"
  })
  void rejectsInvalidRowCounts(String template) throws SQLException {
    try (Connection connection = connect()) {
      for (String value : List.of("1 + 1", "(SELECT 2)", "1.5", "'2'", "NULL", "~1", "~0", "~5")) {
        for (String sql : queries(template.formatted(value))) {
          SQLException error = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(sql), sql);
          assertEquals("42000", error.getSQLState(), sql);
          for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            assertFalse(cause instanceof NumberFormatException, sql);
          }
        }
      }
    }
  }

  /**
   * Rejects large counts instead of wrapping to a smaller or negative integer.
   *
   * @param template
   *          the clause surrounding the row count
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LIMIT %s", "OFFSET %s ROWS", "FETCH NEXT %s ROWS ONLY"
  })
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
   * @throws SQLException
   *           if query execution fails
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
   * @param template
   *          the clause surrounding the parameter
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LIMIT %s", "OFFSET %s ROWS", "FETCH FIRST %s ROWS ONLY"
  })
  void checksBoundParameters(String template) throws SQLException {
    String sql = "SELECT model FROM mtcars " + template.formatted("?");
    try (Connection connection = connect(); PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, 2);
      try (ResultSet rows = statement.executeQuery()) {
        assertEquals(template.startsWith("OFFSET") ? 30 : 2, count(rows));
      }
      statement.setLong(1, 4294967298L);
      SQLException error = assertThrows(SQLDataException.class, statement::executeQuery);
      assertEquals("22003", error.getSQLState());
      statement.setInt(1, -1);
      String state = template.startsWith("OFFSET") ? "2201X" : "2201W";
      assertEquals(state, assertThrows(SQLDataException.class, statement::executeQuery).getSQLState());
      statement.setNull(1, Types.INTEGER);
      assertEquals(state, assertThrows(SQLDataException.class, statement::executeQuery).getSQLState());
    }
  }

  /**
   * Classifies negative literals in plain and grouped queries and nested scopes.
   *
   * @param template
   *          the row-limiting clause
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LIMIT %s", "OFFSET %s ROWS", "FETCH FIRST %s ROWS ONLY"
  })
  void rejectsNegativeCountsAsDataErrors(String template) throws SQLException {
    String state = template.startsWith("OFFSET") ? "2201X" : "2201W";
    try (Connection connection = connect()) {
      for (String sql : queries(template.formatted("-1"))) {
        assertEquals(state,
            assertThrows(SQLDataException.class, () -> connection.prepareStatement(sql), sql).getSQLState());
      }
      String sql = "SELECT cyl FROM mtcars GROUP BY cyl " + template.formatted("-1");
      assertEquals(state, assertThrows(SQLDataException.class, () -> connection.prepareStatement(sql)).getSQLState());
    }
  }

  /**
   * Rejects comma-form LIMIT rather than silently ignoring its offset.
   *
   * @throws SQLException
   *           if connection setup fails
   */
  @Test
  void rejectsCommaLimit() throws SQLException {
    try (Connection connection = connect()) {
      for (String clause : List.of("LIMIT 5, 2", "LIMIT -1, 2", "LIMIT ~1, 2", "LIMIT 4294967298, 2")) {
        for (String sql : queries(clause)) {
          assertEquals("0A000",
              assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement(sql), sql)
                  .getSQLState());
        }
      }
    }
  }

  /**
   * Converts integral numeric parameters in row counts while retaining ordinary
   * decimal scales.
   *
   * @param template
   *          the row-limiting clause
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LIMIT %s", "OFFSET %s ROWS", "FETCH NEXT %s ROWS ONLY"
  })
  void normalizesNumericParametersOnlyInRowCounts(String template) throws SQLException {
    String state = template.startsWith("OFFSET") ? "2201X" : "2201W";
    try (Connection connection = connect()) {
      for (String sql : queries(template.formatted("?"))) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          int expected = template.startsWith("OFFSET") ? (sql.contains("UNION ALL") ? 62 : 30) : 2;
          for (Number value : List.of(2.0, 2.0f, new BigDecimal("2.00"), new BigDecimal("2E+0"))) {
            statement.setObject(1, value);
            try (ResultSet rows = statement.executeQuery()) {
              assertEquals(expected, count(rows), sql);
            }
          }
          for (Object value : new Object[]{
              null, -1, new BigDecimal("-1.00"), 1.5, Double.NaN, Double.POSITIVE_INFINITY
          }) {
            statement.setObject(1, value);
            assertEquals(state, assertThrows(SQLDataException.class, statement::executeQuery, sql).getSQLState());
          }
          statement.setBigDecimal(1, new BigDecimal("2147483648.00"));
          assertEquals("22003", assertThrows(SQLDataException.class, statement::executeQuery, sql).getSQLState());
        }
      }
      try (PreparedStatement statement = connection.prepareStatement("SELECT ? AS d FROM mtcars LIMIT ?")) {
        statement.setBigDecimal(1, new BigDecimal("2.00"));
        statement.setDouble(2, 2.0);
        try (ResultSet rows = statement.executeQuery()) {
          assertTrue(rows.next());
          assertEquals(new BigDecimal("2.00"), rows.getBigDecimal(1));
          assertTrue(rows.next());
          assertFalse(rows.next());
        }
      }
    }
  }

  /**
   * Converts numeric strings and classifies invalid bound values as data errors.
   *
   * @param template
   *          the row-limiting clause
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LIMIT %s", "OFFSET %s ROWS", "FETCH NEXT %s ROWS ONLY"
  })
  void classifiesNonnumericParametersAsDataErrors(String template) throws SQLException {
    String state = template.startsWith("OFFSET") ? "2201X" : "2201W";
    try (Connection connection = connect()) {
      for (String sql : queries(template.formatted("?"))) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          int expected = template.startsWith("OFFSET") ? (sql.contains("UNION ALL") ? 62 : 30) : 2;
          for (String value : List.of("2", " 2 ", "2.00", "2e0")) {
            statement.setString(1, value);
            try (ResultSet rows = statement.executeQuery()) {
              assertEquals(expected, count(rows), sql);
            }
          }
          for (String value : List.of("two", "", "NaN", "Infinity", "2' OR TRUE")) {
            statement.setString(1, value);
            assertEquals("22018", assertThrows(SQLDataException.class, statement::executeQuery, sql).getSQLState());
          }
          for (Object value : List.of(true, false, new byte[]{
              2
          }, java.sql.Date.valueOf("2024-01-01"), "-1", "1.5")) {
            statement.setObject(1, value);
            assertEquals(state, assertThrows(SQLDataException.class, statement::executeQuery, sql).getSQLState());
          }
          statement.setString(1, "2147483648");
          assertEquals("22003", assertThrows(SQLDataException.class, statement::executeQuery, sql).getSQLState());
        }
      }
    }
  }

  /**
   * Converts Character values using the same rules as String parameters.
   *
   * @param template
   *          the row-limiting clause
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LIMIT %s", "OFFSET %s ROWS", "FETCH NEXT %s ROWS ONLY"
  })
  void normalizesCharacterParameters(String template) throws SQLException {
    try (Connection connection = connect()) {
      for (String sql : queries(template.formatted("?"))) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          statement.setObject(1, '2');
          try (ResultSet rows = statement.executeQuery()) {
            assertEquals(template.startsWith("OFFSET") ? (sql.contains("UNION ALL") ? 62 : 30) : 2, count(rows), sql);
          }
          statement.setObject(1, 'x');
          assertEquals("22018", assertThrows(SQLDataException.class, statement::executeQuery, sql).getSQLState());
        }
      }
    }
  }

  /**
   * Retains original parameter ordering in directly parsed CTEs with later
   * select-list markers.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void cteRowCountParametersRetainOriginalPositions() throws SQLException {
    String sql = "WITH c AS (SELECT model FROM mtcars LIMIT ?) SELECT ? AS tag, model FROM c";
    try (Connection connection = connect(); PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setObject(1, '2');
      statement.setString(2, "tag");
      try (ResultSet rows = statement.executeQuery()) {
        assertTrue(rows.next());
        assertEquals("tag", rows.getString("tag"));
        assertTrue(rows.next());
        assertFalse(rows.next());
      }
    }
  }

  /**
   * Binds independent LIMIT and OFFSET parameters after grouped result
   * processing.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void supportsGroupedLimitAndOffsetParameters() throws SQLException {
    String sql = "SELECT DISTINCT COUNT(*) c FROM mtcars GROUP BY cyl ORDER BY c LIMIT ? OFFSET ?";
    try (Connection connection = connect(); PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setBigDecimal(1, new BigDecimal("2.00"));
      statement.setDouble(2, 1.0);
      try (ResultSet rows = statement.executeQuery()) {
        assertEquals(2, count(rows));
      }
      statement.setNull(2, Types.INTEGER);
      assertEquals("2201X", assertThrows(SQLDataException.class, statement::executeQuery).getSQLState());
      statement.setInt(2, 0);
      statement.setNull(1, Types.INTEGER);
      assertEquals("2201W", assertThrows(SQLDataException.class, statement::executeQuery).getSQLState());
    }
  }

  /**
   * Counts rows while leaving result-set ownership with the caller.
   *
   * @param rows
   *          the result set to consume
   * @return the number of rows
   * @throws SQLException
   *           if iteration fails
   */
  private int count(ResultSet rows) throws SQLException {
    int count = 0;
    while (rows.next()) {
      count++;
    }
    return count;
  }

  /**
   * Counts the rows returned by a query.
   *
   * @param connection
   *          the fixture connection
   * @param sql
   *          the query to execute
   * @return the number of result rows
   * @throws SQLException
   *           if execution fails
   */
  private int count(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
      return count(rows);
    }
  }
}
