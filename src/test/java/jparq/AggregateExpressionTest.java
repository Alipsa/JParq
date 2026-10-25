package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Regression coverage for scalar expressions over aggregate results (#231). */
class AggregateExpressionTest {
  /**
   * Tests arithmetic and scalar wrappers, including labels and result
   * cardinality.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void ungroupedExpressions(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertRows(connection, "SELECT SUM(cyl) * 2 FROM mtcars", List.of(List.of("396")));
      assertRows(connection, "SELECT SUM(cyl) * 2 AS x FROM mtcars", List.of(List.of("396")));
      assertRows(connection, "SELECT 2 * COUNT(*) FROM mtcars", List.of(List.of("64")));
      assertRows(connection, "SELECT SUM(cyl) + SUM(gear) FROM mtcars", List.of(List.of("316")));
      assertRows(connection,
          "SELECT COALESCE(MAX(cyl), 0), ABS(SUM(cyl)), ROUND(AVG(mpg), 1), " + "NULLIF(COUNT(*), 0) FROM mtcars",
          List.of(List.of("8", "198", "20.1", "32")));
      assertRows(connection, "SELECT UPPER(MAX(model)) FROM mtcars", List.of(List.of("VOLVO 142E")));
      assertRows(connection, "SELECT CASE WHEN COUNT(*) > 0 THEN SUM(cyl) * 2 ELSE 0 END FROM mtcars",
          List.of(List.of("396")));
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT SUM(cyl) * 2 AS x FROM mtcars")) {
        assertEquals("x", result.getMetaData().getColumnLabel(1));
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
      }
    }
  }

  /**
   * Tests grouping, hidden aggregates, HAVING, ORDER BY, and aliases.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void groupedExpressions(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertRows(connection,
          "SELECT cyl, COUNT(*) + 1, COALESCE(SUM(cyl), 0) FROM mtcars " + "GROUP BY cyl ORDER BY cyl",
          List.of(List.of("4", "12", "44"), List.of("6", "8", "42"), List.of("8", "15", "112")));
      assertRows(connection, "SELECT cyl, SUM(mpg) * 2 FROM mtcars GROUP BY cyl ORDER BY cyl",
          List.of(List.of("4", "586.6"), List.of("6", "276.4"), List.of("8", "422.8")));
      assertRows(connection,
          "SELECT cyl, COUNT(*) + 1 AS n FROM mtcars GROUP BY cyl "
              + "HAVING ABS(COUNT(*)) > 10 ORDER BY ABS(COUNT(*)) DESC",
          List.of(List.of("8", "15"), List.of("4", "12")));
      assertRows(connection,
          "SELECT cyl FROM mtcars GROUP BY cyl HAVING SUM(mpg) * 2 > 400 " + "ORDER BY SUM(mpg) * 2 DESC",
          List.of(List.of("4"), List.of("8")));
      assertRows(connection, "SELECT cyl FROM mtcars GROUP BY cyl ORDER BY ABS(COUNT(*)) DESC",
          List.of(List.of("8"), List.of("4"), List.of("6")));
      assertRows(connection,
          "SELECT COUNT(*) + 1 AS n FROM mtcars HAVING COALESCE(SUM(cyl), 0) > 100 " + "ORDER BY ABS(COUNT(*))",
          List.of(List.of("33")));
      assertRows(connection, "SELECT cyl, COUNT(*) + 1 AS n FROM mtcars GROUP BY cyl HAVING n > 10 ORDER BY n DESC",
          List.of(List.of("8", "15"), List.of("4", "12")));
    }
  }

  /**
   * Tests NULLs, empty inputs, derived outputs, and ungrouped-column validation.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if fixture setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void boundaries(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertRows(connection, "SELECT COALESCE(SUM(cyl), 0), COUNT(*) + 1, SUM(cyl) * 2 FROM mtcars WHERE cyl < 0",
          List.of(java.util.Arrays.asList("0", "1", null)));
      assertRows(connection, "SELECT cyl, COUNT(*) + 1 FROM mtcars WHERE cyl < 0 GROUP BY cyl", List.of());
      assertRows(connection, "SELECT x FROM (SELECT SUM(cyl) * 2 AS x FROM mtcars) d", List.of(List.of("396")));
      assertRows(connection, "WITH c AS (SELECT COUNT(*) + 1 AS n FROM mtcars) SELECT n FROM c",
          List.of(List.of("33")));
      for (String suffix : List.of("", " WHERE cyl < 0", " GROUP BY cyl")) {
        SQLException failure = assertThrows(SQLException.class,
            () -> connection.prepareStatement("SELECT SUM(cyl) + mpg FROM mtcars" + suffix));
        assertEquals("42000", failure.getSQLState());
      }
    }
  }

  /**
   * Preserves grouping scope and defers scalar evaluation until needed.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void evaluationScope(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertRows(connection, "SELECT COUNT(*) / 0 FROM mtcars HAVING COUNT(*) = 0", List.of());
      assertRows(connection, "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE COUNT(*) / 0 END FROM mtcars",
          List.of(List.of("1")));
      assertRows(connection, "SELECT 1 FROM mtcars HAVING COUNT(*) > 0", List.of(List.of("1")));
      assertRows(connection, "SELECT 1 FROM mtcars ORDER BY ABS(COUNT(*))", List.of(List.of("1")));
      assertRows(connection, "SELECT cyl + COUNT(*) FROM mtcars GROUP BY cyl ORDER BY cyl",
          List.of(List.of("15"), List.of("13"), List.of("22")));
      assertRows(connection, "SELECT ABS(cyl + 1) + COUNT(*) FROM mtcars GROUP BY cyl + 1 ORDER BY cyl + 1",
          List.of(List.of("16"), List.of("14"), List.of("23")));
      assertRows(connection,
          "SELECT ABS(CAST(cyl AS INTEGER)) + COUNT(*) FROM mtcars "
              + "GROUP BY CAST(cyl AS INTEGER) ORDER BY CAST(cyl AS INTEGER)",
          List.of(List.of("15"), List.of("13"), List.of("22")));
      assertRows(connection, "SELECT ABS(-cyl) + COUNT(*) FROM mtcars GROUP BY -cyl ORDER BY -cyl DESC",
          List.of(List.of("15"), List.of("13"), List.of("22")));
      assertRows(connection,
          "SELECT COALESCE(cyl, 0) + COUNT(*) FROM mtcars GROUP BY ROLLUP(cyl) " + "ORDER BY GROUPING(cyl), cyl",
          List.of(List.of("15"), List.of("13"), List.of("22"), List.of("32")));
      assertRows(connection, "SELECT COUNT(*) + (SELECT SUM(cyl) FROM mtcars) FROM mtcars", List.of(List.of("230")));
      assertRows(connection,
          "SELECT m.cyl, COUNT(*) + (SELECT MAX(n.cyl) FROM mtcars n WHERE n.cyl = m.cyl) "
              + "FROM mtcars m GROUP BY m.cyl ORDER BY m.cyl",
          List.of(List.of("4", "15"), List.of("6", "13"), List.of("8", "22")));
      for (String where : List.of("", " WHERE m.cyl < 0")) {
        SQLException failure = assertThrows(SQLException.class, () -> {
          try (var statement = connection.createStatement()) {
            statement.executeQuery("SELECT COUNT(*) + (SELECT MAX(n.cyl) FROM mtcars n WHERE n.cyl = m.mpg) "
                + "FROM mtcars m" + where + " GROUP BY m.cyl");
          }
        });
        assertEquals("42000", failure.getSQLState());
      }
      SQLException nested = assertThrows(SQLException.class,
          () -> connection.prepareStatement("SELECT SUM(SUM(cyl)) FROM mtcars"));
      assertEquals("42000", nested.getSQLState());
      for (String order : List.of("mpg", "ABS(mpg)", "mpg + COUNT(*)")) {
        SQLException failure = assertThrows(SQLException.class, () -> {
          try (var statement = connection.createStatement()) {
            statement.executeQuery("SELECT cyl, COUNT(*) FROM mtcars GROUP BY cyl ORDER BY " + order);
          }
        });
        assertEquals("42000", failure.getSQLState());
      }
    }
  }

  /**
   * Opens the fixture database with the requested pushdown setting.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @return fixture connection
   * @throws Exception
   *           if fixture setup or connection creation fails
   */
  private static Connection connect(boolean pushdown) throws Exception {
    return DriverManager.getConnection(
        "jdbc:jparq:" + Path.of(AggregateExpressionTest.class.getResource("/mtcars.parquet").toURI()).getParent()
            + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
  }

  /**
   * Compares all rows, normalizing numeric representations for SQL equality.
   *
   * @param connection
   *          fixture connection
   * @param sql
   *          query to execute
   * @param expected
   *          expected rows
   * @throws SQLException
   *           if query execution fails
   */
  private static void assertRows(Connection connection, String sql, List<List<String>> expected) throws SQLException {
    List<List<String>> rows = new ArrayList<>();
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        List<String> row = new ArrayList<>();
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
          Object value = result.getObject(i);
          row.add(value instanceof Number
              ? new BigDecimal(value.toString()).stripTrailingZeros().toPlainString()
              : value == null ? null : value.toString());
        }
        rows.add(row);
      }
    }
    assertEquals(expected, rows, sql);
  }
}
