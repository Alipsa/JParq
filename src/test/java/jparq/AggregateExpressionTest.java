package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression coverage for scalar expressions over aggregates and grouping keys
 * (#231, #247).
 */
class AggregateExpressionTest {
  /**
   * Evaluates scalar functions over grouping keys without requiring aggregates.
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
  void scalarGroupingKeys(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String expression : List.of("ABS(cyl)", "COALESCE(cyl, 4)", "ABS(COALESCE(cyl, 4))")) {
        assertRows(connection, "SELECT cyl, " + expression + " FROM mtcars GROUP BY cyl ORDER BY cyl",
            List.of(List.of("4", "4"), List.of("6", "6"), List.of("8", "8")));
      }
      assertRows(connection, "SELECT ABS(m.cyl) AS magnitude FROM mtcars m GROUP BY m.cyl ORDER BY magnitude",
          List.of(List.of("4"), List.of("6"), List.of("8")));
      assertRows(connection, "SELECT ABS(cyl + 1) FROM mtcars GROUP BY cyl + 1 ORDER BY cyl + 1",
          List.of(List.of("5"), List.of("7"), List.of("9")));
      assertRows(connection, "SELECT cyl, ABS(cyl) FROM mtcars WHERE cyl < 0 GROUP BY cyl", List.of());
    }
  }

  /**
   * Filters groups using scalar functions over visible and hidden grouping keys.
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
  void scalarGroupingKeysInHaving(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertRows(connection, "SELECT cyl, COUNT(*) FROM mtcars GROUP BY cyl HAVING ABS(cyl) > 0 ORDER BY cyl",
          List.of(List.of("4", "11"), List.of("6", "7"), List.of("8", "14")));
      assertRows(connection,
          "SELECT cyl, COUNT(*) FROM mtcars GROUP BY cyl HAVING NULLIF(cyl, 4) IS NOT NULL ORDER BY cyl",
          List.of(List.of("6", "7"), List.of("8", "14")));
      assertRows(connection, "SELECT COUNT(*) FROM mtcars m GROUP BY m.cyl HAVING ABS(m.cyl) > 4 ORDER BY m.cyl",
          List.of(List.of("7"), List.of("14")));
      assertRows(connection, "SELECT cyl FROM mtcars GROUP BY cyl HAVING COALESCE(MAX(mpg), 0) > 25 ORDER BY cyl",
          List.of(List.of("4")));
      assertRows(connection,
          "SELECT cyl FROM mtcars GROUP BY cyl HAVING ABS(COALESCE(NULLIF(cyl, 4), 0)) > 0 ORDER BY cyl",
          List.of(List.of("6"), List.of("8")));
    }
  }

  /**
   * Preserves NULL keys and keys suppressed by grouping sets in scalar functions.
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
  void nullableScalarGroupingKeys(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertRows(connection,
          "SELECT COALESCE(cyl, 0), COUNT(*) FROM mtcars GROUP BY ROLLUP(cyl) " + "HAVING ABS(COALESCE(cyl, 0)) = 0",
          List.of(List.of("0", "32")));
      assertRows(connection,
          "SELECT COALESCE(k, 0), COUNT(*) FROM (SELECT NULLIF(cyl, 4) AS k FROM mtcars) d "
              + "GROUP BY k HAVING NULLIF(k, 6) IS NULL ORDER BY COALESCE(k, 0)",
          List.of(List.of("0", "11"), List.of("6", "7")));
      assertRows(connection, "SELECT COALESCE(cyl, 0) FROM mtcars GROUP BY ROLLUP(cyl) ORDER BY COALESCE(cyl, 0)",
          List.of(List.of("0"), List.of("4"), List.of("6"), List.of("8")));
    }
  }

  /**
   * Classifies unknown functions and rejects scalar references to ungrouped keys.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if fixture setup or connection creation fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void scalarGroupingFailures(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String sql : List.of("SELECT cyl FROM mtcars GROUP BY cyl HAVING NO_SUCH(MAX(mpg)) > 1",
          "SELECT cyl FROM mtcars GROUP BY cyl HAVING NO_SUCH(cyl) > 1",
          "SELECT NO_SUCH(cyl) FROM mtcars GROUP BY cyl")) {
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> assertRows(connection, sql, List.of()), sql);
        assertEquals("42000", failure.getSQLState(), sql);
      }
      for (String where : List.of("", " WHERE cyl < 0")) {
        for (String sql : List.of("SELECT ABS(mpg) FROM mtcars" + where + " GROUP BY cyl",
            "SELECT cyl FROM mtcars" + where + " GROUP BY cyl HAVING COALESCE(mpg, 0) > 0",
            "SELECT ABS(cyl) FROM mtcars" + where + " GROUP BY cyl + 1")) {
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(sql), sql);
          assertEquals("42000", failure.getSQLState(), sql);
        }
      }
    }
  }

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
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
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
   * Binds distinct predicates in projections, filters, and sort keys.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void distinctPredicates(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertRows(connection,
          "SELECT cyl, CASE WHEN cyl IS DISTINCT FROM 4 THEN 'distinct' ELSE 'same' END "
              + "FROM mtcars GROUP BY cyl ORDER BY 1",
          List.of(List.of("4", "same"), List.of("6", "distinct"), List.of("8", "distinct")));
      assertRows(connection,
          "SELECT cyl FROM mtcars GROUP BY cyl " + "ORDER BY CASE WHEN cyl IS DISTINCT FROM 8 THEN 1 ELSE 0 END, cyl",
          List.of(List.of("8"), List.of("4"), List.of("6")));
      assertRows(connection,
          "SELECT cyl FROM mtcars GROUP BY cyl "
              + "HAVING CASE WHEN cyl IS DISTINCT FROM 4 THEN 1 ELSE 0 END = 1 ORDER BY cyl",
          List.of(List.of("6"), List.of("8")));
      assertRows(connection, "SELECT CASE WHEN MAX(cyl) IS DISTINCT FROM 8 THEN 1 ELSE 0 END FROM mtcars",
          List.of(List.of("0")));
      assertRows(connection, "SELECT CASE WHEN MAX(cyl) IS NOT DISTINCT FROM (SELECT 8) THEN 1 ELSE 0 END FROM mtcars",
          List.of(List.of("1")));
      for (String where : List.of("", " WHERE cyl < 0")) {
        for (String sql : List.of(
            "SELECT cyl, CASE WHEN mpg IS NOT DISTINCT FROM NULL THEN 'null' END FROM mtcars" + where + " GROUP BY cyl",
            "SELECT cyl FROM mtcars" + where + " GROUP BY cyl HAVING CASE WHEN mpg IS DISTINCT FROM 4 THEN 1 END = 1",
            "SELECT cyl FROM mtcars" + where + " GROUP BY cyl ORDER BY CASE WHEN mpg IS DISTINCT FROM 4 THEN 1 END")) {
          SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
          assertEquals("42000", failure.getSQLState(), sql);
        }
      }
    }
  }

  /**
   * Discovers aggregates and subqueries in SQL named string-function operands.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void namedStringOperands(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertRows(connection,
          "SELECT SUBSTRING(MAX(model) FROM 1 FOR 3), "
              + "TRIM(BOTH 'V' FROM MAX(model)), POSITION('o' IN MAX(model)) FROM mtcars",
          List.of(List.of("Vol", "olvo 142E", "2")));
      for (String expression : List.of("SUBSTRING(MAX(model) FROM 1 FOR 3)", "TRIM(BOTH 'V' FROM MAX(model))",
          "POSITION('o' IN MAX(model))")) {
        assertRows(connection, "SELECT cyl FROM mtcars GROUP BY cyl ORDER BY " + expression + ", cyl",
            namedOrder(connection, expression));
      }
      assertRows(connection, "SELECT SUBSTRING((SELECT MAX(model) FROM mtcars) FROM 1 FOR 3), COUNT(*) FROM mtcars",
          List.of(List.of("Vol", "32")));
      assertRows(connection, "SELECT TRIM(BOTH 'V' FROM (SELECT MAX(model) FROM mtcars)), COUNT(*) FROM mtcars",
          List.of(List.of("olvo 142E", "32")));
      for (String expression : List.of("SUBSTRING(MAX(model) OVER () FROM 1 FOR 3)",
          "TRIM(BOTH 'V' FROM MAX(model) OVER ())", "POSITION('o' IN MAX(model) OVER ())")) {
        SQLException failure = assertThrows(SQLException.class,
            () -> connection.prepareStatement("SELECT " + expression + " FROM mtcars GROUP BY cyl"));
        assertEquals("42000", failure.getSQLState());
      }
    }
  }

  /**
   * Reads expected ordering with the aggregate explicitly selected.
   *
   * @param connection
   *          fixture connection
   * @param expression
   *          sort expression
   * @return ordered cylinder rows
   * @throws SQLException
   *           if query execution fails
   */
  private static List<List<String>> namedOrder(Connection connection, String expression) throws SQLException {
    List<List<String>> rows = new ArrayList<>();
    try (var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT cyl, MAX(model) FROM mtcars GROUP BY cyl ORDER BY " + expression + ", cyl")) {
      while (result.next()) {
        rows.add(List.of(result.getString(1)));
      }
    }
    return rows;
  }

  /**
   * Uses shared arithmetic semantics for temporal values and invalid numeric
   * text.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void sharedArithmetic(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertRows(connection, "SELECT MAX(DATE '2024-03-01') - MIN(DATE '2024-02-01'), "
          + "MAX(DATE '2024-03-01') + INTERVAL '1 day' FROM mtcars", List.of(List.of("29", "2024-03-02")));
      assertRows(connection,
          "SELECT COUNT(*) FROM mtcars " + "HAVING MAX(DATE '2024-03-01') - MIN(DATE '2024-02-01') > 1",
          List.of(List.of("32")));
      assertRows(connection,
          "SELECT cyl FROM mtcars GROUP BY cyl " + "ORDER BY MAX(DATE '2024-03-01') - MIN(DATE '2024-02-01'), cyl",
          List.of(List.of("4"), List.of("6"), List.of("8")));
      for (String sql : List.of("SELECT -MAX(model) FROM mtcars", "SELECT COUNT(*) FROM mtcars HAVING -MAX(model) > 0",
          "SELECT cyl FROM mtcars GROUP BY cyl ORDER BY -MAX(model)")) {
        SQLException failure = assertThrows(SQLException.class, () -> assertRows(connection, sql, List.of()), sql);
        assertEquals("22018", failure.getSQLState(), sql);
      }
      for (String where : List.of("", " WHERE cyl < 0")) {
        SQLException failure = assertThrows(SQLException.class, () -> connection
            .prepareStatement("SELECT SUM(mpg) AS mpg FROM mtcars m" + where + " GROUP BY cyl HAVING m.mpg > 200"));
        assertEquals("42000", failure.getSQLState());
      }
      assertRows(connection, "SELECT m.cyl FROM mtcars m GROUP BY m.cyl HAVING m.cyl > 4 ORDER BY m.cyl",
          List.of(List.of("6"), List.of("8")));
      assertRows(connection, "SELECT ABS(cyl) FROM mtcars m GROUP BY ABS(cyl) HAVING ABS(m.cyl) > 4 ORDER BY ABS(cyl)",
          List.of(List.of("6"), List.of("8")));
      assertRows(connection, "SELECT SUM(mpg) AS mpg FROM mtcars m GROUP BY cyl HAVING mpg > 200 ORDER BY mpg",
          List.of(List.of("211.4"), List.of("293.3")));
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
