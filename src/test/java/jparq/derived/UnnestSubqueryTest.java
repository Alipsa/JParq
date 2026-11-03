package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies scalar subqueries used as UNNEST arguments (issue 270). */
class UnnestSubqueryTest {

  /**
   * Evaluates standalone array elements with the enclosing CTE and bindings.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if preparation or execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void standaloneSubqueriesUseTheirExecutionContext(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String sql : List.of("SELECT * FROM UNNEST(ARRAY[(SELECT COUNT(*) FROM mtcars WHERE cyl = 4)]) AS t(x)",
          "WITH c AS (SELECT cyl FROM mtcars WHERE cyl = 4) "
              + "SELECT x FROM UNNEST(ARRAY[(SELECT COUNT(*) FROM c)]) AS t(x)",
          "SELECT x FROM TABLE(UNNEST(ARRAY[(SELECT COUNT(*) FROM mtcars WHERE cyl = 4)])) AS t(x)")) {
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          assertTrue(result.next(), sql);
          assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1), sql);
          assertEquals(11L, result.getObject(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      String sql = "SELECT x FROM UNNEST(ARRAY[(SELECT COUNT(*) FROM mtcars WHERE cyl = ?)]) AS t(x)";
      try (var statement = connection.prepareStatement(sql)) {
        for (int cylinders : List.of(4, 6, 8)) {
          statement.setInt(1, cylinders);
          try (var result = statement.executeQuery()) {
            assertTrue(result.next());
            assertEquals(cylinders == 4 ? 11L : cylinders == 6 ? 7L : 14L, result.getLong(1));
            assertFalse(result.next());
          }
        }
      }
    }
  }

  /**
   * Binds correlated subqueries for each left row in implicit and explicit
   * LATERAL table functions, preserving ordinality.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void joinedSubqueriesUseCurrentOuterRows(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String source : List.of("mtcars", "cars")) {
        String prefix = source.equals("cars") ? "WITH cars AS (SELECT cyl FROM mtcars) " : "";
        for (String join : List.of(",", "CROSS JOIN", "CROSS JOIN LATERAL")) {
          String sql = prefix + "SELECT v.cyl, t.x, t.n FROM (VALUES (4), (6), (8)) v(cyl) " + join
              + " UNNEST(ARRAY[(SELECT COUNT(*) FROM " + source + " m WHERE m.cyl = v.cyl), " + "(SELECT COUNT(*) FROM "
              + source + " WHERE cyl = 4)]) WITH ORDINALITY AS t(x, n) ORDER BY v.cyl, t.n";
          try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
            assertEquals(Types.BIGINT, result.getMetaData().getColumnType(2), sql);
            List<String> rows = new ArrayList<>();
            while (result.next()) {
              rows.add(result.getInt(1) + ":" + result.getLong(2) + ":" + result.getInt(3));
            }
            assertEquals(List.of("4:11:1", "4:11:2", "6:7:1", "6:11:2", "8:14:1", "8:11:2"), rows, sql);
          }
        }
      }
    }
  }

  /**
   * Distinguishes a NULL array from an array containing a NULL scalar result, and
   * permits UNNEST as the first input in a join.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void nullAndArraySubqueriesPreserveUnnestSemantics(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      String empty = "SELECT x FROM UNNEST((SELECT ARRAY[1, 2] WHERE FALSE)) AS t(x)";
      try (var statement = connection.prepareStatement(empty); var result = statement.executeQuery()) {
        assertFalse(result.next());
      }
      String nullElement = "SELECT x, n FROM UNNEST(ARRAY[(SELECT cyl FROM mtcars WHERE FALSE)]) "
          + "WITH ORDINALITY AS t(x, n)";
      try (var statement = connection.prepareStatement(nullElement); var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertNull(result.getObject(1));
        assertEquals(1, result.getInt(2));
        assertFalse(result.next());
      }
      String firstInput = "SELECT x FROM UNNEST((SELECT ARRAY[1, 2])) AS t(x) CROSS JOIN (VALUES (1)) v(k) ORDER BY x";
      try (var statement = connection.prepareStatement(firstInput); var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
        assertTrue(result.next());
        assertEquals(2, result.getInt(1));
        assertFalse(result.next());
      }
      String left = "SELECT v.cyl, t.x FROM (VALUES (4), (6), (8)) v(cyl) LEFT JOIN LATERAL "
          + "UNNEST(ARRAY[(SELECT COUNT(*) FROM mtcars m WHERE m.cyl = v.cyl)]) t(x) " + "ON t.x > 10 ORDER BY v.cyl";
      try (var statement = connection.prepareStatement(left); var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals(4, result.getInt(1));
        assertEquals(11L, result.getLong(2));
        assertTrue(result.next());
        assertEquals(6, result.getInt(1));
        assertNull(result.getObject(2));
        assertTrue(result.next());
        assertEquals(8, result.getInt(1));
        assertEquals(14L, result.getLong(2));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Retains scalar cardinality diagnostics for invalid arguments.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if JDBC setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void scalarCardinalityErrorsRetainSqlState(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String source : List.of("UNNEST(ARRAY[(SELECT cyl FROM mtcars)]) t(x)",
          "UNNEST(ARRAY[(SELECT cyl, mpg FROM mtcars WHERE cyl < 0)]) t(x)",
          "(VALUES (1)) v(k) CROSS JOIN LATERAL UNNEST(ARRAY[(SELECT cyl FROM mtcars)]) t(x)",
          "(VALUES (4)) v(cyl) CROSS JOIN LATERAL "
              + "UNNEST(ARRAY[(SELECT m.cyl FROM mtcars m WHERE m.cyl = v.cyl)]) t(x)")) {
        String sql = "SELECT x FROM " + source;
        SQLException error = assertThrows(SQLException.class, () -> {
          try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
            result.next();
          }
        }, sql);
        assertEquals(source.contains("cyl, mpg") ? "42000" : "21000", error.getSQLState(), sql);
        Class<? extends SQLException> expected = source.contains("cyl, mpg")
            ? SQLSyntaxErrorException.class
            : SQLDataException.class;
        assertInstanceOf(expected, error);
      }
    }
  }

  /**
   * Preserves numeric declarations when every array value is an outer column.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void outerOnlyArraysKeepNumericTypes(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      String max = "SELECT MAX(t.x) FROM mtcars m CROSS JOIN UNNEST(ARRAY[m.hp]) t(x)";
      try (var statement = connection.prepareStatement(max); var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertTrue(result.getObject(1) instanceof Number);
        assertEquals(335, result.getInt(1));
      }
      String order = "SELECT t.x FROM mtcars m CROSS JOIN UNNEST(ARRAY[m.hp]) t(x) ORDER BY t.x";
      try (var statement = connection.prepareStatement(order); var result = statement.executeQuery()) {
        int previous = Integer.MIN_VALUE;
        int count = 0;
        while (result.next()) {
          assertTrue(result.getObject(1) instanceof Number);
          int current = result.getInt(1);
          assertTrue(previous <= current, previous + " <= " + current);
          previous = current;
          count++;
        }
        assertEquals(32, count);
        assertEquals(335, previous);
      }
      for (String join : List.of("JOIN UNNEST(ARRAY[v.cyl]) t(cyl) USING (cyl)",
          "JOIN UNNEST(ARRAY[v.cyl, 5]) t(cyl) USING (cyl)")) {
        try (var statement = connection.prepareStatement("SELECT cyl FROM (VALUES (4)) v(cyl) " + join);
            var result = statement.executeQuery()) {
          assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
          assertTrue(result.next(), join);
          assertEquals(4, result.getObject(1), join);
          assertFalse(result.next(), join);
        }
      }
      String empty = "SELECT t.x FROM (SELECT hp FROM mtcars WHERE FALSE) m " + "CROSS JOIN UNNEST(ARRAY[m.hp]) t(x)";
      try (var statement = connection.prepareStatement(empty); var result = statement.executeQuery()) {
        assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Promotes declared outer values together with evaluated scalar results.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void scalarResultsPromoteDeclaredArrayElements(boolean pushdown) throws Exception {
    String sql = "SELECT t.x FROM (VALUES (4)) v(cyl) CROSS JOIN "
        + "UNNEST(ARRAY[v.cyl, (SELECT AVG(hp) FROM mtcars)]) WITH ORDINALITY t(x, n) ORDER BY t.n";
    try (var connection = DriverManager.getConnection(url(pushdown));
        var statement = connection.prepareStatement(sql);
        var result = statement.executeQuery()) {
      assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1));
      assertTrue(result.next());
      assertEquals(4.0, result.getObject(1));
      assertTrue(result.next());
      assertEquals(146.6875, result.getObject(1));
      assertFalse(result.next());
    }
  }

  /**
   * Resolves unqualified outer columns and diagnoses missing or ambiguous names.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void unqualifiedOuterColumnsAreBoundOrRejected(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String expression : List.of("cyl", "(SELECT cyl)", "COALESCE(cyl, cyl)", "NULLIF(cyl, 0)",
          "(SELECT NULLIF(cyl, 0))", "cyl + 0", "(SELECT cyl FROM (VALUES (5)) inner_values(cyl))")) {
        String sql = "SELECT x FROM (VALUES (4)) v(cyl) CROSS JOIN UNNEST(ARRAY[" + expression + "]) t(x)";
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          assertTrue(result.next(), sql);
          assertEquals(expression.contains("inner_values") ? 5 : 4, result.getInt(1), sql);
          assertTrue(result.getObject(1) instanceof Number, sql);
          assertFalse(result.next(), sql);
        }
      }
      String sameName = "SELECT t.x FROM (VALUES (4)) v(x) CROSS JOIN UNNEST(ARRAY[x]) t(x)";
      try (var statement = connection.prepareStatement(sameName); var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals(4, result.getObject(1));
        assertFalse(result.next());
      }
      for (String sql : List.of(
          "SELECT x FROM (VALUES (4)) v(cyl), (VALUES (5)) w(cyl) " + "CROSS JOIN UNNEST(ARRAY[cyl]) t(x)",
          "SELECT x FROM (VALUES (4)) v(cyl) CROSS JOIN UNNEST(ARRAY[nosuch]) t(x)",
          "SELECT x FROM UNNEST(ARRAY[nosuch]) t(x)")) {
        SQLException error = assertThrows(SQLException.class, () -> {
          try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
            result.next();
          }
        }, sql);
        assertEquals(sql.contains("w(cyl)") ? "42702" : "42000", error.getSQLState(), sql);
      }
    }
  }

  /**
   * Keeps invalid alias diagnostics classified and free of generated SQL.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void invalidAliasesDescribeOriginalUnnest(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      String sql = "SELECT x FROM (VALUES (4)) v(cyl) CROSS JOIN UNNEST(ARRAY[v.cyl]) t(x, y)";
      SQLException error = assertThrows(SQLException.class, () -> {
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          result.next();
        }
      });
      assertEquals("42000", error.getSQLState());
      assertInstanceOf(SQLSyntaxErrorException.class, error);
      assertTrue(error.getMessage().contains("UNNEST(ARRAY[v.cyl])"), error.getMessage());
      assertFalse(error.getMessage().contains("__JPARQ_CORRELATED_VALUE"), error.getMessage());
      assertFalse(error.getMessage().contains("SELECT * FROM"), error.getMessage());
      assertFalse(error.getMessage().contains("Failed to prepare query:"), error.getMessage());
      assertEquals(error.getMessage().indexOf("UNNEST(ARRAY[v.cyl])"),
          error.getMessage().lastIndexOf("UNNEST(ARRAY[v.cyl])"), error.getMessage());
    }
  }

  /**
   * Describes an unresolved joined argument once, retaining its column
   * diagnostic.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void joinedUnnestErrorsDescribeArgumentOnce(boolean pushdown) throws Exception {
    String sql = "SELECT t.x FROM (VALUES (4)) v(cyl) CROSS JOIN UNNEST(ARRAY[nosuch]) t(x)";
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      var error = assertThrows(SQLSyntaxErrorException.class, () -> {
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          result.next();
        }
      });
      String message = error.getMessage();
      String argument = "UNNEST(ARRAY[nosuch])";
      assertEquals("42000", error.getSQLState());
      assertTrue(message.contains("Failed to evaluate " + argument), message);
      assertTrue(message.contains("Column not found in UNNEST argument: nosuch"), message);
      assertFalse(message.contains("Failed to prepare query:"), message);
      assertEquals(message.indexOf(argument), message.lastIndexOf(argument), message);
    }
  }

  /**
   * NULLIF retains its first argument's type even when every result is NULL.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void nullifRetainsDeclaredTypeForNullResults(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String argument : List.of("NULLIF(v.cyl, 4)", "(SELECT NULLIF(v.cyl, 4))")) {
        String sql = "SELECT t.x FROM (VALUES (4)) v(cyl) CROSS JOIN UNNEST(ARRAY[" + argument + "]) t(x)";
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1), sql);
          assertTrue(result.next(), sql);
          assertNull(result.getObject(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Uses ARRAY diagnostics with SQL type names during NULL-bound discovery.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void incompatibleDeclarationsNameArrayElementTypes(boolean pushdown) throws Exception {
    String sql = "SELECT t.x FROM mtcars m CROSS JOIN UNNEST(ARRAY[m.hp, 'text']) t(x)";
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      var error = assertThrows(SQLSyntaxErrorException.class, () -> {
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          result.next();
        }
      });
      assertEquals("42000", error.getSQLState());
      assertTrue(error.getMessage().contains("ARRAY elements must share a common type; found INTEGER and VARCHAR"),
          error.getMessage());
    }
  }

  /**
   * Builds a JDBC URL for the shared Parquet fixture directory.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @return JDBC connection URL
   */
  private static String url(boolean pushdown) {
    return "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown=" + pushdown
        + "&projectionPushdown=" + pushdown;
  }
}
