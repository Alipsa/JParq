package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
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
        SQLSyntaxErrorException error = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
            result.next();
          }
        }, sql);
        assertEquals("42000", error.getSQLState(), sql);
      }
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
