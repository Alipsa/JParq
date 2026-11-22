package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.CorrelatedBindings;

/**
 * Verifies that unaliased correlated {@code LATERAL} projections expose the
 * original expression text as their column label instead of private binding
 * placeholders, per issue 334.
 */
class LateralProjectionLabelTest {

  @TempDir
  Path directory;

  /**
   * Labels a qualified-wildcard LATERAL projection with its original expression.
   *
   * @param expression
   *          correlated projection without an alias
   * @param expected
   *          expected result value for the single row
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @MethodSource("correlatedExpressions")
  void qualifiedWildcardUsesOriginalExpressionLabel(String expression, String expected) throws SQLException {
    String sql = "SELECT x.* FROM (VALUES (1)) a(n) CROSS JOIN LATERAL (SELECT " + expression
        + " FROM (VALUES (1)) s(z)) x";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      String label = result.getMetaData().getColumnLabel(1);
      assertEquals(expression, label);
      assertFalse(label.contains(CorrelatedBindings.FUNCTION_NAME), label);
      assertTrue(result.next());
      assertEquals(expected, result.getObject(1));
    }
  }

  /**
   * Supplies correlated projection expressions with their expected values.
   *
   * @return stream of expression and expected-value pairs
   */
  static Stream<Arguments> correlatedExpressions() {
    return Stream.of(Arguments.of("CONCAT('z', a.n)", "z1"), Arguments.of("'z' || CAST(a.n AS VARCHAR)", "z1"));
  }

  /**
   * Labels an unqualified-star LATERAL projection with its original expression.
   *
   * @param expression
   *          correlated projection without an alias
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CONCAT('z', a.n)", "'z' || CAST(a.n AS VARCHAR)"
  })
  void unqualifiedStarUsesOriginalExpressionLabel(String expression) throws SQLException {
    String sql = "SELECT * FROM (VALUES (1)) a(n) CROSS JOIN LATERAL (SELECT " + expression
        + " FROM (VALUES (1)) s(z)) x";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      var metadata = result.getMetaData();
      assertEquals(expression, metadata.getColumnLabel(2));
      assertFalse(metadata.getColumnName(2).contains(CorrelatedBindings.FUNCTION_NAME));
      assertEquals(2, result.findColumn(expression));
      assertTrue(result.next());
      assertEquals("z1", result.getObject(2));
    }
  }

  /**
   * Keeps LATERAL projection labels stable across executions.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void labelsAreStableAcrossExecutions() throws SQLException {
    String sql = "SELECT x.* FROM (VALUES (1)) a(n) CROSS JOIN LATERAL (SELECT CONCAT('z', a.n) "
        + "FROM (VALUES (1)) s(z)) x";
    String first = null;
    for (int run = 0; run < 2; run++) {
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
          var statement = connection.createStatement();
          var result = statement.executeQuery(sql)) {
        assertTrue(result.next());
        String label = result.getMetaData().getColumnLabel(1);
        if (first == null) {
          first = label;
        } else {
          assertEquals(first, label);
        }
      }
    }
    assertEquals("CONCAT('z', a.n)", first);
  }

  /**
   * Preserves an explicit alias on a correlated LATERAL projection.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void explicitAliasIsPreserved() throws SQLException {
    String sql = "SELECT x.* FROM (VALUES (1)) a(n) CROSS JOIN LATERAL (SELECT CONCAT('z', a.n) AS prefixed "
        + "FROM (VALUES (1)) s(z)) x";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals("prefixed", result.getMetaData().getColumnLabel(1));
      assertTrue(result.next());
      assertEquals("z1", result.getObject(1));
    }
  }

  /**
   * Labels a correlated scalar subquery projection with its original expression.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void nestedScalarSubqueryUsesOriginalExpressionLabel() throws SQLException {
    String sql = "SELECT x.* FROM (VALUES (1)) a(n) CROSS JOIN LATERAL (SELECT "
        + "(SELECT a.n FROM (VALUES (1)) v(x)) FROM (VALUES (1)) s(z)) x";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      String label = result.getMetaData().getColumnLabel(1);
      assertEquals("(SELECT a.n FROM (VALUES (1)) v(x))", label);
      assertFalse(label.contains(CorrelatedBindings.FUNCTION_NAME), label);
      assertTrue(result.next());
      assertEquals(1, result.getObject(1));
    }
  }

  /**
   * Expands qualified wildcards over derived-table labels containing dots.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void qualifiedWildcardResolvesDottedDerivedLabels() throws SQLException {
    String sql = "SELECT x.* FROM (VALUES (1)) a(n) CROSS JOIN (SELECT a.n + 1, b.m + 10 "
        + "FROM (VALUES (1)) a(n) CROSS JOIN (VALUES (2)) b(m)) x";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      var metadata = result.getMetaData();
      assertEquals("a.n + 1", metadata.getColumnLabel(1));
      assertEquals("b.m + 10", metadata.getColumnLabel(2));
      assertTrue(result.next());
      assertEquals(2L, result.getObject(1));
      assertEquals(12L, result.getObject(2));
    }
  }
}
