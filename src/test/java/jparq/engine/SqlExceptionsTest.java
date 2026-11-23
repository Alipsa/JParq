package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import jparq.support.QueryTestSupport;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.SqlParser;

/**
 * Verifies SQLState classification without losing nested diagnostic context.
 */
class SqlExceptionsTest {
  @TempDir
  Path directory;

  /**
   * Preserves implementation-limit classification and context across wrappers.
   */
  @Test
  void implementationLimitsPreserveSqlState() {
    var cause = new IllegalStateException("work limit");
    var failure = SqlExceptions.implementationLimit("resource bound", cause);
    var classified = SqlExceptions.findSqlStateFailure(failure);
    assertEquals("54000", classified.getSQLState());
    assertSame(cause, classified.getCause());
    var wrapped = SqlExceptions.wrap("query context", new IllegalStateException("evaluation context", failure));
    assertEquals(SQLException.class, wrapped.getClass());
    assertEquals("54000", wrapped.getSQLState());
    assertTrue(wrapped.getMessage().contains("query context"));
    assertTrue(wrapped.getMessage().contains("resource bound"));
    assertEquals("54000",
        SqlExceptions.findSqlStateFailure(SqlExceptions.implementationLimit("work bound")).getSQLState());
  }

  /**
   * Preserves invalid-attribute classification separately from exhausted limits.
   */
  @Test
  void invalidAttributesPreserveSqlState() {
    var cause = new NumberFormatException("invalid setting");
    var failure = SqlExceptions.invalidAttribute("configuration value", cause);
    var classified = SqlExceptions.findSqlStateFailure(failure);
    assertEquals("HY024", classified.getSQLState());
    assertSame(cause, classified.getCause());
    var wrapped = SqlExceptions.wrap("query context", new IllegalStateException("evaluation context", failure));
    assertEquals(SQLException.class, wrapped.getClass());
    assertEquals("HY024", wrapped.getSQLState());
    assertTrue(wrapped.getMessage().contains("configuration value"));
    assertEquals("HY024",
        SqlExceptions.findSqlStateFailure(SqlExceptions.invalidAttribute("invalid value")).getSQLState());
  }

  /** Preserves unsupported-feature classification and the original cause. */
  @Test
  void unsupportedFeatureFactoryPreservesCause() {
    var plain = SqlExceptions.unsupportedFeature("Unsupported feature");
    var classified = assertInstanceOf(SQLFeatureNotSupportedException.class, plain.getCause());
    assertEquals("0A000", classified.getSQLState());
    assertEquals("Unsupported feature", classified.getMessage());
    assertNull(classified.getCause());

    var original = new UnsupportedOperationException("Original failure");
    var wrapped = SqlExceptions.unsupportedFeature("Unsupported feature", original);
    var failure = assertInstanceOf(SQLFeatureNotSupportedException.class, wrapped.getCause());
    assertEquals("0A000", failure.getSQLState());
    assertSame(original, failure.getCause());
  }

  /**
   * Redacts every binding inside nested SQL while retaining ordinary expression
   * text.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void describesExpressionsWithoutPrivateBindings() throws Exception {
    String sql = "-(SELECT __JPARQ_CORRELATED_VALUE('private-key', 0) "
        + "+ __JPARQ_CORRELATED_VALUE('another-key', 12))";
    var expression = CCJSqlParserUtil.parseExpression(sql);
    assertEquals("-(SELECT " + SqlExceptions.OUTER_VALUE + " + " + SqlExceptions.OUTER_VALUE + ")",
        SqlExceptions.describe(expression));
    assertTrue(expression.toString().contains("private-key"));
    var ordinary = CCJSqlParserUtil.parseExpression("cyl & 2");
    assertEquals(ordinary.toString(), SqlExceptions.describe(ordinary));
  }

  /** Checks rendered labels use the same redaction as parsed expressions. */
  @Test
  void describesTextWithoutPrivateBindings() {
    String text = "CONCAT('z', __jparq_correlated_value ( 'private-key' , 0 ), "
        + "__JPARQ_CORRELATED_VALUE('another-key', 12))";
    assertEquals("CONCAT('z', " + SqlExceptions.OUTER_VALUE + ", " + SqlExceptions.OUTER_VALUE + ")",
        SqlExceptions.describe(text));
    assertEquals("price", SqlExceptions.describe("price"));
    assertEquals("null", SqlExceptions.describe((String) null));
  }

  /**
   * Uses one predicate diagnostic contract and redacts private operand bindings.
   *
   * @throws Exception
   *           if expression parsing fails
   */
  @Test
  void unsupportedPredicateFactoryClassifiesAndRedacts() throws Exception {
    var ordinary = SqlExceptions.unsupportedPredicate(CCJSqlParserUtil.parseExpression("cyl"));
    assertEquals("Unsupported predicate: cyl", ordinary.getMessage());
    assertEquals("42000", SqlExceptions.findSqlStateFailure(ordinary).getSQLState());
    var bound = SqlExceptions
        .unsupportedPredicate(CCJSqlParserUtil.parseExpression("__JPARQ_CORRELATED_VALUE('private-key', 0)"));
    assertEquals("Unsupported predicate: " + SqlExceptions.OUTER_VALUE, bound.getMessage());
    assertInstanceOf(SQLSyntaxErrorException.class, bound.getCause());
    assertEquals("42000", SqlExceptions.findSqlStateFailure(bound).getSQLState());
  }

  /**
   * Documents the JDBC syntax error contract, including preparation context.
   * 
   * @throws Exception
   *           if opening the connection fails
   */
  @Test
  void malformedSqlRetainsPreparationContext() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement("SELEC 1"));
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains("Failed to prepare query: SELEC 1"));
      assertTrue(failure.getMessage().contains("Failed to parse SQL: SELEC 1"));
    }
  }

  /**
   * Routes a classified failure that is not an ORDER BY position, such as the
   * scalar subquery cardinality check, through the execution boundary with the
   * execution context instead of the read-failure context.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void executionBoundaryClassifiesNonOrderFailures(boolean prepared) throws Exception {
    QueryTestSupport.writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String sql : List.of("SELECT n FROM o WHERE n = (SELECT n FROM o)",
          "SELECT * FROM o WHERE n = (SELECT n FROM o) ORDER BY n")) {
        SQLException failure = assertThrows(SQLDataException.class,
            () -> QueryTestSupport.rows(connection, sql, prepared), sql);
        assertEquals("21000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("Failed to execute query"), failure.getMessage());
        assertFalse(failure.getMessage().contains("Failed reading first parquet record"), failure.getMessage());
      }
    }
  }

  /**
   * Preserves every engine wrapper when a classified error crosses a JDBC
   * boundary.
   * 
   * @param state
   *          the syntax or data SQLState
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "42000", "42702", "42803", "22003", "22007", "22008", "22018", "22021", "2201W", "2201X"
  })
  void wrappingRetainsSubqueryContext(String state) {
    SQLException classified = new SQLException("Inner query failed", state, 7);
    var outer = new SQLException("Failed to execute subquery for join: SELECT broken", classified);
    var failure = SqlExceptions.wrap("Failed to prepare query: SELECT outer_query", outer);
    assertEquals(state, failure.getSQLState());
    assertEquals(7, failure.getErrorCode());
    assertTrue(failure.getMessage().contains("Failed to prepare query: SELECT outer_query"));
    assertTrue(failure.getMessage().contains("Failed to execute subquery for join: SELECT broken"));
    assertSame(outer, failure.getCause());
    assertSame(classified, failure.getCause().getCause());
    assertSame(failure, SqlExceptions.wrap(failure.getMessage(), failure));
    if (state.startsWith("42")) {
      assertInstanceOf(SQLSyntaxErrorException.class, failure);
    } else {
      assertInstanceOf(SQLDataException.class, failure);
    }
  }

  /**
   * Preserves a classified exception's nonzero vendor code and original cause.
   */
  @Test
  void wrappingRetainsVendorErrorCode() {
    var original = new SQLDataException("Numeric conversion failed", "22003", 1234);
    var failure = SqlExceptions.wrap("Failed to execute derived table 'l'", original);
    assertInstanceOf(SQLDataException.class, failure);
    assertEquals("22003", failure.getSQLState());
    assertEquals(1234, failure.getErrorCode());
    assertSame(original, failure.getCause());
  }

  /**
   * Keeps subclasses, vendor codes, and null messages when diagnostics are
   * rendered.
   */
  @Test
  void renderedDiagnosticsKeepClassification() {
    for (String state : List.of("21000", "42000")) {
      var original = new SQLException(null, state, 42);
      var failure = SqlExceptions.wrap("UNNEST argument failed", original, SqlExceptions::describe);
      assertEquals(state, failure.getSQLState());
      assertEquals(42, failure.getErrorCode());
      assertSame(original, failure.getCause());
      Class<? extends SQLException> expected = "21000".equals(state)
          ? SQLDataException.class
          : SQLSyntaxErrorException.class;
      assertInstanceOf(expected, failure);
      assertEquals("UNNEST argument failed", failure.getMessage());
    }
    String sql = "SELECT * FROM UNNEST(__JPARQ_CORRELATED_VALUE('private', 0))";
    var original = new SQLException("Failed to prepare query: " + sql + ": Column not found: missing", "42000");
    var failure = SqlExceptions.wrap("Failed to evaluate UNNEST(a.values)", original,
        text -> SqlExceptions.describeSubqueryFailure(text, sql, "UNNEST(a.values)"));
    assertEquals("Failed to evaluate UNNEST(a.values): Column not found: missing", failure.getMessage());
    assertEquals("null", SqlExceptions.describeSubqueryFailure(null, sql, "UNNEST(a.values)"));
    assertSame(original, failure.getCause());
    // The same contract holds when the embedding source redacted the SQL
    // before the wrapper saw it.
    String redactedSql = SqlExceptions.describe(sql);
    var redacted = new SQLException("Failed to prepare query: " + redactedSql + ": Column not found: missing", "42000");
    var rendered = SqlExceptions.wrap("Failed to evaluate UNNEST(a.values)", redacted,
        text -> SqlExceptions.describeSubqueryFailure(text, sql, "UNNEST(a.values)"));
    assertEquals("Failed to evaluate UNNEST(a.values): Column not found: missing", rendered.getMessage());
    assertSame(redacted, rendered.getCause());
  }

  /**
   * A deeper failure carrying the inner subquery keeps that subquery's text; the
   * inner query is never rewritten to the outer UNNEST's expression. The inner
   * subquery is a strict substring of the generated wrapper, so it has a
   * different length and can never be mistaken for the wrapper, which the
   * renderer only replaces as a whole. The fixture carries the wrapper's own
   * preparation prefix so the stripping is exercised (issue #329).
   */
  @Test
  void deeperSubqueryFailureRetainsItsOwnQueryText() {
    String wrapper = "SELECT * FROM UNNEST((SELECT o.id FROM orders o WHERE o.id = __JPARQ_CORRELATED_VALUE('k', 0)))"
        + " AS u (id)";
    String redactedWrapper = SqlExceptions.describe(wrapper);
    String inner = "SELECT o.id FROM orders o WHERE o.id = " + SqlExceptions.OUTER_VALUE;
    assertTrue(redactedWrapper.contains(inner) && !redactedWrapper.equals(inner),
        "fixture must make the inner a strict substring of the wrapper");
    String detail = "Failed to prepare query: " + redactedWrapper + ": Failed to execute sub query: " + inner
        + ": Column not found: o.id";
    var original = new SQLException(detail, "42000");
    String expression = "UNNEST((SELECT o.id FROM orders o WHERE o.id = " + SqlExceptions.OUTER_VALUE + "))";
    var failure = SqlExceptions.wrap("Failed to evaluate " + expression, original,
        text -> SqlExceptions.describeSubqueryFailure(text, wrapper, expression));
    assertEquals(
        "Failed to evaluate " + expression + ": Failed to execute sub query: " + inner + ": Column not found: o.id",
        failure.getMessage());
    assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"));
  }

  /**
   * Redaction can make two generated wrappers textually identical when they share
   * a shape and differ only in the private binding key or slot. The deeper
   * wrapper's own preparation framing is then treated as redundant and stripped
   * too, but the user's expression and the real detail are retained, so no deeper
   * query is misattributed to the outer expression (issue #329).
   */
  @Test
  void redactionCollisionBetweenSameShapeWrappersIsBenign() {
    String outer = "SELECT * FROM UNNEST((SELECT o.id FROM orders o WHERE o.id = __JPARQ_CORRELATED_VALUE('k1', 0)))"
        + " AS u (id)";
    String inner = "SELECT * FROM UNNEST((SELECT o.id FROM orders o WHERE o.id = __JPARQ_CORRELATED_VALUE('k2', 1)))"
        + " AS u (id)";
    assertEquals(SqlExceptions.describe(outer), SqlExceptions.describe(inner),
        "fixture must make the two wrappers collide after redaction");
    String expression = "UNNEST((SELECT o.id FROM orders o WHERE o.id = " + SqlExceptions.OUTER_VALUE + "))";
    String detail = "Failed to prepare query: " + SqlExceptions.describe(inner) + ": Column not found: o.id";
    var original = new SQLException(detail, "42000");
    var failure = SqlExceptions.wrap("Failed to evaluate " + expression, original,
        text -> SqlExceptions.describeSubqueryFailure(text, outer, expression));
    assertEquals("Failed to evaluate " + expression + ": Column not found: o.id", failure.getMessage());
    assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"));
  }

  /**
   * Retains unclassified failure details in joined and LATERAL derived tables.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void derivedTableFailuresRetainUnclassifiedDetails(boolean pushdown) throws Exception {
    Path fixtures = Path.of(SqlExceptionsTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + fixtures + "?predicatePushdown=" + pushdown);
        var statement = connection.createStatement()) {
      for (String sql : List.of(
          "SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT a.mpg FROM nosuchtable a WHERE a.cyl = o.cyl) x",
          "SELECT * FROM mtcars o JOIN (SELECT * FROM nosuchtable) x ON TRUE")) {
        var failure = assertThrows(SQLException.class, () -> statement.executeQuery(sql), sql);
        String context = "Failed to execute derived table 'x': ";
        assertTrue(failure.getMessage().contains(context + "Failed to prepare query:"), failure::getMessage);
        assertTrue(failure.getMessage().contains("nosuchtable"), failure::getMessage);
        var cause = assertInstanceOf(SQLException.class, failure.getCause());
        assertNull(SqlExceptions.findSqlStateFailure(cause));
        assertEquals(cause.getSQLState(), failure.getSQLState());
        assertEquals(cause.getMessage(), failure.getMessage().substring(context.length()));
      }
    }
  }

  /**
   * A top-level (non-join) derived table over a missing table surfaces the table
   * lookup failure in the top-level message rather than only the cause chain
   * (issue #330).
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void topLevelDerivedTableMissingTableRetainsDetail(boolean pushdown) throws Exception {
    Path fixtures = Path.of(SqlExceptionsTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + fixtures + "?predicatePushdown=" + pushdown);
        var statement = connection.createStatement()) {
      var failure = assertThrows(SQLException.class,
          () -> statement.executeQuery("SELECT * FROM (SELECT * FROM nosuchtable) x"));
      // The table lookup detail is no longer hidden in the cause chain: it is
      // appended to the top-level message (issue #330).
      assertEquals("Failed to prepare query: SELECT * FROM (SELECT * FROM nosuchtable) x: "
          + "Failed to execute derived table 'x': Failed to prepare query: SELECT * FROM nosuchtable: "
          + "Table not found: PUBLIC.nosuchtable", failure.getMessage());
      // The missing-table failure is not yet classified (it should be 42S02/42000,
      // tracked by issue #594); pin the current null state so that fix is visible.
      assertNull(failure.getSQLState(), failure::getMessage);
      var cause = assertInstanceOf(SQLException.class, failure.getCause());
      assertEquals("Failed to execute derived table 'x': Failed to prepare query: SELECT * FROM nosuchtable: "
          + "Table not found: PUBLIC.nosuchtable", cause.getMessage());
    }
  }

  /**
   * A scalar subquery and an EXISTS predicate over a missing table both surface
   * the table lookup failure in the top-level message, even though the direct
   * failure is a stateless runtime wrapper that carries only the subquery context
   * (issue #330).
   *
   * @param sql
   *          the failing query with a nested reference to a missing table
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT (SELECT n.cyl FROM nosuchtable n WHERE n.cyl = m.cyl) FROM mtcars m",
      "SELECT * FROM mtcars m WHERE EXISTS (SELECT 1 FROM nosuchtable n WHERE n.cyl = m.cyl)"
  })
  void nestedSubqueryMissingTableRetainsDetail(String sql) throws Exception {
    Path fixtures = Path.of(SqlExceptionsTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + fixtures);
        var statement = connection.createStatement()) {
      var failure = assertThrows(SQLException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      }, sql);
      assertTrue(failure.getMessage().contains("Failed to execute sub query:"), failure::getMessage);
      assertTrue(failure.getMessage().contains("Table not found: PUBLIC.nosuchtable"), failure::getMessage);
      // The wrapper in between is not an SQLException; the reason still surfaces
      // through the innermost SQLException.
      assertInstanceOf(IllegalStateException.class, failure.getCause(), sql);
    }
  }

  /**
   * The top-level preparation path renders its own context without redacting, so
   * an unclassified correlated failure whose nested diagnostics carry a private
   * binding must rely on the nested sites redacting it; this pins that no raw
   * binding reaches the top-level message (issue #330). The two cases wrapped in
   * a derived table fail in top-level preparation, so their top-level failure
   * starts with {@code Failed to prepare query: <sql>}, which is exactly the path
   * that wraps without redacting; the test asserts that prefix so a routing
   * change cannot silently drop the preparation-path coverage.
   *
   * @param sql
   *          a correlated query with an unclassified nested failure
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @param topLevelPreparation
   *          whether the query fails in top-level preparation
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @MethodSource("unclassifiedCorrelatedLeakCases")
  void correlatedUnclassifiedFailuresDoNotLeakBindingsOnTopLevel(String sql, boolean pushdown,
      boolean topLevelPreparation) throws Exception {
    Path fixtures = Path.of(SqlExceptionsTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    String url = "jdbc:jparq:" + fixtures + "?predicatePushdown=" + pushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      var failure = assertThrows(SQLException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      }, sql);
      if (topLevelPreparation) {
        assertTrue(failure.getMessage().startsWith("Failed to prepare query: " + sql), failure::getMessage);
      }
      assertNoPrivateBindings(failure);
    }
  }

  /**
   * The correlated queries with unclassified nested failures used by
   * {@code correlatedUnclassifiedFailuresDoNotLeakBindingsOnTopLevel}, in both
   * pushdown modes, flagging which fail in top-level preparation.
   *
   * @return the (query, pushdown, topLevelPreparation) test cases
   */
  static Stream<Arguments> unclassifiedCorrelatedLeakCases() {
    return Stream.concat(
        leakCases(List.of("SELECT (SELECT n.cyl FROM nosuchtable n WHERE n.cyl = m.cyl) FROM mtcars m",
            "SELECT m.cyl FROM mtcars m WHERE m.cyl IN (SELECT n.cyl FROM nosuchtable n WHERE n.cyl = m.cyl)",
            "SELECT m.cyl FROM mtcars m, LATERAL (SELECT n.cyl FROM nosuchtable n WHERE n.cyl = m.cyl) l"), false),
        leakCases(List.of(
            "SELECT * FROM (SELECT (SELECT n.cyl FROM nosuchtable n WHERE n.cyl = m.cyl) c FROM mtcars m) x",
            "SELECT * FROM (SELECT m.cyl FROM mtcars m WHERE EXISTS (SELECT 1 FROM nosuchtable n WHERE n.cyl = m.cyl)) x"),
            true));
  }

  /**
   * Pairs each correlated query with both pushdown modes.
   *
   * @param queries
   *          the correlated queries with unclassified nested failures
   * @param topLevelPreparation
   *          whether these queries fail in top-level preparation
   * @return the (query, pushdown, topLevelPreparation) test cases
   */
  private static Stream<Arguments> leakCases(List<String> queries, boolean topLevelPreparation) {
    return queries.stream().flatMap(query -> Stream.of(Arguments.of(query, true, topLevelPreparation),
        Arguments.of(query, false, topLevelPreparation)));
  }

  /**
   * Preserves classified failures and derived-table context in LATERAL queries.
   *
   * @param predicate
   *          the failing inner predicate
   * @param state
   *          the expected SQLState
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "a.mpg > CAST(123.45 AS DECIMAL(2,1)) | 22003", "a.mpg > CAST('NaN' AS DOUBLE) | 22018", "NOT a.model | 42000"
  })
  void lateralFailuresRetainClassification(String predicate, String state) throws Exception {
    Path fixtures = Path.of(SqlExceptionsTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    for (boolean pushdown : List.of(true, false)) {
      String url = "jdbc:jparq:" + fixtures + "?predicatePushdown=" + pushdown;
      try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
        for (String condition : List.of("a.cyl = o.cyl AND " + predicate,
            "o.cyl IS NOT NULL AND a.cyl = o.cyl AND " + predicate)) {
          String sql = "SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT a.mpg FROM mtcars a WHERE " + condition
              + ") l";
          var failure = assertThrows(SQLException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, sql);
          assertEquals(state, failure.getSQLState(), sql);
          if ("42000".equals(state)) {
            assertInstanceOf(SQLSyntaxErrorException.class, failure, sql);
          } else {
            assertInstanceOf(SQLDataException.class, failure, sql);
          }
          String context = "Failed to execute derived table 'l'";
          assertTrue(failure.getMessage().contains(context), failure::getMessage);
          assertEquals(failure.getMessage().indexOf(context), failure.getMessage().lastIndexOf(context),
              failure::getMessage);
          var cause = SqlExceptions.findSqlStateFailure(failure.getCause());
          assertNotNull(cause);
          assertEquals(state, cause.getSQLState());
        }
      }
    }
  }

  /**
   * Cases where a correlated LATERAL subquery fails, mapped to the expected
   * top-level SQLState. A null state pins a known top-level classification gap to
   * the issue that tracks it, so fixing the gap fails the test until the pinned
   * state is updated.
   *
   * @return failing correlated LATERAL queries with expected states
   */
  static Stream<Arguments> correlatedLateralFailures() {
    return Stream.of(
        // preparation fails on a missing column of the inner scope
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT a.nosuchcol FROM mtcars a WHERE a.cyl"
            + " = o.cyl) l", "42000"),
        // a projection reading an outer value fails on the first outer row
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT CAST(o.model AS INTEGER) AS v FROM"
            + " mtcars a WHERE a.cyl = o.cyl) l", "22018"),
        // a nested subquery carrying the outer binding fails while iterating
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT a.mpg FROM mtcars a WHERE a.mpg IN"
            + " (SELECT CAST(o.model AS INTEGER))) l", "22018"),
        // a predicate subquery projecting several outer columns fails at preparation
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT a.mpg FROM mtcars a WHERE a.cyl IN"
            + " (SELECT o.cyl, o.mpg)) l", "42000"),
        // a nested aggregate over an outer value fails at preparation
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT SUM(SUM(o.cyl)) AS s FROM mtcars a"
            + " WHERE a.cyl = o.cyl) l", "42000"),
        // an ARRAY subquery projecting several outer columns fails at evaluation;
        // the missing top-level classification is issue #389
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT ARRAY(SELECT o.cyl, o.mpg) AS x FROM"
            + " mtcars a WHERE a.cyl = o.cyl) l", null),
        // an NTILE bucket reading an outer value below zero fails at evaluation;
        // the missing top-level classification is issue #583
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT NTILE(o.cyl - 100) OVER (ORDER BY"
            + " a.mpg) AS n FROM mtcars a WHERE a.cyl = o.cyl) l", null),
        // an NTH_VALUE position reading an outer value below zero fails at evaluation;
        // the missing top-level classification is issue #586
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT NTH_VALUE(a.mpg, o.cyl - 100) OVER"
            + " (ORDER BY a.mpg) AS n FROM mtcars a WHERE a.cyl = o.cyl) l", null),
        // a LAG offset reading an outer value below zero fails at evaluation;
        // the missing top-level classification is issue #590
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT LAG(a.mpg, o.cyl - 100) OVER (ORDER BY"
            + " a.mpg) AS n FROM mtcars a WHERE a.cyl = o.cyl) l", null),
        // a GROUPING argument reading an outer value fails at preparation
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT a.cyl, GROUPING(o.cyl) AS g FROM"
            + " mtcars a GROUP BY a.cyl) l", "42000"),
        // a GROUPING argument in HAVING reading an outer value fails at evaluation
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT a.cyl, COUNT(*) AS c FROM mtcars a"
            + " GROUP BY ROLLUP(a.cyl) HAVING GROUPING(o.cyl) = 0) l", "42000"),
        // an ungrouped SELECT expression combining an outer value fails at preparation
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT a.mpg + o.cyl AS z FROM mtcars a GROUP"
            + " BY a.cyl) l", "42803"),
        // an EXISTS subquery whose table-less LATERAL body references a preceding
        // FROM item fails while preparing the rewritten subquery; the missing
        // top-level classification is issue #416
        Arguments.of("SELECT o.cyl FROM mtcars o WHERE EXISTS (SELECT 1 FROM mtcars y CROSS JOIN LATERAL (SELECT"
            + " y.cyl AS k WHERE y.cyl = o.cyl) l WHERE l.k = o.cyl)", null),
        // a ROWS frame offset reading an outer value fails at evaluation;
        // the missing top-level classification is issue #591
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT COUNT(*) OVER (ORDER BY a.mpg ROWS"
            + " BETWEEN o.cyl - 100 PRECEDING AND CURRENT ROW) AS c FROM mtcars a) l", null),
        // a RANGE frame offset reading an outer value fails at evaluation;
        // the missing top-level classification is issue #591
        Arguments.of("SELECT o.cyl FROM mtcars o CROSS JOIN LATERAL (SELECT SUM(a.mpg) OVER (ORDER BY a.mpg RANGE"
            + " BETWEEN o.cyl PRECEDING AND CURRENT ROW) AS c FROM mtcars a) l", null));
  }

  /**
   * Correlated LATERAL failures never expose the private
   * {@code __JPARQ_CORRELATED_VALUE} binding token or its UUID in any link of the
   * failure chain (causes, suppressed exceptions, and next-exception links);
   * bindings render as the neutral outer-value marker somewhere in the reported
   * chain instead (issue #329).
   *
   * @param sql
   *          failing correlated LATERAL query
   * @param state
   *          expected top-level SQLState, or null pinning a known classification
   *          gap (issue #389 for the ARRAY case, issue #416 for the
   *          EXISTS-with-LATERAL case, issue #583 for the NTILE case, issue #586
   *          for the NTH_VALUE case, issue #590 for the LAG case, issue #591 for
   *          the ROWS and RANGE frame offset cases)
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @MethodSource("correlatedLateralFailures")
  void correlatedLateralFailuresRedactBindings(String sql, String state) throws Exception {
    Path fixtures = Path.of(SqlExceptionsTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    for (boolean pushdown : List.of(true, false)) {
      String url = "jdbc:jparq:" + fixtures + "?predicatePushdown=" + pushdown;
      try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
        SQLException failure = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals(state, failure.getSQLState(), sql + ": " + failure);
        List<String> chain = failureChainMessages(failure);
        assertTrue(chain.stream().anyMatch(message -> message.contains(SqlExceptions.OUTER_VALUE)),
            () -> "outer-value marker missing from chain: " + failure);
        assertNoPrivateBindings(failure);
      }
    }
  }

  /**
   * Checks every link of a failure chain for the private correlated binding
   * token.
   *
   * @param failure
   *          failure whose chain is inspected
   */
  private static void assertNoPrivateBindings(SQLException failure) {
    for (String message : failureChainMessages(failure)) {
      assertFalse(message.contains("__JPARQ_CORRELATED_VALUE"), () -> "private binding leaked: " + message);
    }
  }

  /**
   * Collects the messages of every link in a failure chain, following causes,
   * suppressed exceptions, and {@code SQLException} next-exception links.
   *
   * @param failure
   *          failure whose chain is inspected
   * @return every link message, starting with the top-level failure
   */
  private static List<String> failureChainMessages(SQLException failure) {
    List<String> messages = new ArrayList<>();
    collectFailureMessages(failure, messages, Collections.newSetFromMap(new IdentityHashMap<>()));
    return messages;
  }

  /**
   * Appends the messages of a failure and all of its related links, visiting each
   * link at most once so cyclic links cannot recurse.
   *
   * @param failure
   *          failure whose links are collected
   * @param messages
   *          collector receiving each link message
   * @param visited
   *          links already collected, preventing cyclic recursion
   */
  private static void collectFailureMessages(Throwable failure, List<String> messages, Set<Throwable> visited) {
    if (!visited.add(failure)) {
      return;
    }
    messages.add(String.valueOf(failure.getMessage()));
    Throwable cause = failure.getCause();
    if (cause != null) {
      collectFailureMessages(cause, messages, visited);
    }
    for (Throwable suppressed : failure.getSuppressed()) {
      collectFailureMessages(suppressed, messages, visited);
    }
    // A single step follows the rest of the chain through recursion, and the
    // visited set stops a cyclic next-exception chain.
    if (failure instanceof SQLException sqlException && sqlException.getNextException() != null) {
      collectFailureMessages(sqlException.getNextException(), messages, visited);
    }
  }

  /**
   * Retains nested-subquery context when iteration fails after returning a row.
   * 
   * @param state
   *          the syntax or data SQLState
   * @throws Exception
   *           if fixture construction fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "42000", "42702", "42803", "22003", "22007", "22008", "22018", "22021", "2201W", "2201X"
  })
  void iterationRetainsSubqueryContext(String state) throws Exception {
    var schema = SchemaBuilder.record("Row").fields().requiredInt("id").endRecord();
    var row = new GenericData.Record(schema);
    row.put("id", 1);
    var classified = new SQLException("Nested expression failed", state);
    var readFailure = new IOException("Failed to execute subquery for join: SELECT broken", classified);
    var reader = mock(RecordReader.class);
    when(reader.read()).thenReturn(row).thenThrow(readFailure);
    var select = SqlParser.parseSelect("SELECT id FROM input");
    try (var result = new JParqResultSet(reader, select, "input", "PUBLIC", null, null, List.of("id"), List.of("id"),
        null)) {
      assertTrue(result.next());
      SQLException failure = assertThrows(SQLException.class, result::next);
      assertEquals(state, failure.getSQLState());
      assertTrue(failure.getMessage().contains("Failed to execute subquery for join: SELECT broken"));
      assertSame(readFailure, failure.getCause());
    }
  }

  /** Checks truncation boundaries without splitting supplementary characters. */
  @Test
  void diagnosticValuesPreserveSurrogatePairs() {
    assertEquals("x".repeat(64), SqlExceptions.diagnosticValue("x".repeat(64)));
    assertEquals("x".repeat(64) + "…", SqlExceptions.diagnosticValue("x".repeat(65)));
    assertEquals("x".repeat(63) + "…", SqlExceptions.diagnosticValue("x".repeat(63) + "😀tail"));
    assertEquals("x".repeat(62) + "😀…", SqlExceptions.diagnosticValue("x".repeat(62) + "😀tail"));
    assertEquals("😀".repeat(32), SqlExceptions.diagnosticValue("😀".repeat(32)));
  }

  /** Ensures unclassified wrappers do not hide a classified nested failure. */
  @Test
  void nullSqlStateDoesNotHideClassifiedCause() {
    var classified = new SQLException("Invalid temporal value", "22008");
    var wrapper = new SQLException("Unclassified wrapper", classified);
    assertSame(classified, SqlExceptions.findSqlStateFailure(wrapper));
    var unclassified = new SQLException("Unclassified failure");
    assertNull(SqlExceptions.findSqlStateFailure(unclassified));
    assertNull(SqlExceptions.wrap("Evaluation failed", unclassified).getSQLState());
  }

  /**
   * Preserves unsupported features through nested wrappers and repeated wrapping.
   */
  @Test
  void unsupportedFeaturesKeepTheirClassification() {
    var original = new SQLFeatureNotSupportedException("Unsupported interval unit: WEEK", "0A000", 7);
    var wrapper = new IllegalArgumentException("Interval evaluation failed", original);
    assertSame(original, SqlExceptions.findSqlStateFailure(wrapper));
    var failure = SqlExceptions.wrap("Projection failed", wrapper);
    assertInstanceOf(SQLFeatureNotSupportedException.class, failure);
    assertEquals("0A000", failure.getSQLState());
    assertEquals(7, failure.getErrorCode());
    assertSame(wrapper, failure.getCause());
    assertTrue(failure.getMessage().contains("Unsupported interval unit: WEEK"));
    assertSame(failure, SqlExceptions.wrap(failure.getMessage(), failure));
    assertInstanceOf(SQLFeatureNotSupportedException.class, SqlExceptions.wrap("Subquery failed", failure));
  }

  /**
   * Preserves the direct failure's own SQLState, vendor code, and detail for
   * unclassified failures without claiming a syntax or numeric classification.
   */
  @Test
  void unrelatedErrorsKeepTheirOriginalClassification() {
    var original = new SQLException("Input unavailable", "08006", 42);
    assertNull(SqlExceptions.findSqlStateFailure(original));
    var failure = SqlExceptions.wrap("Failed to prepare query", original);
    assertEquals("08006", failure.getSQLState());
    assertEquals(42, failure.getErrorCode());
    assertSame(original, failure.getCause());
    assertEquals("Failed to prepare query: Input unavailable", failure.getMessage());
    var unexpected = new IllegalArgumentException("Unexpected engine failure");
    var wrapped = SqlExceptions.wrap("Evaluation failed", unexpected);
    assertNull(wrapped.getSQLState());
    assertSame(unexpected, wrapped.getCause());
    assertEquals("Evaluation failed: Unexpected engine failure", wrapped.getMessage());
  }

  /**
   * Appends the unclassified failure detail without inventing a SQLState; a
   * state-less SQLException simply keeps its null state (issue #330). The message
   * is neutral so the test does not endorse any SQLState for a specific failure.
   */
  @Test
  void unclassifiedFailureAppendsDetailWithoutInventingAState() {
    var original = new SQLException("boom");
    assertNull(SqlExceptions.findSqlStateFailure(original));
    var failure = SqlExceptions.wrap("Operation failed", original);
    assertNull(failure.getSQLState());
    assertSame(original, failure.getCause());
    assertEquals("Operation failed: boom", failure.getMessage());
  }

  /**
   * Keeps an unclassified failure detail redacted when the diagnostics renderer
   * hides private correlated bindings (issue #330).
   */
  @Test
  void unclassifiedFailureDetailIsRenderedLikeClassifiedDetail() {
    var original = new SQLException("Column not found: __JPARQ_CORRELATED_VALUE('private-key', 0)");
    var failure = SqlExceptions.wrap("Failed to evaluate UNNEST(a.values)", original, SqlExceptions::describe);
    assertNull(failure.getSQLState());
    assertSame(original, failure.getCause());
    assertEquals("Failed to evaluate UNNEST(a.values): Column not found: " + SqlExceptions.OUTER_VALUE,
        failure.getMessage());
    assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"));
  }

  /**
   * When the direct failure is a stateless runtime wrapper that carries only the
   * subquery context, the reason from the innermost SQLException is still
   * surfaced in the top-level message (issue #330).
   */
  @Test
  void unclassifiedWrapperSurfacesInnermostSqlExceptionDetail() {
    var reason = new SQLException("Table not found: PUBLIC.nosuchtable");
    var wrapper = new IllegalStateException("Failed to execute sub query: (SELECT 1 FROM x) [outer bindings: m.cyl=6]",
        reason);
    var failure = SqlExceptions.wrap("Failed to evaluate projection (SELECT ...)", wrapper);
    assertEquals("Failed to evaluate projection (SELECT ...): "
        + "Failed to execute sub query: (SELECT 1 FROM x) [outer bindings: m.cyl=6]: "
        + "Table not found: PUBLIC.nosuchtable", failure.getMessage());
    assertSame(wrapper, failure.getCause());
  }

  /**
   * Keeps the SQLState and vendor code of the first state-bearing SQLException in
   * the chain even when the direct failure is an ordinary runtime wrapper (issue
   * #330).
   */
  @Test
  void unclassifiedWrapperRetainsStatefulCauseStateAndCode() {
    var stateful = new SQLException("stateful cause", "08006", 7);
    var wrapper = new IllegalStateException("wrapper failed", stateful);
    var failure = SqlExceptions.wrap("operation failed", wrapper);
    assertEquals("08006", failure.getSQLState());
    assertEquals(7, failure.getErrorCode());
    assertSame(wrapper, failure.getCause());
    assertEquals("operation failed: wrapper failed: stateful cause", failure.getMessage());
  }

  /**
   * Selects the classified context when the cause chain carries a recognized
   * SQLState and the fallback context otherwise, without altering the wrapped
   * classification.
   */
  @Test
  void dualContextWrapSelectsContextByClassification() {
    var classified = SqlExceptions.syntaxFailure("ORDER BY position is outside the select list: 99");
    var failure = SqlExceptions.wrap("Failed to execute query", "Failed to open query data source", classified);
    assertInstanceOf(SQLSyntaxErrorException.class, failure);
    assertEquals("42000", failure.getSQLState());
    assertSame(classified, failure.getCause());
    assertEquals("Failed to execute query: ORDER BY position is outside the select list: 99", failure.getMessage());
    var plain = new IllegalStateException("disk gone");
    var fallback = SqlExceptions.wrap("Failed to execute query", "Failed to open query data source", plain);
    assertEquals(SQLException.class, fallback.getClass());
    assertNull(fallback.getSQLState());
    assertSame(plain, fallback.getCause());
    assertEquals("Failed to open query data source: disk gone", fallback.getMessage());
  }

  /** Tolerates a null failure without raising (issue #330). */
  @Test
  void wrapToleratesNullFailure() {
    var failure = SqlExceptions.wrap("Evaluation failed", null);
    assertNull(failure.getSQLState());
    assertNull(failure.getCause());
    assertEquals("Evaluation failed", failure.getMessage());
  }

  /**
   * Does not repeat a detail the context already carries, as when a caller passes
   * the failure message as the context (issue #330).
   */
  @Test
  void wrapDeduplicatesDetailAlreadyInContext() {
    var original = new SQLException("boom");
    var failure = SqlExceptions.wrap("boom", original);
    assertEquals("boom", failure.getMessage());
    var nested = new SQLException("boom", "08006", 3);
    var deduplicated = SqlExceptions.wrap("boom", nested);
    assertEquals("boom", deduplicated.getMessage());
    assertEquals("08006", deduplicated.getSQLState());
    assertEquals(3, deduplicated.getErrorCode());
  }

  /**
   * Redacts the innermost SQLException detail before appending it, so a raw
   * correlated binding never reaches the message even when the caller's
   * diagnostics do no redaction of their own (issue #330).
   */
  @Test
  void unclassifiedInnermostDetailIsRedactedOnIdentityPath() {
    var reason = new SQLException("Column not found: __JPARQ_CORRELATED_VALUE('private-key', 0)");
    var wrapper = new IllegalStateException("Failed to execute sub query: (SELECT 1 FROM x)", reason);
    var failure = SqlExceptions.wrap("Failed reading first parquet record", wrapper);
    assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure::getMessage);
    assertEquals("Failed reading first parquet record: Failed to execute sub query: (SELECT 1 FROM x): "
        + "Column not found: " + SqlExceptions.OUTER_VALUE, failure.getMessage());
  }

  /**
   * Redacts the direct failure's detail as well, so the redaction guarantee does
   * not depend on where in the chain the binding text sits (issue #330).
   */
  @Test
  void unclassifiedDirectDetailIsRedactedOnIdentityPath() {
    var original = new SQLException("Column not found: __JPARQ_CORRELATED_VALUE('private-key', 0)");
    var failure = SqlExceptions.wrap("ctx", original);
    assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure::getMessage);
    assertEquals("ctx: Column not found: " + SqlExceptions.OUTER_VALUE, failure.getMessage());
    var wrapper = new IllegalStateException("sub __JPARQ_CORRELATED_VALUE('private-key', 0)",
        new SQLException("reason"));
    var wrappedFailure = SqlExceptions.wrap("ctx", wrapper);
    assertFalse(wrappedFailure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), wrappedFailure::getMessage);
    assertEquals("ctx: sub " + SqlExceptions.OUTER_VALUE + ": reason", wrappedFailure.getMessage());
  }

  /**
   * Redacts the failure's own text when it is used as a fallback for a null
   * context, which comes from the failure rather than the caller (issue #330).
   */
  @Test
  void wrapNullContextFallbackIsRedacted() {
    var original = new SQLException("x __JPARQ_CORRELATED_VALUE('k', 0)");
    var failure = SqlExceptions.wrap(null, original);
    assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure::getMessage);
    assertEquals("java.sql.SQLException: x " + SqlExceptions.OUTER_VALUE, failure.getMessage());
  }

  /**
   * Does not drop a short detail by substring coincidence: a detail is skipped
   * only when the message equals it or carries it at a segment boundary (issue
   * #330).
   */
  @Test
  void wrapDoesNotDropShortDetailBySubstringCoincidence() {
    var original = new SQLException("__JPARQ_CORRELATED_VALUE('k', 0)");
    var failure = SqlExceptions.wrap("ctx " + SqlExceptions.OUTER_VALUE, original);
    assertEquals("ctx " + SqlExceptions.OUTER_VALUE + ": " + SqlExceptions.OUTER_VALUE, failure.getMessage());
  }

  /**
   * De-duplicates a detail only when it forms a complete segment: a detail that
   * is only a prefix of a longer segment is still appended, and a context that
   * starts with the detail as a complete segment is not repeated (issue #330).
   */
  @Test
  void wrapDeDuplicatesOnlyCompleteSegments() {
    var prefix = SqlExceptions.wrap("ctx: boomerang", new SQLException("boom"));
    assertEquals("ctx: boomerang: boom", prefix.getMessage());
    var segment = SqlExceptions.wrap("boom: more", new SQLException("boom"));
    assertEquals("boom: more", segment.getMessage());
  }

  /**
   * Redacts the classified detail as well, so both branches stay consistent on
   * caller paths whose diagnostics do no redaction of their own (issue #330).
   */
  @Test
  void classifiedDetailIsRedactedOnIdentityPath() {
    var classified = new SQLSyntaxErrorException("Bad __JPARQ_CORRELATED_VALUE('private-key', 0)", "42000");
    var wrapper = new IllegalArgumentException("Bad __JPARQ_CORRELATED_VALUE('private-key', 0)", classified);
    var failure = SqlExceptions.wrap("ctx", wrapper);
    assertInstanceOf(SQLSyntaxErrorException.class, failure);
    assertEquals("42000", failure.getSQLState());
    assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure::getMessage);
    assertEquals("ctx: Bad " + SqlExceptions.OUTER_VALUE, failure.getMessage());
  }

  /**
   * A cyclic cause chain (which {@code initCause} allows) terminates the chain
   * walk instead of looping forever (issue #330).
   */
  @Test
  void wrapToleratesCyclicCauseChain() {
    var outer = new SQLException("outer boom");
    var inner = new SQLException("inner boom", outer);
    outer.initCause(inner);
    var failure = SqlExceptions.wrap("operation failed", outer);
    assertSame(outer, failure.getCause());
    assertNull(failure.getSQLState());
    assertEquals("operation failed: outer boom: inner boom", failure.getMessage());
  }

  /**
   * When the chain holds several SQLExceptions, the reported state and vendor
   * code come from the first (outermost) state-bearing one while the appended
   * detail comes from the innermost one (issue #330).
   */
  @Test
  void wrapStateAndDetailMayComeFromDifferentChainLinks() {
    var innermost = new SQLException("inner detail", "08006", 7);
    var outermost = new SQLException("outer detail", "08000", 1, innermost);
    var failure = SqlExceptions.wrap("operation failed", outermost);
    assertSame(outermost, failure.getCause());
    assertEquals("08000", failure.getSQLState());
    assertEquals(1, failure.getErrorCode());
    assertEquals("operation failed: outer detail: inner detail", failure.getMessage());
  }
}
