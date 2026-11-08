package jparq.engine;

import static jparq.support.QueryTestSupport.rootCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.concurrent.atomic.AtomicInteger;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.JSqlParserSupport;
import se.alipsa.jparq.engine.SqlParser;

/**
 * Verifies shared traversal without requiring callers to handle query scopes.
 */
class StatementExpressionTraversalTest {
  @TempDir
  Path directory;

  /**
   * Visits each CAST exactly once, including inside quantified subqueries.
   *
   * @param sql
   *          statement with one CAST in a nested scope or clause
   * @throws Exception
   *           if parsing fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT CAST(1 AS INTEGER)", "SELECT SUBSTRING('abc' FROM CAST(1 AS INTEGER))",
      "SELECT SUBSTRING('abc' FROM 1 FOR CAST(1 AS INTEGER))", "SELECT POSITION(CAST(1 AS VARCHAR) IN '123')",
      "SELECT OVERLAY('abc' PLACING CAST(1 AS VARCHAR) FROM 1)", "SELECT TRIM('x' FROM CAST(1 AS VARCHAR))",
      "SELECT TRIM(BOTH FROM CAST(1 AS VARCHAR))", "SELECT TRIM(LEADING FROM CAST(1 AS VARCHAR))",
      "SELECT TRIM(FROM CAST(1 AS VARCHAR))", "SELECT SUBSTRING('abc' FROM (SELECT CAST(1 AS INTEGER)))",
      "SELECT 1 WHERE 1 < ANY (SELECT CAST(1 AS INTEGER))", "SELECT 1 WHERE 1 < ALL (SELECT CAST(1 AS INTEGER))",
      "SELECT 1 WHERE 1 < SOME (SELECT CAST(1 AS INTEGER))",
      "SELECT 1 WHERE 1 < ANY (SELECT 2 WHERE 2 < ALL (SELECT CAST(1 AS INTEGER)))",
      "SELECT 1 LIMIT (SELECT CAST(1 AS INTEGER))", "SELECT 1 UNION ALL SELECT 2 LIMIT (SELECT CAST(1 AS INTEGER))",
      "SELECT 1 OFFSET (SELECT CAST(1 AS INTEGER)) ROWS", "SELECT 1 FETCH FIRST (SELECT CAST(1 AS INTEGER)) ROWS ONLY",
      "SELECT 1 UNION ALL SELECT 2 OFFSET (SELECT CAST(1 AS INTEGER)) ROWS",
      "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n OFFSET (SELECT CAST(1 AS INTEGER)) ROWS",
      "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n LIMIT 1 OFFSET (SELECT CAST(1 AS INTEGER))",
      "SELECT 1 UNION ALL SELECT 2 FETCH FIRST (SELECT CAST(1 AS INTEGER)) ROWS ONLY",
      "SELECT * FROM UNNEST(ARRAY[(SELECT CAST(1 AS INTEGER))]) AS t(x)",
      "WITH c AS (SELECT CAST(1 AS INTEGER)) SELECT 1 UNION ALL SELECT 2", "VALUES (CAST(1 AS INTEGER))",
      "SELECT 1 FROM (a JOIN b ON a.id = CAST(b.id AS INTEGER))", "SELECT d.n FROM (SELECT CAST(1 AS INTEGER) AS n) d"
  })
  void visitsNestedCastOnce(String sql) throws Exception {
    var visits = new AtomicInteger();
    var statement = (Select) JSqlParserSupport.parseStatement(sql);
    JSqlParserSupport.visitExpressions(statement, new JSqlParserSupport.StatementExpressionVisitor() {
      /** {@inheritDoc} */
      @Override
      public <S> Void visit(CastExpression cast, S context) {
        visits.incrementAndGet();
        return super.visit(cast, context);
      }
    });
    assertEquals(1, visits.get(), sql);
  }

  /**
   * Marks datetime-dependent named operands for execution-time replanning.
   *
   * @param sql
   *          query containing a datetime keyword in a named operand
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT POSITION('x' IN CAST(CURRENT_DATE AS VARCHAR))",
      "SELECT SUBSTRING('abc' FROM EXTRACT(HOUR FROM LOCALTIME))",
      "SELECT OVERLAY('abc' PLACING CAST(CURRENT_DATE AS VARCHAR) FROM 1)",
      "SELECT TRIM(BOTH FROM CAST(LOCALTIME AS VARCHAR))"
  })
  void namedArgumentsRetainDatetimeDependencies(String sql) {
    assertTrue(SqlParser.prepareQuery(sql).timeDependent(), sql);
  }

  /**
   * Keeps ordinary named operands independent of execution time.
   *
   * @param sql
   *          query without datetime value functions
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT SUBSTRING('abc' FROM 1)", "SELECT POSITION('b' IN 'abc')", "SELECT OVERLAY('abc' PLACING 'x' FROM 1)",
      "SELECT TRIM(BOTH FROM ' abc ')", "SELECT POSITION('CURRENT_DATE' IN 'CURRENT_DATE')"
  })
  void ordinaryNamedArgumentsRemainTimeIndependent(String sql) {
    assertFalse(SqlParser.prepareQuery(sql).timeDependent(), sql);
  }

  /**
   * Rejects unsupported FILTER clauses during preparation, including in skipped
   * branches.
   *
   * @param expression
   *          named operand containing an aggregate FILTER
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SUBSTRING('abc' FROM CAST(COUNT(*) FILTER (WHERE id > 1) AS INTEGER))",
      "POSITION(CAST(COUNT(*) FILTER (WHERE id > 1) AS VARCHAR) IN '123')",
      "TRIM(BOTH FROM CAST(COUNT(*) FILTER (WHERE id > 1) AS VARCHAR))",
      "CASE WHEN FALSE THEN SUBSTRING('abc' FROM CAST(COUNT(*) FILTER (WHERE id > 1) AS INTEGER)) ELSE 'x' END"
  })
  void namedArgumentsRejectFilterDuringPreparation(String expression) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      var error = assertThrows(SQLSyntaxErrorException.class,
          () -> connection.prepareStatement("SELECT " + expression + " FROM (VALUES (1)) t(id)"));
      assertEquals("42000", error.getSQLState());
      assertTrue(error.getMessage().contains("Aggregate FILTER (WHERE ...) clauses are not supported"),
          error.getMessage());
    }
  }

  /**
   * Applies qualified datetime keyword validation inside named operands.
   */
  @Test
  void namedArgumentsRejectQualifiedDatetimeKeywords() {
    var error = assertThrows(IllegalArgumentException.class,
        () -> SqlParser.prepareQuery("SELECT POSITION('x' IN CAST(t.current_date AS VARCHAR)) FROM t"));
    assertTrue(rootCause(error).getMessage().contains("Reserved datetime keyword must be delimited"),
        rootCause(error).getMessage());
  }

  /**
   * Rejects projection aliases embedded in table-free ORDER BY expressions.
   */
  @Test
  void namedArgumentsDoNotExposeOrderByAliasesInsideExpressions() {
    var error = assertThrows(IllegalArgumentException.class,
        () -> SqlParser.prepareQuery("SELECT 'abc' AS s ORDER BY SUBSTRING(s FROM 1)"));
    assertEquals("Missing column without FROM: s", rootCause(error).getMessage());
  }

}
