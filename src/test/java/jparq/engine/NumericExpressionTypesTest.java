package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqPreparedStatement;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.NumericType;
import se.alipsa.jparq.engine.ArrayExpressionSchemas;
import se.alipsa.jparq.engine.CorrelatedBindings;
import se.alipsa.jparq.engine.NumericExpressionTypes;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;

/** Verifies numeric schema conversion and branch type combination. */
class NumericExpressionTypesTest {
  /**
   * Known NULL bindings do not hide other VALUES rows' declared numeric type.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if preparing or executing the statement fails
   */
  @Test
  void nullBindingsAreNeutralInValues(@TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var bindings = new CorrelatedBindings("null-row", Collections.singletonList(null),
            List.of(Schema.create(Schema.Type.NULL)));
        var statement = connection.prepareStatement(
            "SELECT v.x FROM " + "(VALUES (__JPARQ_CORRELATED_VALUE('null-row', 0)), (CAST(NULL AS INTEGER))) v(x)");
        var result = statement.executeQuery()) {
      assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
      for (int row = 0; row < 2; row++) {
        assertTrue(result.next());
        assertNull(result.getObject(1));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Joined VALUES declarations inspect every expression once without evaluation.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if preparing or inspecting the statement fails
   */
  @Test
  void valuesDeclarationsAreCachedWithoutEvaluation(@TempDir Path directory) throws Exception {
    int rows = 1500;
    String values = IntStream.range(0, rows).mapToObj(index -> "(1, 2)").collect(Collectors.joining(","));
    var literals = new AtomicInteger();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = (JParqPreparedStatement) connection
            .prepareStatement("SELECT v.x FROM (VALUES " + values + ") v(x,y) JOIN (VALUES (1)) q(z) ON 1 = 1");
        var evaluators = mockConstruction(ValueExpressionEvaluator.class);
        var schemas = mockStatic(ArrayExpressionSchemas.class, CALLS_REAL_METHODS)) {
      schemas.when(() -> ArrayExpressionSchemas.resolve(any(), any(), any())).thenAnswer(invocation -> {
        if (invocation.getArgument(0) instanceof LongValue) {
          literals.incrementAndGet();
        }
        return invocation.callRealMethod();
      });
      assertEquals(Schema.Type.INT, statement.firstProjectionSchema().getType());
      assertEquals(Types.INTEGER, statement.firstProjectionNumericType().jdbcType());
      assertEquals(Schema.Type.INT, statement.firstProjectionSchema().getType());
      assertEquals(rows * 2, literals.get());
      assertTrue(evaluators.constructed().isEmpty());
    }
  }

  /**
   * Closing a statement closes its retained declaration executor.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if preparing, inspecting, or closing the statement fails
   */
  @Test
  void closesDeclarationExecutor(@TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = (JParqPreparedStatement) connection.prepareStatement("SELECT 1")) {
      try (var executors = mockConstruction(SubqueryExecutor.class)) {
        assertNotNull(statement.firstProjectionNumericType());
        assertEquals(1, executors.constructed().size());
        statement.close();
        verify(executors.constructed().getFirst()).close();
      }
    }
  }

  /**
   * Execution retains the declaration executor so re-executed statements reuse
   * its cached shapes; only closing the statement closes it.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if preparing, executing, or closing the statement fails
   */
  @Test
  void executionRetainsDeclarationsUntilClose(@TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = (JParqPreparedStatement) connection.prepareStatement("SELECT 1")) {
      try (var executors = mockConstruction(SubqueryExecutor.class)) {
        assertNotNull(statement.firstProjectionNumericType());
        SubqueryExecutor declarations = executors.constructed().getFirst();
        try (var result = statement.executeQuery()) {
          verify(declarations, never()).close();
          assertTrue(result.next());
          assertEquals(1, result.getInt(1));
        }
        statement.close();
        verify(declarations).close();
      }
    }
  }

  /**
   * Statement-level declaration caches make re-executed statements prepare each
   * scalar subquery's metadata once instead of once per execution, while a
   * closing executor leaves the shared entries in place.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if preparing or inspecting a subquery fails
   */
  @Test
  void executionExecutorsShareDeclarations(@TempDir Path directory) throws Exception {
    Select select = (Select) CCJSqlParserUtil.parse("SELECT 1");
    Schema schema = Schema.create(Schema.Type.INT);
    var shared = new SubqueryExecutor.SharedDeclarations();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var nested = mockConstruction(JParqPreparedStatement.class, (prepared, context) -> {
          when(prepared.firstProjectionSchema()).thenReturn(schema);
          when(prepared.firstProjectionNumericType()).thenReturn(NumericExpressionTypes.numeric(schema));
        })) {
      try (
          var first = new SubqueryExecutor((JParqConnection) connection, null, null,
              new SubqueryExecutor.ExecutionPlans(), null, shared);
          var second = new SubqueryExecutor((JParqConnection) connection, null, null,
              new SubqueryExecutor.ExecutionPlans(), null, shared)) {
        assertEquals(schema, first.projectionResultSchema(select));
        assertEquals(schema, second.projectionResultSchema(select));
        assertEquals(1, nested.constructed().size());
        first.close();
        assertEquals(schema, second.projectionResultSchema(select));
      }
    }
  }

  /**
   * A plain shape with a stable statement keeps its metadata statement cached for
   * row execution instead of closing it, while an unstable statement is still
   * closed as soon as its declaration is read.
   *
   * @throws Exception
   *           if the declaration lookup fails
   */
  @Test
  void stableShapeRetainsMetadataStatement() throws Exception {
    Select select = (Select) CCJSqlParserUtil.parse("SELECT 1");
    var connection = mock(JParqConnection.class);
    var stable = mock(JParqPreparedStatement.class);
    when(stable.isStableSubqueryShape()).thenReturn(true);
    when(stable.firstProjectionNumericType()).thenReturn(new NumericType(Types.INTEGER, 10, 0));
    try (var executor = new SubqueryExecutor(connection, sql -> stable, null, new SubqueryExecutor.ExecutionPlans(),
        null, null)) {
      assertEquals(Types.INTEGER, executor.projectionNumericType(select).jdbcType());
      verify(stable, never()).close();
    }
    verify(stable).close();

    var disposable = mock(JParqPreparedStatement.class);
    when(disposable.isStableSubqueryShape()).thenReturn(false);
    when(disposable.firstProjectionNumericType()).thenReturn(new NumericType(Types.INTEGER, 10, 0));
    var executor = new SubqueryExecutor(connection, sql -> disposable, null, new SubqueryExecutor.ExecutionPlans(),
        null, null);
    try (executor) {
      assertEquals(Types.INTEGER, executor.projectionNumericType(select).jdbcType());
      verify(disposable).close();
    }
  }

  /**
   * A declaration-only executor never executes rows, so it closes even a stable
   * metadata statement as soon as its declaration is read instead of holding the
   * statement's materialized inputs until the executor closes.
   *
   * @throws Exception
   *           if the declaration lookup fails
   */
  @Test
  void declarationOnlyExecutorClosesStableMetadataStatement() throws Exception {
    Select select = (Select) CCJSqlParserUtil.parse("SELECT 1");
    var connection = mock(JParqConnection.class);
    var stable = mock(JParqPreparedStatement.class);
    when(stable.isStableSubqueryShape()).thenReturn(true);
    when(stable.firstProjectionNumericType()).thenReturn(new NumericType(Types.INTEGER, 10, 0));
    try (var executor = new SubqueryExecutor(connection, sql -> stable, null, new SubqueryExecutor.ExecutionPlans(),
        null, new SubqueryExecutor.SharedDeclarations(), false)) {
      assertEquals(Types.INTEGER, executor.projectionNumericType(select).jdbcType());
      verify(stable).close();
    }
    verify(stable).close();
  }

  /**
   * A row-executing executor closes a retained non-reusable metadata statement
   * after its single run, so the statement's materialized inputs do not outlive
   * the subquery result, while a reusable statement stays cached until the
   * executor closes.
   *
   * @throws Exception
   *           if the declaration lookup or execution fails
   */
  @Test
  void executedRetainedStatementClosesWhenNotReusable() throws Exception {
    Select select = (Select) CCJSqlParserUtil.parse("SELECT 1");
    var connection = mock(JParqConnection.class);
    assertRetainedStatementLifecycle(select, connection, false);
    assertRetainedStatementLifecycle(select, connection, true);
  }

  /**
   * A retained statement whose shape is never executed, for example a CASE branch
   * that is typed but never selected, stays open until the executor closes,
   * releasing its materialized inputs with the statement.
   *
   * @throws Exception
   *           if the declaration lookup or close fails
   */
  @Test
  void neverExecutedRetainedStatementClosesWithExecutor() throws Exception {
    Select select = (Select) CCJSqlParserUtil.parse("SELECT 1");
    var connection = mock(JParqConnection.class);
    assertNeverExecutedStatementClosesWithExecutor(select, connection);
  }

  /**
   * Reads one shape's declaration without executing it, verifying that a retained
   * statement stays open until the executor closes.
   *
   * @param select
   *          subquery shape
   * @param connection
   *          mock connection
   * @throws Exception
   *           if the declaration lookup or close fails
   */
  private void assertNeverExecutedStatementClosesWithExecutor(Select select, JParqConnection connection)
      throws Exception {
    var prepared = mock(JParqPreparedStatement.class);
    when(prepared.isStableSubqueryShape()).thenReturn(true);
    when(prepared.isReusableSubqueryShape()).thenReturn(false);
    when(prepared.firstProjectionNumericType()).thenReturn(new NumericType(Types.INTEGER, 10, 0));
    when(prepared.declaredProjectionSchema()).thenReturn(Schema.create(Schema.Type.INT));
    try (var executor = new SubqueryExecutor(connection, sql -> prepared, null, new SubqueryExecutor.ExecutionPlans(),
        null, null)) {
      assertEquals(Types.INTEGER, executor.projectionNumericType(select).jdbcType());
      verify(prepared, never()).close();
    }
    verify(prepared, times(1)).close();
  }

  /**
   * Reads one shape's declaration and executes it, verifying whether a retained
   * statement closes after the run or with the executor.
   *
   * @param select
   *          subquery shape
   * @param connection
   *          mock connection
   * @param reusable
   *          whether the prepared statement is reusable
   * @throws Exception
   *           if the declaration lookup or execution fails
   */
  private void assertRetainedStatementLifecycle(Select select, JParqConnection connection, boolean reusable)
      throws Exception {
    var rs = emptyResultSet();
    var prepared = mock(JParqPreparedStatement.class);
    when(prepared.isStableSubqueryShape()).thenReturn(true);
    when(prepared.isReusableSubqueryShape()).thenReturn(reusable);
    when(prepared.firstProjectionNumericType()).thenReturn(new NumericType(Types.INTEGER, 10, 0));
    when(prepared.declaredProjectionSchema()).thenReturn(Schema.create(Schema.Type.INT));
    when(prepared.executeQuery()).thenReturn(rs);
    try (var executor = new SubqueryExecutor(connection, sql -> prepared, null, new SubqueryExecutor.ExecutionPlans(),
        null, null)) {
      assertEquals(Types.INTEGER, executor.projectionNumericType(select).jdbcType());
      verify(prepared, never()).close();
      var result = executor.execute(select);
      assertTrue(result.rows().isEmpty());
      if (reusable) {
        verify(prepared, never()).close();
      } else {
        verify(prepared).close();
      }
    }
    verify(prepared, times(1)).close();
  }

  /**
   * Builds an empty one-column result set for mocked subquery execution.
   *
   * @return empty mocked result set
   * @throws SQLException
   *           if the mock cannot be stubbed
   */
  private static JParqResultSet emptyResultSet() throws SQLException {
    var rs = mock(JParqResultSet.class);
    var meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getColumnLabel(1)).thenReturn("1");
    when(rs.next()).thenReturn(false);
    return rs;
  }

  /**
   * Optional numeric inspection shields public callers from incompatible types.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if preparing or inspecting the statement fails
   */
  @Test
  void incompatibleNumericDeclarationIsUnknown(@TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = (JParqPreparedStatement) connection.prepareStatement("SELECT 1");
        var resolver = mockStatic(NumericExpressionTypes.class)) {
      resolver.when(() -> NumericExpressionTypes.resolve(any(), any(), any(), any()))
          .thenThrow(new IllegalArgumentException("Incompatible numeric declaration"));
      assertNull(statement.firstProjectionNumericType());
    }
  }

  /**
   * Value and numeric lookups share one nested preparation per scalar shape.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if statement preparation or declaration lookup fails
   */
  @Test
  void projectionLookupsShareNestedPreparation(@TempDir Path directory) throws Exception {
    Schema schema = Schema.create(Schema.Type.INT);
    NumericType numeric = NumericExpressionTypes.numeric(schema);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = (JParqPreparedStatement) connection.prepareStatement("SELECT (SELECT 1)")) {
      try (var nested = mockConstruction(JParqPreparedStatement.class, (prepared, context) -> {
        when(prepared.firstProjectionSchema()).thenReturn(schema);
        when(prepared.firstProjectionNumericType()).thenReturn(numeric);
      })) {
        assertEquals(schema, statement.firstProjectionSchema());
        assertEquals(numeric, statement.firstProjectionNumericType());
        assertEquals(schema, statement.firstProjectionSchema());
        assertEquals(1, nested.constructed().size());
        verify(nested.constructed().getFirst(), never()).executeQuery();
        verify(nested.constructed().getFirst()).close();
      }
    }
  }

  /**
   * Unknown branches combined with NULL subqueries cannot become always NULL.
   *
   * @throws Exception
   *           if an expression cannot be parsed
   */
  @Test
  void nullSubqueriesDoNotHideUnknownBranches() throws Exception {
    for (String sql : List.of("CASE WHEN TRUE THEN (SELECT NULL) ELSE unknown_column END",
        "CASE WHEN TRUE THEN unknown_column ELSE (SELECT NULL) END", "COALESCE((SELECT NULL), unknown_column)",
        "COALESCE(unknown_column, (SELECT NULL))")) {
      var expression = CCJSqlParserUtil.parseExpression(sql);
      assertNull(ArrayExpressionSchemas.resolve(expression, column -> null, select -> Schema.create(Schema.Type.NULL)),
          sql);
      assertNull(NumericExpressionTypes.resolve(expression, column -> null, select -> Schema.create(Schema.Type.NULL)),
          sql);
    }
  }

  /**
   * Public numeric inspection safely declines untyped correlated placeholders.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if preparing or inspecting the statement fails
   */
  @Test
  void untypedProjectionBindings(@TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var bindings = new CorrelatedBindings("unknown", Collections.singletonList(null));
        var statement = (JParqPreparedStatement) connection
            .prepareStatement("SELECT COALESCE(ROUND(__JPARQ_CORRELATED_VALUE('unknown', 0), 0), 1)")) {
      assertNull(statement.firstProjectionSchema());
      assertNull(statement.firstProjectionNumericType());
    }
  }

  /**
   * Known numeric set branches do not depend on optional value-schema lookups.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if statement preparation or declaration lookup fails
   */
  @Test
  void numericSetBranchesSkipValueSchemas(@TempDir Path directory) throws Exception {
    var numeric = new NumericType(Types.DECIMAL, 8, 3);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = (JParqPreparedStatement) connection.prepareStatement("SELECT 1 UNION ALL SELECT 1.0")) {
      try (var branches = mockConstruction(JParqPreparedStatement.class, (branch, context) -> {
        when(branch.firstProjectionNumericType()).thenReturn(numeric);
        when(branch.firstProjectionSchema()).thenThrow(new SQLException("value schema is unavailable"));
      })) {
        assertEquals(numeric, statement.firstProjectionNumericType());
        assertEquals(2, branches.constructed().size());
        for (var branch : branches.constructed()) {
          verify(branch, never()).firstProjectionSchema();
          verify(branch, never()).executeQuery();
        }
      }
    }
  }

  /** Recognizes nullable numeric schemas and retains decimal bounds. */
  @Test
  void nullableNumericSchemas() {
    Schema decimal = LogicalTypes.decimal(8, 3).addToSchema(Schema.create(Schema.Type.BYTES));
    assertEquals(new NumericType(Types.DECIMAL, 8, 3), NumericExpressionTypes.numeric(decimal));
    Schema nullable = Schema.createUnion(Schema.create(Schema.Type.NULL), decimal);
    assertEquals(new NumericType(Types.DECIMAL, 8, 3), NumericExpressionTypes.numeric(nullable));
    assertEquals(NumericType.of(Types.INTEGER, 0, 0), NumericExpressionTypes.numeric(Schema.create(Schema.Type.INT)));
    assertNull(NumericExpressionTypes.numeric(null));
    assertNull(NumericExpressionTypes.numeric(Schema.create(Schema.Type.NULL)));
    assertNull(NumericExpressionTypes.numeric(Schema.create(Schema.Type.STRING)));
  }

  /**
   * Retains unbounded decimals in either order while honoring approximate
   * precedence.
   */
  @Test
  void unboundedCommonBranches() {
    var unbounded = new NumericType(Types.DECIMAL, 0, 0);
    var bounded = new NumericType(Types.DECIMAL, 8, 3);
    var integer = NumericType.of(Types.INTEGER, 0, 0);
    var approximate = NumericType.of(Types.DOUBLE, 0, 0);
    for (NumericType type : List.of(unbounded, bounded, integer)) {
      assertEquals(unbounded, NumericExpressionTypes.common(unbounded, type));
      assertEquals(unbounded, NumericExpressionTypes.common(type, unbounded));
    }
    assertEquals(approximate, NumericExpressionTypes.common(unbounded, approximate));
    assertEquals(approximate, NumericExpressionTypes.common(approximate, unbounded));
    assertEquals(new NumericType(Types.DECIMAL, 13, 3), NumericExpressionTypes.common(integer, bounded));
    assertEquals(bounded, NumericExpressionTypes.common(null, bounded));
    assertEquals(bounded, NumericExpressionTypes.common(bounded, null));
    assertNull(NumericExpressionTypes.common(null, null));
  }
}
