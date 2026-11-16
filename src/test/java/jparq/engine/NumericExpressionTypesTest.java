package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqPreparedStatement;
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
   * Execution closes declaration work before replacing its plan cache.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if preparing or executing the statement fails
   */
  @Test
  void executionClosesPreviousDeclarations(@TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = (JParqPreparedStatement) connection.prepareStatement("SELECT 1")) {
      try (var executors = mockConstruction(SubqueryExecutor.class)) {
        assertNotNull(statement.firstProjectionNumericType());
        SubqueryExecutor declarations = executors.constructed().getFirst();
        try (var result = statement.executeQuery()) {
          verify(declarations).close();
          assertTrue(result.next());
          assertEquals(1, result.getInt(1));
        }
      }
    }
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
