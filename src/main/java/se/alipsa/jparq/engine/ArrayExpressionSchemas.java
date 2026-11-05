package se.alipsa.jparq.engine;

import java.sql.JDBCType;
import java.util.List;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import se.alipsa.jparq.NumericType;
import se.alipsa.jparq.engine.function.StructArrayTypes;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/** Resolves array declarations without using the current outer row's values. */
public final class ArrayExpressionSchemas {
  /** Prevents utility class construction. */
  private ArrayExpressionSchemas() {
  }

  /**
   * Resolves typed bindings, array constructors, and common expression branches.
   * Unknown expressions retain the caller's value-based inference.
   *
   * @param expression
   *          expression whose declaration is needed
   * @return declared schema, or null when unknown
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if known array element types are
   *           incompatible
   */
  public static Schema resolve(Expression expression) {
    return resolve(expression, column -> null);
  }

  /**
   * Resolves declarations using the input columns visible to the expression.
   *
   * @param expression
   *          expression whose declaration is needed
   * @param columns
   *          source column schema resolver
   * @return declared schema, or null when unknown
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if known array element types are
   *           incompatible
   */
  public static Schema resolve(Expression expression, java.util.function.Function<Column, Schema> columns) {
    if (expression instanceof Column column) {
      return JdbcTypeMapper.nonNullSchema(columns.apply(column));
    }
    if (expression instanceof Function function && CorrelatedBindings.matches(function)) {
      return JdbcTypeMapper.nonNullSchema(CorrelatedBindings.schema(function));
    }
    if (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return resolve(list.getFirst(), columns);
    }
    if (expression instanceof net.sf.jsqlparser.statement.select.ParenthesedSelect select) {
      return resolve(select.getSelect(), columns);
    }
    if (expression instanceof net.sf.jsqlparser.statement.select.PlainSelect select
        && select.getSelectItems().size() == 1) {
      return resolve(select.getSelectItems().getFirst().getExpression(), columns);
    }
    if (expression instanceof ArrayConstructor array) {
      Schema element = common(array.getExpressions(), columns);
      return element == null ? null : Schema.createArray(element);
    }
    if (expression instanceof Function function && function.getParameters() != null) {
      if ("ARRAY".equalsIgnoreCase(function.getName())) {
        Schema element = common(function.getParameters(), columns);
        return element == null ? null : Schema.createArray(element);
      }
      if ("NULLIF".equalsIgnoreCase(function.getName()) && !function.getParameters().isEmpty()) {
        return resolve(function.getParameters().getFirst(), columns);
      }
      if ("COALESCE".equalsIgnoreCase(function.getName())) {
        return common(function.getParameters(), columns);
      }
    }
    if (expression instanceof CaseExpression choice) {
      return common(ExpressionBranches.caseResults(choice), columns);
    }
    if (expression instanceof BinaryExpression binary && NumericExpressionTypes.operation(expression) != null) {
      NumericType left = numeric(resolve(binary.getLeftExpression(), columns));
      NumericType right = numeric(resolve(binary.getRightExpression(), columns));
      return left == null || right == null
          ? null
          : left.arithmetic(right, NumericExpressionTypes.operation(expression)).schema();
    }
    if (expression instanceof SignedExpression signed) {
      return resolve(signed.getExpression(), columns);
    }
    NumericType numeric = NumericExpressionTypes.resolve(expression, column -> numeric(columns.apply(column)));
    if (numeric != null) {
      return numeric.schema();
    }
    return expression instanceof StringValue ? Schema.create(Schema.Type.STRING) : null;
  }

  /**
   * Describes a known numeric declaration without consulting projection metadata.
   *
   * @param schema
   *          declared operand schema, or null
   * @return its numeric descriptor, or null for unknown or nonnumeric operands
   */
  private static NumericType numeric(Schema schema) {
    return schema == null ? null : NumericType.of(JdbcTypeMapper.mapSchemaToJdbcType(schema), schema);
  }

  /**
   * Combines known branch declarations, retaining array and logical types.
   *
   * @param expressions
   *          candidate values or branches
   * @param columns
   *          source column schema resolver
   * @return common known schema, or null when no declaration is available
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if known array element types are
   *           incompatible
   */
  private static Schema common(List<? extends Expression> expressions,
      java.util.function.Function<Column, Schema> columns) {
    Schema result = null;
    for (Expression expression : expressions) {
      result = common(result, resolve(expression, columns));
    }
    return result;
  }

  /**
   * Promotes numeric elements and recursively combines ARRAY and STRUCT
   * declarations.
   *
   * @param left
   *          first declaration, possibly null
   * @param right
   *          second declaration, possibly null
   * @return the common declaration
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if known array element types are
   *           incompatible
   */
  private static Schema common(Schema left, Schema right) {
    if (left == null || right == null || left.equals(right)) {
      return left == null ? right : left;
    }
    if (left.getType() == Schema.Type.RECORD && right.getType() == Schema.Type.RECORD) {
      return StructArrayTypes.common(left, right);
    }
    if (left.getType() == Schema.Type.ARRAY && right.getType() == Schema.Type.ARRAY) {
      return Schema.createArray(common(left.getElementType(), right.getElementType()));
    }
    NumericType first = NumericType.of(JdbcTypeMapper.mapSchemaToJdbcType(left), left);
    NumericType second = NumericType.of(JdbcTypeMapper.mapSchemaToJdbcType(right), right);
    if (first != null && second != null) {
      return first.common(second).schema();
    }
    throw SqlExceptions.incompatibleArrayElements(JDBCType.valueOf(JdbcTypeMapper.mapSchemaToJdbcType(left)).getName(),
        JDBCType.valueOf(JdbcTypeMapper.mapSchemaToJdbcType(right)).getName());
  }
}
