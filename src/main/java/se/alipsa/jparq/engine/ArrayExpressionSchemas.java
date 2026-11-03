package se.alipsa.jparq.engine;

import java.sql.SQLException;
import java.util.List;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import org.apache.avro.Schema;
import se.alipsa.jparq.NumericType;
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
   * @throws SQLException
   *           if known array element types are incompatible
   */
  public static Schema resolve(Expression expression) throws SQLException {
    if (expression instanceof Function function && CorrelatedBindings.matches(function)) {
      return JdbcTypeMapper.nonNullSchema(CorrelatedBindings.schema(function));
    }
    if (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return resolve(list.getFirst());
    }
    if (expression instanceof net.sf.jsqlparser.statement.select.ParenthesedSelect select) {
      return resolve(select.getSelect());
    }
    if (expression instanceof net.sf.jsqlparser.statement.select.PlainSelect select
        && select.getSelectItems().size() == 1) {
      return resolve(select.getSelectItems().getFirst().getExpression());
    }
    if (expression instanceof ArrayConstructor array) {
      Schema element = common(array.getExpressions());
      return element == null ? null : Schema.createArray(element);
    }
    if (expression instanceof Function function && function.getParameters() != null) {
      if ("ARRAY".equalsIgnoreCase(function.getName())) {
        Schema element = common(function.getParameters());
        return element == null ? null : Schema.createArray(element);
      }
      if ("COALESCE".equalsIgnoreCase(function.getName())) {
        return common(function.getParameters());
      }
    }
    if (expression instanceof CaseExpression choice) {
      return common(ExpressionBranches.caseResults(choice));
    }
    if (expression instanceof BinaryExpression binary && NumericExpressionTypes.operation(expression) != null) {
      NumericType left = numeric(resolve(binary.getLeftExpression()));
      NumericType right = numeric(resolve(binary.getRightExpression()));
      return left == null || right == null
          ? null
          : left.arithmetic(right, NumericExpressionTypes.operation(expression)).schema();
    }
    if (expression instanceof SignedExpression signed) {
      return resolve(signed.getExpression());
    }
    NumericType numeric = NumericExpressionTypes.resolve(expression, column -> null);
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
   * @return common known schema, or null when no declaration is available
   * @throws SQLException
   *           if declarations are incompatible
   */
  private static Schema common(List<? extends Expression> expressions) throws SQLException {
    Schema result = null;
    for (Expression expression : expressions) {
      result = common(result, resolve(expression));
    }
    return result;
  }

  /**
   * Promotes numeric elements and recursively combines array declarations.
   *
   * @param left
   *          first declaration, possibly null
   * @param right
   *          second declaration, possibly null
   * @return the common declaration
   * @throws SQLException
   *           if the known declarations are incompatible
   */
  private static Schema common(Schema left, Schema right) throws SQLException {
    if (left == null || right == null || left.equals(right)) {
      return left == null ? right : left;
    }
    if (left.getType() == Schema.Type.ARRAY && right.getType() == Schema.Type.ARRAY) {
      return Schema.createArray(common(left.getElementType(), right.getElementType()));
    }
    NumericType first = NumericType.of(JdbcTypeMapper.mapSchemaToJdbcType(left), left);
    NumericType second = NumericType.of(JdbcTypeMapper.mapSchemaToJdbcType(right), right);
    if (first != null && second != null) {
      return first.common(second).schema();
    }
    throw new SQLException("UNNEST array elements must have compatible types", "42000");
  }
}
