package se.alipsa.jparq.engine;

import java.sql.Types;
import java.util.Locale;
import java.util.function.Function;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/**
 * Resolves interval declarations without evaluating rows or losing empty-result
 * metadata.
 */
public final class IntervalExpressionTypes {

  /** Prevents utility class instantiation. */
  private IntervalExpressionTypes() {
  }

  /**
   * Recognizes interval literals, arithmetic, and typed input columns.
   *
   * @param expression
   *          expression to inspect
   * @param columns
   *          resolver for input column schemas
   * @return whether the expression has interval type
   */
  public static boolean isInterval(Expression expression, Function<Column, Schema> columns) {
    Expression value = ExpressionEvaluator.unwrapParenthesis(expression);
    if (value instanceof IntervalExpression) {
      return true;
    }
    if (value instanceof Column column) {
      return IntervalType.matches(columns.apply(column));
    }
    if (value instanceof net.sf.jsqlparser.expression.Function function && CorrelatedBindings.matches(function)) {
      return IntervalType.matches(CorrelatedBindings.schema(function));
    }
    if (value instanceof SignedExpression signed) {
      return isInterval(signed.getExpression(), columns);
    }
    if (value instanceof Subtraction subtraction) {
      int left = temporalType(subtraction.getLeftExpression(), columns);
      int right = temporalType(subtraction.getRightExpression(), columns);
      if (left != Types.OTHER && right != Types.OTHER) {
        return !(left == Types.DATE && right == Types.DATE);
      }
      return isInterval(subtraction.getLeftExpression(), columns)
          && isInterval(subtraction.getRightExpression(), columns);
    }
    if (value instanceof Addition addition) {
      return isInterval(addition.getLeftExpression(), columns) && isInterval(addition.getRightExpression(), columns);
    }
    return false;
  }

  /**
   * Resolves supported temporal operands by declaration.
   *
   * @param expression
   *          possible temporal operand
   * @param columns
   *          resolver for input column schemas
   * @return DATE, TIME, TIMESTAMP, or OTHER
   */
  private static int temporalType(Expression expression, Function<Column, Schema> columns) {
    Expression value = ExpressionEvaluator.unwrapParenthesis(expression);
    if (value instanceof Column column) {
      int type = JdbcTypeMapper.mapSchemaToJdbcType(columns.apply(column));
      return type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP ? type : Types.OTHER;
    }
    if (value instanceof DateValue) {
      return Types.DATE;
    }
    if (value instanceof TimeValue) {
      return Types.TIME;
    }
    if (value instanceof TimestampValue) {
      return Types.TIMESTAMP;
    }
    if (value instanceof net.sf.jsqlparser.expression.Function function && CorrelatedBindings.matches(function)) {
      return JdbcTypeMapper.mapSchemaToJdbcType(CorrelatedBindings.schema(function));
    }
    String type = null;
    if (value instanceof DateTimeLiteralExpression literal) {
      type = literal.getType().name();
    } else if (value instanceof TimeKeyExpression key) {
      type = key.getStringValue();
    } else if (value instanceof CastExpression cast && cast.getColDataType() != null) {
      type = cast.getColDataType().getDataType();
    }
    if (type == null) {
      return Types.OTHER;
    }
    return switch (type.toUpperCase(Locale.ROOT).replace("CURRENT_", "").replace("()", "")) {
      case "DATE" -> Types.DATE;
      case "TIME" -> Types.TIME;
      case "TIMESTAMP", "LOCALTIMESTAMP" -> Types.TIMESTAMP;
      default -> Types.OTHER;
    };
  }
}
