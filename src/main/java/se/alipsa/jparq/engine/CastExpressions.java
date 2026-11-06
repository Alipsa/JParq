package se.alipsa.jparq.engine;

import java.util.function.Predicate;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.TranscodingFunction;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;

/**
 * Shares CAST declarations with cast-style CONVERT without rewriting SQL
 * labels.
 */
public final class CastExpressions {
  /** Prevents utility construction. */
  private CastExpressions() {
  }

  /**
   * Resolves a CAST or unambiguously ordered cast-style CONVERT declaration.
   *
   * @param expression
   *          expression to inspect
   * @return equivalent CAST, or null for other expressions and charset conversion
   */
  public static CastExpression asCast(Expression expression) {
    return asCast(expression, name -> false);
  }

  /**
   * Resolves both CONVERT argument orders using known source column names. A view
   * is returned without changing the original expression or its label.
   *
   * @param expression
   *          expression to inspect
   * @param columns
   *          identifies visible source column names
   * @return original or equivalent CAST, or null for a non-CAST or ambiguous
   *         expression
   */
  public static CastExpression asCast(Expression expression, Predicate<String> columns) {
    Expression value = ExpressionEvaluator.unwrapParenthesis(expression);
    if (value instanceof CastExpression cast) {
      return cast;
    }
    if (!(value instanceof TranscodingFunction convert) || convert.isTranscodeStyle()
        || convert.getColDataType() == null || convert.getExpression() == null) {
      return null;
    }
    Expression operand = convert.getExpression();
    ColDataType type = convert.getColDataType();
    String candidate = type.getDataType();
    String operandText = operand.toString();
    boolean firstType = SqlParser.isConvertTypeName(candidate);
    boolean firstColumn = columns.test(candidate);
    // A valid target followed by a function is a value expression, even when
    // its name (such as CHAR) is also a SQL type. Source-first binary bounds
    // still use the type position when it names a bound source column.
    boolean functionValue = operand instanceof Function && firstType && !firstColumn;
    boolean secondType = !functionValue && SqlParser.isConvertTypeName(operandText);
    boolean secondColumn = columns.test(operandText);
    if (firstType && secondType && firstColumn == secondColumn && operand instanceof Column) {
      return null;
    }
    boolean swap = secondType && (firstColumn && !secondColumn || !firstType);
    if (swap) {
      operand = new Column(candidate);
      type = new ColDataType();
      type.setDataType(operandText);
    }
    CastExpression cast = new CastExpression();
    cast.setColDataType(type);
    cast.setLeftExpression(operand);
    return cast;
  }

  /**
   * Resolves a cast-style CONVERT after source columns have been bound.
   *
   * @param convert
   *          cast-style conversion
   * @param columns
   *          identifies visible source column names
   * @return equivalent CAST declaration
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if the arguments cannot be resolved
   */
  public static CastExpression requireCast(TranscodingFunction convert, Predicate<String> columns) {
    CastExpression cast = asCast(convert, columns);
    if (cast == null) {
      throw SqlExceptions.syntaxFailure(
          "Ambiguous CONVERT arguments; use CAST with an explicit target type: " + SqlExceptions.describe(convert));
    }
    return cast;
  }
}
