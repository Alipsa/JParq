package se.alipsa.jparq.engine;

import java.util.function.Predicate;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
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
   * @return original or equivalent CAST, or null for a non-CAST expression
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
    boolean secondType = SqlParser.isConvertTypeName(operandText);
    boolean firstColumn = columns.test(candidate);
    boolean secondColumn = columns.test(operandText);
    boolean firstBounded = type.getArgumentsStringList() != null && !type.getArgumentsStringList().isEmpty()
        || candidate != null && candidate.indexOf('(') >= 0;
    boolean secondBounded = secondType && operandText.indexOf('(') >= 0;
    if (firstType && secondType && !firstColumn && !secondColumn && firstBounded == secondBounded) {
      return null;
    }
    boolean swap = secondType
        && (firstColumn && !secondColumn || !firstType || !firstBounded && secondBounded && !secondColumn);
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
}
