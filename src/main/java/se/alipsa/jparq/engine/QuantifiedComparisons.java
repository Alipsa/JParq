package se.alipsa.jparq.engine;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.AnyType;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.avro.generic.IndexedRecord;

/**
 * Shared SQL three-valued comparisons over quantified arrays and subqueries.
 */
public final class QuantifiedComparisons {
  private QuantifiedComparisons() {
  }

  /**
   * A resolved scalar and its context-specific candidate coercion.
   *
   * @param value
   *          the scalar value, possibly null
   * @param coerceCandidate
   *          converts candidates to the scalar's type
   */
  public record ScalarOperand(Object value, UnaryOperator<Object> coerceCandidate) {
  }

  /**
   * Resolves the quantified side of a comparison and evaluates its operands once.
   *
   * @param comparison
   *          the binary comparison
   * @param scalars
   *          resolves scalar values and candidate coercion
   * @param valuesEvaluator
   *          evaluates array operands
   * @param subqueries
   *          executes subqueries
   * @return the comparison result, or null for SQL UNKNOWN
   * @throws IllegalArgumentException
   *           if the comparison does not have exactly one quantified operand
   */
  public static Boolean evaluate(BinaryExpression comparison,
      java.util.function.Function<Expression, ScalarOperand> scalars,
      java.util.function.Function<Expression, Object> valuesEvaluator,
      java.util.function.Function<Select, List<Object>> subqueries) {
    Expression left = comparison.getLeftExpression();
    Expression right = comparison.getRightExpression();
    boolean leftQuantified = JSqlParserSupport.isQuantifiedOperand(left);
    boolean rightQuantified = JSqlParserSupport.isQuantifiedOperand(right);
    if (leftQuantified && rightQuantified) {
      throw new IllegalArgumentException("Quantified comparison requires a scalar operand on one side");
    }
    if (!leftQuantified && !rightQuantified) {
      throw new IllegalArgumentException("ANY/SOME/ALL evaluation requested without a quantified operand");
    }
    ScalarOperand scalar = scalars.apply(leftQuantified ? right : left);
    return evaluate(leftQuantified ? left : right, scalar.value(), comparison.getStringExpression(), leftQuantified,
        valuesEvaluator, subqueries, scalar.coerceCandidate());
  }

  /**
   * Evaluates ANY, SOME, or ALL using the caller's row or group context.
   *
   * @param anyExpr
   *          the quantified operand
   * @param otherValue
   *          the scalar value
   * @param operator
   *          the comparison operator
   * @param anyOnLeft
   *          whether the quantified operand precedes the operator
   * @param valuesEvaluator
   *          evaluates array operands in the current context
   * @param subqueries
   *          executes subqueries in the current context
   * @param coerceCandidate
   *          adapts candidates to the scalar column's type, if needed
   * @return the comparison result, or null for SQL UNKNOWN
   * @throws IllegalArgumentException
   *           if the operand is not quantified or contains invalid array values
   */
  public static Boolean evaluate(Expression anyExpr, Object otherValue, String operator, boolean anyOnLeft,
      java.util.function.Function<Expression, Object> valuesEvaluator,
      java.util.function.Function<Select, List<Object>> subqueries, UnaryOperator<Object> coerceCandidate) {
    if (!JSqlParserSupport.isQuantifiedOperand(anyExpr)) {
      throw new IllegalArgumentException("Not a quantified operand: " + anyExpr);
    }
    List<?> values;
    boolean isAll;
    if (anyExpr instanceof AnyComparisonExpression subquery) {
      values = subqueries.apply(subquery.getSelect());
      isAll = subquery.getAnyType() == AnyType.ALL;
    } else {
      Function function = (Function) anyExpr;
      if (function.getParameters() == null || function.getParameters().size() != 1) {
        throw new IllegalArgumentException("ANY/SOME/ALL requires exactly one array operand");
      }
      Object array = valuesEvaluator.apply(function.getParameters().getFirst());
      if (array == null) {
        return null;
      }
      if (array instanceof Object[] objects) {
        array = Arrays.asList(objects);
      }
      if (!(array instanceof List<?> list)) {
        throw new IllegalArgumentException("ANY/SOME/ALL requires an array operand");
      }
      values = list;
      isAll = "ALL".equalsIgnoreCase(function.getName());
    }
    if (values.stream().anyMatch(QuantifiedComparisons::isNonScalar)) {
      throw new IllegalArgumentException("ANY/SOME/ALL requires scalar array elements");
    }
    Boolean result = isAll;
    for (Object rawValue : values) {
      Object candidate = coerceCandidate.apply(rawValue);
      Integer cmp = SqlComparisons.compareOrUnknown(anyOnLeft ? candidate : otherValue,
          anyOnLeft ? otherValue : candidate);
      Boolean comparison = SqlComparisons.compare(operator, cmp);
      result = isAll ? SqlTruth.and(result, comparison) : SqlTruth.or(result, comparison);
      if (isAll && Boolean.FALSE.equals(result) || !isAll && Boolean.TRUE.equals(result)) {
        return result;
      }
    }
    return result;
  }

  /**
   * Identifies structured candidates that cannot participate in scalar
   * comparison.
   *
   * @param value
   *          the candidate value
   * @return whether the value is an array, map, or record
   */
  private static boolean isNonScalar(Object value) {
    return value instanceof List<?> || value instanceof Map<?, ?> || value instanceof Object[]
        || value instanceof IndexedRecord;
  }
}
