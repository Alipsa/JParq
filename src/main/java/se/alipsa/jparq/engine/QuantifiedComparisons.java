package se.alipsa.jparq.engine;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.AnyType;
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
   *           if an array operand is invalid
   */
  public static Boolean evaluate(Expression anyExpr, Object otherValue, String operator, boolean anyOnLeft,
      java.util.function.Function<Expression, Object> valuesEvaluator,
      java.util.function.Function<Select, List<Object>> subqueries, UnaryOperator<Object> coerceCandidate) {
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
      if (list.stream().anyMatch(value -> value instanceof List<?> || value instanceof Map<?, ?>
          || value instanceof Object[] || value instanceof IndexedRecord)) {
        throw new IllegalArgumentException("ANY/SOME/ALL requires scalar array elements");
      }
      values = list;
      isAll = "ALL".equalsIgnoreCase(function.getName());
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

}
