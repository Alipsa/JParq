package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.IsUnknownExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;

/** Recognizes Boolean expression types without evaluating input rows. */
public final class BooleanExpressions {
  /** Prevents construction of this expression utility. */
  private BooleanExpressions() {
  }

  /**
   * Identifies predicates supported as values by the expression evaluator.
   *
   * @param expression
   *          the expression to inspect
   * @return whether the expression is a Boolean predicate
   */
  public static boolean isPredicate(Expression expression) {
    return expression instanceof NotExpression || expression instanceof AndExpression
        || expression instanceof OrExpression || expression instanceof ComparisonOperator
        || expression instanceof IsDistinctExpression || expression instanceof IsNullExpression
        || expression instanceof IsBooleanExpression || expression instanceof IsUnknownExpression
        || expression instanceof ExistsExpression || expression instanceof InExpression || expression instanceof Between
        || expression instanceof LikeExpression || expression instanceof SimilarToExpression;
  }

  /**
   * Determines whether an expression has a statically known Boolean result.
   * Untyped NULL branches are compatible with a known Boolean branch.
   *
   * @param expression
   *          the expression to inspect
   * @param booleanColumn
   *          resolves whether a column has a Boolean schema
   * @return whether the expression has Boolean type
   */
  public static boolean isBoolean(Expression expression, Predicate<Column> booleanColumn) {
    Expression value = ExpressionEvaluator.unwrapParenthesis(expression);
    if (value instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return isBoolean(list.getFirst(), booleanColumn);
    }
    if (isPredicate(value) || value instanceof BooleanValue) {
      return true;
    }
    if (value instanceof Column column) {
      return booleanColumn.test(column);
    }
    if (value instanceof CastExpression cast) {
      return cast.getColDataType() != null && ("BOOLEAN".equalsIgnoreCase(cast.getColDataType().getDataType())
          || "BOOL".equalsIgnoreCase(cast.getColDataType().getDataType()));
    }
    List<Expression> branches = new ArrayList<>();
    if (value instanceof CaseExpression choice) {
      if (choice.getWhenClauses() != null) {
        choice.getWhenClauses().forEach(clause -> branches.add(clause.getThenExpression()));
      }
      branches.add(choice.getElseExpression());
    } else if (value instanceof Function function && "COALESCE".equalsIgnoreCase(function.getName())
        && function.getParameters() != null) {
      branches.addAll(function.getParameters());
    }
    boolean typed = false;
    for (Expression branch : branches) {
      Expression unwrapped = ExpressionEvaluator.unwrapParenthesis(branch);
      if (unwrapped == null || unwrapped instanceof NullValue) {
        continue;
      }
      if (!isBoolean(unwrapped, booleanColumn)) {
        return false;
      }
      typed = true;
    }
    return typed;
  }
}
