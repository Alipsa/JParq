package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;

/**
 * Collects result expressions without evaluating their values or conditions.
 */
public final class ExpressionBranches {
  /** Prevents utility class construction. */
  private ExpressionBranches() {
  }

  /**
   * Collects THEN expressions and an explicit ELSE in SQL order.
   *
   * @param choice
   *          simple or searched CASE expression
   * @return result branches, excluding an omitted ELSE
   */
  public static List<Expression> caseResults(CaseExpression choice) {
    List<Expression> branches = new ArrayList<>();
    if (choice.getWhenClauses() != null) {
      choice.getWhenClauses().forEach(clause -> branches.add(clause.getThenExpression()));
    }
    if (choice.getElseExpression() != null) {
      branches.add(choice.getElseExpression());
    }
    return branches;
  }
}
