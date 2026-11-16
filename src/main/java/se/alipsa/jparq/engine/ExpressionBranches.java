package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

/**
 * Collects result expressions without evaluating their values or conditions.
 */
public final class ExpressionBranches {
  /** Prevents utility class construction. */
  private ExpressionBranches() {
  }

  /**
   * Identifies a COALESCE expression or its two-argument IFNULL equivalent,
   * independently of name capitalization. In SQL, IFNULL(x, y) selects the first
   * non-NULL argument exactly like COALESCE(x, y).
   *
   * @param expression
   *          expression to inspect
   * @return whether the expression invokes COALESCE or two-argument IFNULL
   */
  public static boolean isCoalesce(Expression expression) {
    if (!(expression instanceof Function function)) {
      return false;
    }
    if ("COALESCE".equalsIgnoreCase(function.getName())) {
      return true;
    }
    return "IFNULL".equalsIgnoreCase(function.getName()) && function.getParameters() != null
        && function.getParameters().size() == 2;
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
