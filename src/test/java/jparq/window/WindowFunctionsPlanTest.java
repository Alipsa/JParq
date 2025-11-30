package jparq.window;

import java.util.Collections;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.window.WindowFunctions;
import se.alipsa.jparq.engine.window.WindowPlan;

/**
 * Tests for verifying window planning collects supported analytic expressions.
 */
class WindowFunctionsPlanTest {

  /**
   * Ensure that supported analytic expressions are collected into the plan.
   *
   * @throws JSQLParserException
   *           if the SQL expression cannot be parsed
   */
  @Test
  void planCollectsSupportedAnalyticExpressions() throws JSQLParserException {
    List<Expression> expressions = List.of(CCJSqlParserUtil.parseExpression("employee_id"),
        CCJSqlParserUtil.parseExpression("ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)"),
        CCJSqlParserUtil.parseExpression("SUM(salary) OVER (PARTITION BY dept ORDER BY change_date DESC)"),
        CCJSqlParserUtil.parseExpression("COUNT(*) OVER (PARTITION BY dept)"),
        CCJSqlParserUtil.parseExpression("LAG(salary, 1, 0) OVER (PARTITION BY dept ORDER BY change_date DESC)"),
        CCJSqlParserUtil.parseExpression("FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY change_date DESC "
            + "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"));

    WindowPlan plan = WindowFunctions.plan(expressions);

    Assertions.assertNotNull(plan, "A plan should be produced when analytic expressions are present");
    Assertions.assertEquals(1, plan.rowNumberWindows().size(), "Expected one ROW_NUMBER analytic definition");
    Assertions.assertEquals(1, plan.sumWindows().size(), "Expected one SUM analytic definition");
    Assertions.assertEquals(1, plan.countWindows().size(), "Expected one COUNT analytic definition");
    Assertions.assertEquals(1, plan.lagWindows().size(), "Expected one LAG analytic definition");
    Assertions.assertEquals(1, plan.firstValueWindows().size(), "Expected one FIRST_VALUE analytic definition");
    Assertions.assertTrue(plan.rankWindows().isEmpty(), "No RANK windows should be present");
    Assertions.assertTrue(plan.avgWindows().isEmpty(), "No AVG windows should be present");
  }

  /**
   * Verify that a plan is not generated when no analytic expressions are
   * discovered.
   *
   * @throws JSQLParserException
   *           if the SQL expression cannot be parsed
   */
  @Test
  void planReturnsNullWithoutAnalyticExpressions() throws JSQLParserException {
    List<Expression> expressions = List.of(CCJSqlParserUtil.parseExpression("employee_id"),
        CCJSqlParserUtil.parseExpression("department"));

    WindowPlan plan = WindowFunctions.plan(expressions);

    Assertions.assertNull(plan, "No plan should be created when analytic expressions are absent");
  }

  /**
   * Verify that invalid analytic definitions are rejected during planning.
   *
   * @throws JSQLParserException
   *           if an expression cannot be parsed
   */
  @Test
  void planRejectsInvalidAnalyticConfigurations() throws JSQLParserException {
    AnalyticExpression rowNumberWithArgument = new AnalyticExpression();
    rowNumberWithArgument.setName("ROW_NUMBER");
    rowNumberWithArgument.setExpression(CCJSqlParserUtil.parseExpression("salary"));

    IllegalArgumentException rowNumberException = Assertions.assertThrows(IllegalArgumentException.class,
        () -> WindowFunctions.plan(List.of(rowNumberWithArgument)));
    Assertions.assertTrue(
        rowNumberException.getMessage().contains("ROW_NUMBER must not include an argument expression"),
        "ROW_NUMBER with an argument should be rejected");

    AnalyticExpression rankWithoutOrder = new AnalyticExpression();
    rankWithoutOrder.setName("RANK");
    IllegalArgumentException rankException = Assertions.assertThrows(IllegalArgumentException.class,
        () -> WindowFunctions.plan(List.of(rankWithoutOrder)));
    Assertions.assertTrue(rankException.getMessage().contains("RANK requires an ORDER BY clause"),
        "RANK requires an ORDER BY clause");

    AnalyticExpression countMissingArgument = new AnalyticExpression();
    countMissingArgument.setName("COUNT");
    IllegalArgumentException countException = Assertions.assertThrows(IllegalArgumentException.class,
        () -> WindowFunctions.plan(List.of(countMissingArgument)));
    Assertions.assertTrue(countException.getMessage().contains("COUNT requires an argument expression"),
        "COUNT without an argument should be rejected");

    AnalyticExpression lagWithoutOrder = new AnalyticExpression();
    lagWithoutOrder.setName("LAG");
    lagWithoutOrder.setExpression(CCJSqlParserUtil.parseExpression("salary"));
    IllegalArgumentException lagException = Assertions.assertThrows(IllegalArgumentException.class,
        () -> WindowFunctions.plan(List.of(lagWithoutOrder)));
    Assertions.assertTrue(lagException.getMessage().contains("LAG requires an ORDER BY clause"),
        "LAG requires ordering to be valid");

    AnalyticExpression nthValueMissingOffset = new AnalyticExpression();
    nthValueMissingOffset.setName("NTH_VALUE");
    nthValueMissingOffset.setExpression(CCJSqlParserUtil.parseExpression("salary"));
    OrderByElement salaryOrder = new OrderByElement();
    salaryOrder.setExpression(CCJSqlParserUtil.parseExpression("salary"));
    nthValueMissingOffset.setOrderByElements(List.of(salaryOrder));
    IllegalArgumentException nthValueException = Assertions.assertThrows(IllegalArgumentException.class,
        () -> WindowFunctions.plan(List.of(nthValueMissingOffset)));
    Assertions.assertTrue(nthValueException.getMessage().contains("NTH_VALUE requires a position argument"),
        "NTH_VALUE requires a numeric position argument");
  }

  /**
   * Confirm that null, unsupported or incomplete analytic definitions are safely
   * ignored during planning.
   *
   * @throws JSQLParserException
   *           if expressions cannot be parsed
   */
  @Test
  void planHandlesNullAndUnsupportedDefinitions() throws JSQLParserException {
    Assertions.assertNull(WindowFunctions.plan(null), "Null expression lists should yield no plan");
    Assertions.assertNull(WindowFunctions.plan(Collections.singletonList(null)),
        "Null entries inside the list should be skipped");

    AnalyticExpression missingName = new AnalyticExpression();
    missingName.setExpression(CCJSqlParserUtil.parseExpression("salary"));
    Assertions.assertNull(WindowFunctions.plan(List.of(missingName)), "Analytics without names should be ignored");

    AnalyticExpression unsupported = new AnalyticExpression();
    unsupported.setName("ROW_NUM");
    Assertions.assertNull(WindowFunctions.plan(List.of(unsupported)), "Unsupported analytics should be ignored");

    AnalyticExpression distinctSum = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("SUM(DISTINCT salary) OVER (ORDER BY salary)");
    IllegalArgumentException distinctException = Assertions.assertThrows(IllegalArgumentException.class,
        () -> WindowFunctions.plan(List.of(distinctSum)));
    Assertions.assertTrue(distinctException.getMessage().contains("does not support DISTINCT"),
        "SUM must reject DISTINCT modifiers");
  }
}
