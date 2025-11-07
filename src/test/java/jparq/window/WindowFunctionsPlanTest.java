package jparq.window;

import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
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
        CCJSqlParserUtil.parseExpression("SUM(salary) OVER (PARTITION BY dept ORDER BY change_date DESC)"));

    WindowPlan plan = WindowFunctions.plan(expressions);

    Assertions.assertNotNull(plan, "A plan should be produced when analytic expressions are present");
    Assertions.assertEquals(1, plan.rowNumberWindows().size(), "Expected one ROW_NUMBER analytic definition");
    Assertions.assertEquals(1, plan.sumWindows().size(), "Expected one SUM analytic definition");
    Assertions.assertTrue(plan.rankWindows().isEmpty(), "No RANK windows should be present");
    Assertions.assertTrue(plan.avgWindows().isEmpty(), "No AVG windows should be present");
  }

  /**
   * Verify that a plan is not generated when no analytic expressions are discovered.
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
}
