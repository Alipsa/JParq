package jparq.window;

import java.util.List;
import java.util.stream.Collectors;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.window.FirstValueWindow;
import se.alipsa.jparq.engine.window.LastValueWindow;
import se.alipsa.jparq.engine.window.RowNumberWindow;
import se.alipsa.jparq.engine.window.SumWindow;
import se.alipsa.jparq.engine.window.WindowPlan;

/**
 * Tests for verifying the {@link WindowPlan.Builder} behavior.
 */
class WindowPlanBuilderTest {

  /**
   * Ensure that the builder produces an empty plan when no windows are provided.
   */
  @Test
  void builderProducesEmptyPlanByDefault() {
    WindowPlan plan = WindowPlan.builder().build();

    Assertions.assertTrue(plan.isEmpty(), "Expected plan to be empty when no windows are supplied");
  }

  /**
   * Verify that configured windows are captured by the builder.
   *
   * @throws JSQLParserException
   *           if analytic expressions cannot be parsed
   */
  @Test
  void builderCapturesConfiguredWindows() throws JSQLParserException {
    AnalyticExpression rowNumberExpression = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)");
    List<Expression> rowPartitions = partitionExpressions(rowNumberExpression);
    List<OrderByElement> rowOrder = orderByElements(rowNumberExpression);
    RowNumberWindow rowNumberWindow = new RowNumberWindow(rowNumberExpression, rowPartitions, rowOrder);

    AnalyticExpression sumExpression = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("SUM(salary) OVER (PARTITION BY dept ORDER BY change_date DESC)");
    List<Expression> sumPartitions = partitionExpressions(sumExpression);
    List<OrderByElement> sumOrder = orderByElements(sumExpression);
    Expression sumArgument = analyticArgument(sumExpression);
    SumWindow sumWindow = new SumWindow(sumExpression, sumPartitions, sumOrder, sumArgument,
        sumExpression.isDistinct() || sumExpression.isUnique(), sumExpression.getWindowElement());

    AnalyticExpression firstValueExpression = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY change_date DESC)");
    List<Expression> firstValuePartitions = partitionExpressions(firstValueExpression);
    List<OrderByElement> firstValueOrder = orderByElements(firstValueExpression);
    Expression firstValueArgument = analyticArgument(firstValueExpression);
    FirstValueWindow firstValueWindow = new FirstValueWindow(firstValueExpression, firstValuePartitions,
        firstValueOrder, firstValueArgument, firstValueExpression.getWindowElement());

    AnalyticExpression lastValueExpression = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY change_date DESC)");
    List<Expression> lastValuePartitions = partitionExpressions(lastValueExpression);
    List<OrderByElement> lastValueOrder = orderByElements(lastValueExpression);
    Expression lastValueArgument = analyticArgument(lastValueExpression);
    LastValueWindow lastValueWindow = new LastValueWindow(lastValueExpression, lastValuePartitions, lastValueOrder,
        lastValueArgument, lastValueExpression.getWindowElement());

    WindowPlan plan = WindowPlan.builder().rowNumberWindows(List.of(rowNumberWindow)).sumWindows(List.of(sumWindow))
        .firstValueWindows(List.of(firstValueWindow)).lastValueWindows(List.of(lastValueWindow)).build();

    Assertions.assertFalse(plan.isEmpty(), "Expected plan to contain configured windows");
    Assertions.assertEquals(1, plan.rowNumberWindows().size(), "Unexpected ROW_NUMBER window count");
    Assertions.assertEquals(1, plan.sumWindows().size(), "Unexpected SUM window count");
    Assertions.assertEquals(1, plan.firstValueWindows().size(), "Unexpected FIRST_VALUE window count");
    Assertions.assertEquals(1, plan.lastValueWindows().size(), "Unexpected LAST_VALUE window count");
  }

  /**
   * Convert the partition expression list into an immutable collection of
   * expressions.
   *
   * @param analyticExpression
   *          analytic expression providing the partition list
   * @return immutable list of partition expressions
   */
  private static List<Expression> partitionExpressions(AnalyticExpression analyticExpression) {
    ExpressionList<?> partitionList = analyticExpression.getPartitionExpressionList();
    if (partitionList == null) {
      return List.of();
    }
    return partitionList.stream().map(Expression.class::cast).collect(Collectors.toUnmodifiableList());
  }

  /**
   * Convert the analytic expression order by elements into an immutable
   * collection.
   *
   * @param analyticExpression
   *          analytic expression providing ordering metadata
   * @return immutable list of order by elements
   */
  private static List<OrderByElement> orderByElements(AnalyticExpression analyticExpression) {
    List<OrderByElement> elements = analyticExpression.getOrderByElements();
    return elements == null ? List.of() : List.copyOf(elements);
  }

  /**
   * Retrieve the analytic argument expression handled by the supplied analytic
   * descriptor.
   *
   * @param analyticExpression
   *          analytic expression defining the argument
   * @return the argument expression, or {@code null} when not present
   */
  private static Expression analyticArgument(AnalyticExpression analyticExpression) {
    return analyticExpression.getExpression();
  }
}
