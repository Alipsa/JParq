package se.alipsa.jparq.engine.window;

import java.util.List;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Representation of a LAST_VALUE analytic expression.
 */
public final class LastValueWindow {

  private final AnalyticExpression expression;
  private final List<Expression> partitionExpressions;
  private final List<OrderByElement> orderByElements;
  private final Expression valueExpression;
  private final WindowElement windowElement;

  /**
   * Constructor.
   *
   * @param expression
   *          the analytic expression
   * @param partitionExpressions
   *          expressions defining the PARTITION BY clause
   * @param orderByElements
   *          ORDER BY elements defining the ordering within each partition
   * @param valueExpression
   *          the expression evaluated by the LAST_VALUE function
   * @param windowElement
   *          the window frame specification requested by the analytic expression
   */
  public LastValueWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
      List<OrderByElement> orderByElements, Expression valueExpression, WindowElement windowElement) {
    this.expression = expression;
    this.partitionExpressions = partitionExpressions == null ? List.of() : List.copyOf(partitionExpressions);
    this.orderByElements = orderByElements == null ? List.of() : List.copyOf(orderByElements);
    this.valueExpression = valueExpression;
    this.windowElement = windowElement;
  }

  /**
   * Retrieve the underlying analytic expression.
   *
   * @return the {@link AnalyticExpression} represented by this window
   */
  public AnalyticExpression expression() {
    return expression;
  }

  /**
   * Retrieve expressions defining the PARTITION BY clause.
   *
   * @return immutable list of partition expressions
   */
  public List<Expression> partitionExpressions() {
    return partitionExpressions;
  }

  /**
   * Retrieve ORDER BY elements defining the ordering within each partition.
   *
   * @return immutable list of {@link OrderByElement} descriptors
   */
  public List<OrderByElement> orderByElements() {
    return orderByElements;
  }

  /**
   * Retrieve the value expression evaluated by the LAST_VALUE function.
   *
   * @return the expression supplying the value to retrieve from the last row of
   *         the frame
   */
  public Expression valueExpression() {
    return valueExpression;
  }

  /**
   * Retrieve the window frame specification associated with this LAST_VALUE
   * window.
   *
   * @return the {@link WindowElement} describing the requested frame, or
   *         {@code null} when the default applies
   */
  public WindowElement windowElement() {
    return windowElement;
  }
}
