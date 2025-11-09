package se.alipsa.jparq.engine.window;

import java.util.List;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Representation of an NTH_VALUE analytic expression.
 */
public final class NthValueWindow {

  private final AnalyticExpression expression;
  private final List<Expression> partitionExpressions;
  private final List<OrderByElement> orderByElements;
  private final Expression valueExpression;
  private final Expression nthExpression;
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
   *          the expression evaluated by the NTH_VALUE function
   * @param nthExpression
   *          the expression identifying the desired position within the frame
   * @param windowElement
   *          the window frame specification requested by the analytic expression
   */
  public NthValueWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
      List<OrderByElement> orderByElements, Expression valueExpression, Expression nthExpression,
      WindowElement windowElement) {
    this.expression = expression;
    this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
    this.orderByElements = orderByElements == null ? List.of() : orderByElements;
    this.valueExpression = valueExpression;
    this.nthExpression = nthExpression;
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
   * Retrieve the value expression evaluated by the NTH_VALUE function.
   *
   * @return the expression supplying the value to retrieve
   */
  public Expression valueExpression() {
    return valueExpression;
  }

  /**
   * Retrieve the expression that resolves the requested position within the
   * frame.
   *
   * @return the expression yielding the N argument for NTH_VALUE
   */
  public Expression nthExpression() {
    return nthExpression;
  }

  /**
   * Retrieve the window frame specification associated with this NTH_VALUE
   * window.
   *
   * @return the {@link WindowElement} describing the requested frame, or
   *         {@code null} when the default applies
   */
  public WindowElement windowElement() {
    return windowElement;
  }
}
