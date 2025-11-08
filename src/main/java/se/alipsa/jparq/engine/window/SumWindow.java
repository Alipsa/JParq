package se.alipsa.jparq.engine.window;

import java.util.List;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Representation of a SUM analytic expression.
 */
public final class SumWindow {

  private final AnalyticExpression expression;
  private final List<Expression> partitionExpressions;
  private final List<OrderByElement> orderByElements;
  private final Expression argument;
  private final boolean distinct;
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
   * @param argument
   *          the argument expression evaluated by the SUM function
   * @param distinct
   *          whether DISTINCT or UNIQUE modifiers were present
   * @param windowElement
   *          the window frame specification associated with this SUM window
   */
  public SumWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
      List<OrderByElement> orderByElements, Expression argument, boolean distinct, WindowElement windowElement) {
    this.expression = expression;
    this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
    this.orderByElements = orderByElements == null ? List.of() : orderByElements;
    this.argument = argument;
    this.distinct = distinct;
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
   * Retrieve the argument expression evaluated by the SUM function.
   *
   * @return the {@link Expression} supplying the value to aggregate
   */
  public Expression argument() {
    return argument;
  }

  /**
   * Determine whether the window request asked for DISTINCT processing.
   *
   * @return {@code true} when DISTINCT or UNIQUE modifiers were present
   */
  public boolean distinct() {
    return distinct;
  }

  /**
   * Retrieve the window frame specification associated with this SUM window.
   *
   * @return the {@link WindowElement} describing the requested frame, or
   *         {@code null} when the default applies
   */
  public WindowElement windowElement() {
    return windowElement;
  }
}
