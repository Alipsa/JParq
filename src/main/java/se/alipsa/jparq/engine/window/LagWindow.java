package se.alipsa.jparq.engine.window;

import java.util.List;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Representation of a LAG analytic expression.
 */
public final class LagWindow {

  private final AnalyticExpression expression;
  private final List<Expression> partitionExpressions;
  private final List<OrderByElement> orderByElements;
  private final Expression valueExpression;
  private final Expression offsetExpression;
  private final Expression defaultExpression;

  LagWindow(AnalyticExpression expression, List<Expression> partitionExpressions, List<OrderByElement> orderByElements,
      Expression valueExpression, Expression offsetExpression, Expression defaultExpression) {
    this.expression = expression;
    this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
    this.orderByElements = orderByElements == null ? List.of() : orderByElements;
    this.valueExpression = valueExpression;
    this.offsetExpression = offsetExpression;
    this.defaultExpression = defaultExpression;
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
   * Retrieve the value expression evaluated by the LAG function.
   *
   * @return the expression supplying the value to retrieve from previous rows
   */
  public Expression valueExpression() {
    return valueExpression;
  }

  /**
   * Retrieve the offset expression evaluated by the LAG function.
   *
   * @return the expression describing how many rows to look back (may be
   *         {@code null})
   */
  public Expression offsetExpression() {
    return offsetExpression;
  }

  /**
   * Retrieve the default expression evaluated when the offset is outside the
   * partition.
   *
   * @return the default expression or {@code null} when none was provided
   */
  public Expression defaultExpression() {
    return defaultExpression;
  }
}
