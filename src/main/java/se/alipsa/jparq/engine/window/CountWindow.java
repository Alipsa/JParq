package se.alipsa.jparq.engine.window;

import java.util.List;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Representation of a COUNT analytic expression.
 */
public final class CountWindow {

  private final AnalyticExpression expression;
  private final List<Expression> partitionExpressions;
  private final List<OrderByElement> orderByElements;
  private final Expression argument;
  private final boolean countStar;
  private final WindowElement windowElement;

  CountWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
      List<OrderByElement> orderByElements, Expression argument, boolean countStar, WindowElement windowElement) {
    this.expression = expression;
    this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
    this.orderByElements = orderByElements == null ? List.of() : orderByElements;
    this.argument = argument;
    this.countStar = countStar;
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
   * Retrieve the argument expression evaluated by the COUNT function.
   *
   * @return the {@link Expression} supplying the value to aggregate, or
   *         {@code null} when COUNT(*)
   */
  public Expression argument() {
    return argument;
  }

  /**
   * Determine whether the COUNT expression represents COUNT(*).
   *
   * @return {@code true} when the analytic expression is COUNT(*)
   */
  public boolean countStar() {
    return countStar;
  }

  /**
   * Retrieve the window frame specification associated with this COUNT window.
   *
   * @return the {@link WindowElement} describing the requested frame, or
   *         {@code null} when the default applies
   */
  public WindowElement windowElement() {
    return windowElement;
  }
}
