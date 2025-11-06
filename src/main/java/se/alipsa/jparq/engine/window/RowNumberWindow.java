package se.alipsa.jparq.engine.window;

import java.util.List;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Representation of a ROW_NUMBER analytic expression.
 */
public final class RowNumberWindow {

  private final AnalyticExpression expression;
  private final List<Expression> partitionExpressions;
  private final List<OrderByElement> orderByElements;

  RowNumberWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
      List<OrderByElement> orderByElements) {
    this.expression = expression;
    this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
    this.orderByElements = orderByElements == null ? List.of() : orderByElements;
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
}