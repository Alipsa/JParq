package se.alipsa.jparq.engine;

import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Utility methods for ORDER BY related comparisons.
 */
public final class OrderingUtil {

  private OrderingUtil() {
  }

  /**
   * Compare two values with SQL-style null ordering semantics.
   *
   * @param left
   *          the left-hand value, may be {@code null}
   * @param right
   *          the right-hand value, may be {@code null}
   * @param asc
   *          {@code true} when ascending order is requested
   * @param nullOrdering
   *          explicit null ordering directive, may be {@code null}
   * @return negative if {@code left < right}, zero if equal, positive otherwise
   */
  public static int compareNulls(Object left, Object right, boolean asc, OrderByElement.NullOrdering nullOrdering) {
    if (left == right) {
      return 0;
    }
    if (left != null && right != null) {
      return 0;
    }
    if (nullOrdering == OrderByElement.NullOrdering.NULLS_FIRST) {
      return left == null ? -1 : 1;
    }
    if (nullOrdering == OrderByElement.NullOrdering.NULLS_LAST) {
      return left == null ? 1 : -1;
    }
    if (asc) {
      return left == null ? 1 : -1;
    }
    return left == null ? -1 : 1;
  }
}
