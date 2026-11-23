package se.alipsa.jparq.engine;

import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Utility methods for ORDER BY related comparisons and positional references.
 */
public final class OrderingUtil {

  private OrderingUtil() {
  }

  /**
   * Parses an ORDER BY positional reference, rejecting references outside 1
   * through the largest acceptable position with the JDBC syntax classification
   * instead of leaking a raw parse or arithmetic failure.
   *
   * @param literal
   *          the positional reference to parse
   * @param maximum
   *          the largest acceptable position, or a negative value when the output
   *          width is not known yet; positions above {@link Integer#MAX_VALUE}
   *          are rejected either way, because no select list can be that wide
   * @return the parsed position
   * @throws IllegalArgumentException
   *           if the reference does not name a position within 1 through the
   *           maximum, with SQLState 42000 naming the position as written
   */
  public static int position(String literal, int maximum) {
    long value;
    try {
      value = Long.parseLong(literal);
    } catch (NumberFormatException overflow) {
      throw invalidPosition(literal);
    }
    int upperBound = maximum < 0 ? Integer.MAX_VALUE : maximum;
    if (value < 1 || value > upperBound) {
      throw invalidPosition(literal);
    }
    return (int) value;
  }

  /**
   * Validates an already-parsed ORDER BY position against the largest acceptable
   * position.
   *
   * @param position
   *          the parsed position
   * @param maximum
   *          the largest acceptable position
   * @return the position
   * @throws IllegalArgumentException
   *           if the position is outside 1 through the maximum, with SQLState
   *           42000 naming the invalid position
   */
  public static int position(int position, int maximum) {
    if (position < 1 || position > maximum) {
      throw invalidPosition(position);
    }
    return position;
  }

  /**
   * Creates the syntax classification for an invalid ORDER BY position.
   *
   * @param position
   *          the invalid position to name in the message, as written when the
   *          original literal is available and as parsed otherwise
   * @return an engine exception carrying SQLState 42000
   */
  private static IllegalArgumentException invalidPosition(Object position) {
    return SqlExceptions.syntaxFailure("ORDER BY position is outside the select list: " + position);
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
