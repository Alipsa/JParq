package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * SQL value comparisons shared by the expression evaluators.
 *
 * <p>
 * Comparisons follow SQL-style type coercion rules. A {@code null} return value
 * represents SQL UNKNOWN, used both for NULL operands and for values that
 * cannot be compared (e.g. NaN).
 * </p>
 */
public final class SqlComparisons {

  private SqlComparisons() {
  }

  /**
   * Compare two values using SQL-style type coercion rules.
   *
   * @param l
   *          non-null left-hand value
   * @param r
   *          non-null right-hand value
   * @return negative when {@code l < r}, zero when equal, positive when greater
   * @throws IllegalArgumentException
   *           if the values cannot be compared, such as non-finite numbers
   * @throws NullPointerException
   *           if either value is null
   */
  public static int typedCompare(Object l, Object r) {
    if (l instanceof Number && r instanceof Number) {
      return new BigDecimal(l.toString()).compareTo(new BigDecimal(r.toString()));
    }
    if (l instanceof Boolean && r instanceof Boolean) {
      return Boolean.compare((Boolean) l, (Boolean) r);
    }
    if (l instanceof byte[] && r instanceof byte[]) {
      return compareBinary((byte[]) l, (byte[]) r);
    }
    if (l instanceof java.nio.ByteBuffer lb && r instanceof java.nio.ByteBuffer rb) {
      byte[] la = new byte[lb.remaining()];
      byte[] ra = new byte[rb.remaining()];
      lb.duplicate().get(la);
      rb.duplicate().get(ra);
      return compareBinary(la, ra);
    }
    if (l instanceof Timestamp && r instanceof Timestamp) {
      return ((Timestamp) l).compareTo((Timestamp) r);
    }
    if (l instanceof Date && r instanceof Date) {
      return Long.compare(((Date) l).getTime(), ((Date) r).getTime());
    }
    if (l instanceof se.alipsa.jparq.helper.TemporalInterval li
        && r instanceof se.alipsa.jparq.helper.TemporalInterval ri) {
      return li.compareTo(ri);
    }
    return l.toString().compareTo(r.toString());
  }

  /**
   * Compare two values, mapping SQL UNKNOWN to {@code null}.
   *
   * @param left
   *          left-hand value (may be {@code null})
   * @param right
   *          right-hand value (may be {@code null})
   * @return the comparison result, or {@code null} when either side is NULL or
   *         the values cannot be compared (e.g. NaN)
   */
  public static Integer compareOrUnknown(Object left, Object right) {
    if (left == null || right == null) {
      return null;
    }
    try {
      return typedCompare(left, right);
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Apply a comparison operator to a possibly UNKNOWN comparison result.
   *
   * @param operator
   *          textual representation of the operator ({@code =}, {@code <},
   *          {@code >}, {@code <=}, {@code >=}, {@code <>}, {@code !=})
   * @param cmp
   *          comparison result, or {@code null} for UNKNOWN
   * @return {@code true} or {@code false} when decisive, {@code null} for UNKNOWN
   * @throws IllegalArgumentException
   *           if a known comparison result has an unsupported operator
   */
  public static Boolean compare(String operator, Integer cmp) {
    if (cmp == null) {
      return null;
    }
    return switch (operator) {
      case "=" -> cmp == 0;
      case "<" -> cmp < 0;
      case ">" -> cmp > 0;
      case "<=" -> cmp <= 0;
      case ">=" -> cmp >= 0;
      case "<>", "!=" -> cmp != 0;
      default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
    };
  }

  /**
   * Compare two byte arrays using lexicographic ordering.
   *
   * @param left
   *          left-hand value, may be {@code null}
   * @param right
   *          right-hand value, may be {@code null}
   * @return negative when {@code left < right}, zero when equal, otherwise
   *         positive
   */
  private static int compareBinary(byte[] left, byte[] right) {
    if (left == right) {
      return 0;
    }
    if (left == null) {
      return -1;
    }
    if (right == null) {
      return 1;
    }
    return Arrays.compare(left, right);
  }
}
