package se.alipsa.jparq.engine;

import static se.alipsa.jparq.engine.SqlExceptions.syntaxFailure;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * SQL value comparisons shared by the expression evaluators.
 *
 * <p>
 * Comparisons follow SQL-style type coercion rules. A {@code null} return value
 * represents SQL UNKNOWN, used for NULL operands and for values that cannot be
 * compared. Floating-point values use one total order, matching PostgreSQL and
 * DuckDB: {@code -Infinity < every finite value < +Infinity < NaN}, where NaN
 * equals NaN and {@code -0.0} equals {@code 0.0}.
 * </p>
 */
public final class SqlComparisons {
  private static final Set<String> SUPPORTED_OPERATORS = Set.of("=", "<", ">", "<=", ">=", "<>", "!=");
  private static final int RANK_NEGATIVE_INFINITY = 0;
  private static final int RANK_FINITE = 1;
  private static final int RANK_POSITIVE_INFINITY = 2;
  private static final int RANK_NAN = 3;

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
   * @throws NullPointerException
   *           if either value is null
   * @throws IllegalArgumentException
   *           if a number is compared with an interval; the cause carries
   *           SQLState 42000
   */
  public static int typedCompare(Object l, Object r) {
    if (l instanceof Number ln && r instanceof Number rn) {
      return compareNumbers(ln, rn);
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
    if (l instanceof TemporalInterval li && r instanceof TemporalInterval ri) {
      return li.compareTo(ri);
    }
    if (l instanceof Number && r instanceof TemporalInterval || l instanceof TemporalInterval && r instanceof Number) {
      throw syntaxFailure(
          "Cannot compare a numeric day count with INTERVAL; compare DATE differences with numeric days");
    }
    return l.toString().compareTo(r.toString());
  }

  /**
   * Compare two numbers using the total order shared by the Java engine and
   * Parquet pushdown: {@code -Infinity < finite values < +Infinity < NaN}. NaN
   * equals NaN and {@code -0.0} equals {@code 0.0}. Finite values are compared
   * exactly as decimals.
   *
   * @param left
   *          left-hand number
   * @param right
   *          right-hand number
   * @return negative when {@code left < right}, zero when equal, positive when
   *         greater
   * @throws NullPointerException
   *           if either number is null
   */
  public static int compareNumbers(Number left, Number right) {
    int leftRank = rank(left);
    int rightRank = rank(right);
    if (leftRank != RANK_FINITE || rightRank != RANK_FINITE) {
      return Integer.compare(leftRank, rightRank);
    }
    return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString()));
  }

  /**
   * Classify a number as negative infinity, finite, positive infinity or NaN.
   *
   * @param number
   *          the number to classify
   * @return the rank of the number in the floating-point order
   */
  private static int rank(Number number) {
    if (!(number instanceof Double || number instanceof Float)) {
      return RANK_FINITE;
    }
    double value = number.doubleValue();
    if (Double.isNaN(value)) {
      return RANK_NAN;
    }
    if (Double.isInfinite(value)) {
      return value > 0 ? RANK_POSITIVE_INFINITY : RANK_NEGATIVE_INFINITY;
    }
    return RANK_FINITE;
  }

  /**
   * Compare two values, mapping SQL UNKNOWN to {@code null}.
   *
   * @param left
   *          left-hand value (may be {@code null})
   * @param right
   *          right-hand value (may be {@code null})
   * @return the comparison result, or {@code null} when either side is NULL or
   *         the values cannot be compared
   * @throws IllegalArgumentException
   *           if a number is compared with an interval; the cause carries
   *           SQLState 42000
   */
  public static Integer compareOrUnknown(Object left, Object right) {
    if (left == null || right == null) {
      return null;
    }
    try {
      return typedCompare(left, right);
    } catch (RuntimeException e) {
      if (SqlExceptions.findSqlStateFailure(e) != null) {
        throw e;
      }
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
   *           if the operator is unsupported, even for an UNKNOWN result
   */
  public static Boolean compare(String operator, Integer cmp) {
    if (!SUPPORTED_OPERATORS.contains(operator)) {
      throw new IllegalArgumentException("Unsupported operator: " + operator);
    }
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
