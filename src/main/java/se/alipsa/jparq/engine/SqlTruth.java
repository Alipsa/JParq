package se.alipsa.jparq.engine;

import java.util.Objects;

/** SQL Boolean operations, using null to represent UNKNOWN. */
public final class SqlTruth {
  private SqlTruth() {
  }

  /**
   * Negates a SQL truth value.
   * 
   * @param value
   *          the operand, or null for UNKNOWN
   * @return its negation, or null for UNKNOWN
   */
  public static Boolean not(Boolean value) {
    return value == null ? null : !value;
  }

  /**
   * Tests a truth value with SQL {@code IS [NOT] TRUE/FALSE} semantics.
   *
   * @param truth
   *          the operand, or null for UNKNOWN
   * @param target
   *          whether the target is TRUE rather than FALSE
   * @param negated
   *          whether the predicate includes NOT
   * @return whether the operand matches the target, inverted when negated; never
   *         UNKNOWN
   */
  public static boolean is(Boolean truth, boolean target, boolean negated) {
    return negated != Boolean.valueOf(target).equals(truth);
  }

  /**
   * Computes SQL conjunction.
   * 
   * @param left
   *          the left operand
   * @param right
   *          the right operand
   * @return FALSE if either is FALSE, otherwise UNKNOWN if either is UNKNOWN,
   *         otherwise TRUE
   */
  public static Boolean and(Boolean left, Boolean right) {
    if (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) {
      return false;
    }
    return left == null || right == null ? null : Boolean.TRUE;
  }

  /**
   * Computes SQL disjunction.
   * 
   * @param left
   *          the left operand
   * @param right
   *          the right operand
   * @return TRUE if either is TRUE, otherwise UNKNOWN if either is UNKNOWN,
   *         otherwise FALSE
   */
  public static Boolean or(Boolean left, Boolean right) {
    if (Boolean.TRUE.equals(left) || Boolean.TRUE.equals(right)) {
      return true;
    }
    return left == null || right == null ? null : Boolean.FALSE;
  }

  /**
   * Computes the SQL {@code [NOT] IN} result from the outcome of comparing the
   * left value against every candidate.
   *
   * @param found
   *          whether any candidate equaled the left value
   * @param unknown
   *          whether any candidate comparison was UNKNOWN (the left value or a
   *          candidate was NULL or could not be compared)
   * @param negated
   *          whether the predicate was {@code NOT IN}
   * @return the membership result, inverted when negated; {@code null} when
   *         nothing matched and a comparison was UNKNOWN
   */
  public static Boolean in(boolean found, boolean unknown, boolean negated) {
    if (found) {
      return !negated;
    }
    return unknown ? null : negated;
  }

  /**
   * Computes the SQL {@code [NOT] IN} result by comparing the left value against
   * every candidate.
   *
   * @param leftValue
   *          the value to match (may be {@code null})
   * @param candidates
   *          the candidate values, already coerced to the left value's type
   * @param negated
   *          whether the predicate was {@code NOT IN}
   * @return the membership result, inverted when negated; UNKNOWN when no
   *         candidate matches and a comparison is UNKNOWN. An empty collection
   *         yields FALSE for IN and TRUE for NOT IN, even for a NULL left value
   */
  public static Boolean in(Object leftValue, Iterable<?> candidates, boolean negated) {
    boolean found = false;
    boolean unknown = false;
    for (Object candidate : candidates) {
      Integer cmp = SqlComparisons.compareOrUnknown(leftValue, candidate);
      if (cmp == null) {
        unknown = true;
        continue;
      }
      if (cmp == 0) {
        found = true;
        break;
      }
    }
    return in(found, unknown, negated);
  }

  /**
   * Determines whether two values are SQL DISTINCT.
   *
   * @param left
   *          the left value (may be {@code null})
   * @param right
   *          the right value (may be {@code null})
   * @return {@code true} when exactly one value is {@code null} or the values
   *         compare as unequal, including when they cannot be compared
   */
  public static boolean isDistinctFrom(Object left, Object right) {
    if (left == null || right == null) {
      return left != right;
    }
    if (Objects.equals(left, right)) {
      return false;
    }
    Integer cmp = SqlComparisons.compareOrUnknown(left, right);
    return cmp == null || cmp != 0;
  }
}
