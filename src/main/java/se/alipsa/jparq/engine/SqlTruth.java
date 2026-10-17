package se.alipsa.jparq.engine;

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
   *          candidate was NULL)
   * @param negated
   *          whether the predicate was {@code NOT IN}
   * @return TRUE when a candidate matched, FALSE when nothing matched and no
   *         comparison was UNKNOWN, otherwise {@code null} for UNKNOWN
   */
  public static Boolean in(boolean found, boolean unknown, boolean negated) {
    if (found) {
      return !negated;
    }
    return unknown ? null : negated;
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
    try {
      return ExpressionEvaluator.typedCompare(left, right) != 0;
    } catch (RuntimeException e) {
      return true;
    }
  }
}
