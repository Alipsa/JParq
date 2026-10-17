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
}
