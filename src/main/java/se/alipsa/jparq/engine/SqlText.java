package se.alipsa.jparq.engine;

import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Separates explicit SQL text conversion from implicit string operand coercion.
 */
public final class SqlText {

  /** Prevents utility class instantiation. */
  private SqlText() {
  }

  /**
   * Converts a value to text while preserving SQL NULL.
   *
   * @param value
   *          the SQL value
   * @return its text, or null for SQL NULL
   * @throws IllegalArgumentException
   *           if an interval is used implicitly as text, carrying SQLState 42000
   */
  public static String valueOf(Object value) {
    validateStringOperand(value);
    return value == null ? null : value.toString();
  }

  /**
   * Converts a value for an explicit character CAST, preserving SQL NULL.
   *
   * @param value
   *          SQL value to convert
   * @return SQL interval text or the value's normal character representation
   */
  public static String cast(Object value) {
    return value == null ? null : value.toString();
  }

  /**
   * Rejects interval operands in operations that require character values.
   *
   * @param value
   *          the value, including nested SQL arrays
   * @throws IllegalArgumentException
   *           if an interval is used implicitly as text, carrying SQLState 42000
   */
  private static void validateStringOperand(Object value) {
    if (value instanceof TemporalInterval) {
      throw SqlExceptions.syntaxFailure("INTERVAL requires an explicit character cast for text operations");
    }
    if (value instanceof Object[] array) {
      for (Object element : array) {
        validateStringOperand(element);
      }
    } else if (value instanceof Iterable<?> elements) {
      for (Object element : elements) {
        validateStringOperand(element);
      }
    }
  }
}
