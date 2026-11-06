package se.alipsa.jparq.engine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
   *           if binary data or an interval is used implicitly as text, carrying
   *           SQLState 42000
   */
  public static String valueOf(Object value) {
    validateStringOperand(value);
    return value == null ? null : value.toString();
  }

  /**
   * Converts a value for an explicit character CAST, preserving SQL NULL. Binary
   * inputs are decoded as UTF-8.
   *
   * @param value
   *          SQL value to convert
   *
   * @return UTF-8 binary text, SQL interval text, or the normal character
   *         representation
   */
  public static String cast(Object value) {
    if (value instanceof byte[] bytes) {
      return new String(bytes, StandardCharsets.UTF_8);
    }
    if (value instanceof ByteBuffer buffer) {
      return StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
    }
    return value == null ? null : value.toString();
  }

  /**
   * Rejects binary and interval operands in operations that require character
   * values.
   *
   * @param value
   *          the value, including nested SQL arrays
   * @throws IllegalArgumentException
   *           if binary data or an interval is used implicitly as text, carrying
   *           SQLState 42000
   */
  private static void validateStringOperand(Object value) {
    if (value instanceof byte[] || value instanceof ByteBuffer) {
      throw SqlExceptions.syntaxFailure("Binary data requires an explicit character cast for text operations");
    }
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
