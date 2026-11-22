package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.util.Locale;

/**
 * Shares the CAST AS BOOLEAN conversion between the CAST expression and the
 * JDBC boolean getters.
 */
public final class BooleanCasts {
  /** Prevents utility class instantiation. */
  private BooleanCasts() {
  }

  /**
   * Converts a value to a boolean using the engine's CAST AS BOOLEAN rules:
   * booleans pass through, numbers convert by their sign (non-finite
   * floating-point values are rejected), and text accepts 'true', 't', 'yes',
   * 'y', '1', 'false', 'f', 'no', 'n', and '0' ignoring case and leading or
   * trailing characters with code point U+0020 or below; blank text is false.
   *
   * @param value
   *          the input, possibly null
   * @return the converted boolean, or null for a null input
   * @throws IllegalArgumentException
   *           if the input cannot be converted or is a non-finite floating-point
   *           value, carrying SQLState 22018
   */
  public static Boolean toBoolean(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean bool) {
      return bool;
    }
    if (value instanceof Number number) {
      if (number instanceof BigDecimal decimal) {
        return decimal.signum() != 0;
      }
      if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long) {
        return number.longValue() != 0L;
      }
      double result = number.doubleValue();
      if ((number instanceof Float || number instanceof Double) && !Double.isFinite(result)) {
        String message = "Cannot cast value '" + SqlExceptions.diagnosticValue(value) + "' to BOOLEAN";
        throw new IllegalArgumentException(message, new SQLDataException(message, "22018"));
      }
      return result != 0d;
    }
    String text = SqlText.valueOf(value).trim();
    if (text.isEmpty()) {
      return false;
    }
    String normalized = text.toLowerCase(Locale.ROOT);
    if ("true".equals(normalized) || "t".equals(normalized) || "yes".equals(normalized) || "y".equals(normalized)
        || "1".equals(normalized)) {
      return true;
    }
    if ("false".equals(normalized) || "f".equals(normalized) || "no".equals(normalized) || "n".equals(normalized)
        || "0".equals(normalized)) {
      return false;
    }
    String message = "Cannot cast value '" + SqlExceptions.diagnosticValue(value) + "' to BOOLEAN";
    throw new IllegalArgumentException(message, new SQLDataException(message, "22018"));
  }
}
