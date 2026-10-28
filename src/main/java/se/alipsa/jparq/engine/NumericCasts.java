package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLDataException;

/**
 * Shares numeric conversion and range validation across CAST and JDBC getters.
 */
public final class NumericCasts {
  /** Prevents utility class instantiation. */
  private NumericCasts() {
  }

  /**
   * Converts inputs to a finite numeric target while rejecting overflow.
   *
   * @param value
   *          the input, possibly null; numbers, numeric text, and booleans are
   *          supported, and other objects are parsed from their string form
   * @param type
   *          the resolved numeric target
   * @param target
   *          the declared SQL target type for diagnostics
   * @return the converted number, truncating integer fractions toward zero, or
   *         null for null input
   * @throws IllegalArgumentException
   *           if the input is invalid (22018) or out of range (22003)
   */
  public static Number convert(Object value, NumericCastTarget type, String target) {
    if (value == null) {
      return null;
    }
    BigDecimal decimal = toBigDecimal(value);
    try {
      switch (type) {
        case REAL:
          float result = decimal.floatValue();
          if (!Float.isFinite(result)) {
            throw new ArithmeticException("Floating-point overflow");
          }
          return result;
        case DOUBLE:
          double doubleResult = decimal.doubleValue();
          if (!Double.isFinite(doubleResult)) {
            throw new ArithmeticException("Floating-point overflow");
          }
          return doubleResult;
        default:
          // Bound the integer digits before rescaling to avoid expanding extreme
          // exponents.
          long digits = (long) decimal.precision() - decimal.scale();
          if (digits > 19 && decimal.signum() != 0) {
            throw new ArithmeticException("Integer overflow");
          }
          BigDecimal integer = digits <= 0 ? BigDecimal.ZERO : decimal.setScale(0, RoundingMode.DOWN);
          return switch (type) {
            case TINYINT -> integer.byteValueExact();
            case SMALLINT -> integer.shortValueExact();
            case INTEGER -> integer.intValueExact();
            default -> integer.longValueExact();
          };
      }
    } catch (ArithmeticException exception) {
      String message = "Cannot cast numeric value '" + SqlExceptions.diagnosticValue(value) + "' to " + target
          + ": value out of range";
      throw new IllegalArgumentException(message, new SQLDataException(message, "22003", exception));
    }
  }

  /**
   * Converts a finite numeric input without leaking unclassified parse errors.
   *
   * @param value
   *          the input, possibly null; numbers, numeric text, and booleans are
   *          supported, and other objects are parsed from their string form
   * @return the decimal value, with booleans represented as zero or one, or null
   *         for null input
   * @throws IllegalArgumentException
   *           if conversion fails, carrying SQLState 22018
   */
  public static BigDecimal toBigDecimal(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof BigDecimal bd) {
      return bd;
    }
    if (value instanceof Boolean bool) {
      return bool ? BigDecimal.ONE : BigDecimal.ZERO;
    }
    try {
      return new BigDecimal(value.toString().trim());
    } catch (NumberFormatException exception) {
      String message = "Cannot cast value '" + SqlExceptions.diagnosticValue(value) + "' to a numeric value";
      throw new IllegalArgumentException(message, new SQLDataException(message, "22018", exception));
    }
  }

}
