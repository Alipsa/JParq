package se.alipsa.jparq.engine.function;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Helpers for ARRAY functions and constructors.
 */
public final class ArrayFunctions {

  private ArrayFunctions() {
  }

  /**
   * Normalize ARRAY element types so that a single SQL ARRAY has a consistent
   * element type.
   *
   * @param values
   *          raw values
   * @return homogenized list or {@code List.of()} when {@code values} is null or
   *         empty
   */
  public static List<Object> homogenizeArrayValues(List<Object> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    NumericType numericTarget = NumericType.NONE;
    Class<?> objectTarget = null;
    for (Object value : values) {
      if (value == null) {
        continue;
      }
      if (value instanceof Number number) {
        numericTarget = NumericType.promote(numericTarget, NumericType.of(number));
        continue;
      }
      if (value instanceof CharSequence) {
        objectTarget = determineObjectTarget(objectTarget, String.class);
        continue;
      }
      if (value instanceof List<?>) {
        objectTarget = determineObjectTarget(objectTarget, List.class);
        continue;
      }
      objectTarget = determineObjectTarget(objectTarget, value.getClass());
    }
    if (numericTarget != NumericType.NONE) {
      return convertNumericValues(values, numericTarget);
    }
    if (objectTarget == null) {
      return Collections.unmodifiableList(new ArrayList<>(values));
    }
    List<Object> coerced = new ArrayList<>(values.size());
    for (Object value : values) {
      if (value == null) {
        coerced.add(null);
        continue;
      }
      if (objectTarget == String.class) {
        coerced.add(value.toString());
        continue;
      }
      if (objectTarget == List.class) {
        if (value instanceof List<?>) {
          coerced.add(value);
          continue;
        }
        throw new IllegalArgumentException("ARRAY elements must all be lists or scalars of the same type");
      }
      if (!objectTarget.equals(value.getClass())) {
        throw new IllegalArgumentException("ARRAY elements must share a common type; found " + objectTarget.getName()
            + " and " + value.getClass().getName());
      }
      coerced.add(value);
    }
    return Collections.unmodifiableList(coerced);
  }

  private static Class<?> determineObjectTarget(Class<?> current, Class<?> candidate) {
    if (current == null || current == candidate) {
      return candidate;
    }
    throw new IllegalArgumentException(
        "ARRAY elements must share a common type; found " + current.getName() + " and " + candidate.getName());
  }

  private static List<Object> convertNumericValues(List<Object> values, NumericType target) {
    List<Object> converted = new ArrayList<>(values.size());
    for (Object value : values) {
      if (value == null) {
        converted.add(null);
        continue;
      }
      Number number = (Number) value;
      converted.add(switch (target) {
        case BIG_DECIMAL -> NumericFunctions.toBigDecimal(number);
        case DOUBLE -> number.doubleValue();
        case FLOAT -> number.floatValue();
        case LONG -> number.longValue();
        case INTEGER -> number.intValue();
        case SHORT -> number.shortValue();
        case BYTE -> number.byteValue();
        case NONE -> number;
      });
    }
    return Collections.unmodifiableList(converted);
  }

  private enum NumericType {
    NONE, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BIG_DECIMAL;

    static NumericType of(Number value) {
      if (value instanceof BigDecimal || value instanceof java.math.BigInteger) {
        return BIG_DECIMAL;
      }
      String className = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);
      return switch (className) {
        case "double" -> DOUBLE;
        case "float" -> FLOAT;
        case "long" -> LONG;
        case "integer", "int" -> INTEGER;
        case "short" -> SHORT;
        case "byte" -> BYTE;
        default -> DOUBLE;
      };
    }

    static NumericType promote(NumericType current, NumericType candidate) {
      if (candidate == null || candidate == NONE) {
        return current;
      }
      if (current == NONE) {
        return candidate;
      }
      return values()[Math.max(current.ordinal(), candidate.ordinal())];
    }
  }
}
