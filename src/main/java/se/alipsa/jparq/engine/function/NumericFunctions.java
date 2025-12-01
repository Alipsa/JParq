package se.alipsa.jparq.engine.function;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;

/**
 * The numeric functions that are supported.
 */
public class NumericFunctions {

  private NumericFunctions() {
    // Utility class
  }
  /**
   * Perform a numeric function.
   *
   * @param name
   *          The name of the function
   * @param args
   *          The arguments of the function
   * @return The result of the function
   */
  public static Object evaluate(String name, List<Object> args) {
    return switch (name) {
      case "ABS" -> unaryBigDecimal(args, BigDecimal::abs);
      case "CEIL", "CEILING" -> unaryBigDecimal(args, v -> v.setScale(0, RoundingMode.CEILING));
      case "FLOOR" -> unaryBigDecimal(args, v -> v.setScale(0, RoundingMode.FLOOR));
      case "ROUND" -> roundFunction(args);
      case "SQRT" -> sqrtFunction(args);
      case "TRUNC", "TRUNCATE" -> truncateFunction(args);
      case "MOD" -> modFunction(args);
      case "POWER", "POW" -> powerFunction(args);
      case "EXP" -> expFunction(args);
      case "LOG" -> logFunction(args);
      case "LOG10" -> log10Function(args);
      case "RAND", "RANDOM" -> randFunction(args);
      case "SIGN" -> signFunction(args);
      case "SIN" -> trigFunction(args, Math::sin);
      case "COS" -> trigFunction(args, Math::cos);
      case "TAN" -> trigFunction(args, Math::tan);
      case "COT" -> cotFunction(args);
      case "ASIN" -> inverseTrigFunction(args, Math::asin);
      case "ACOS" -> inverseTrigFunction(args, Math::acos);
      case "ATAN" -> inverseTrigFunction(args, Math::atan);
      case "ATAN2" -> atan2Function(args);
      case "DEGREES" -> trigFunction(args, Math::toDegrees);
      case "RADIANS" -> trigFunction(args, Math::toRadians);
      case "PI" -> Math.PI;
      default -> null;
    };
  }

  private static Object unaryBigDecimal(List<Object> args, java.util.function.Function<BigDecimal, BigDecimal> op) {
    if (args.isEmpty()) {
      return null;
    }
    Object value = args.getFirst();
    if (value == null) {
      return null;
    }
    BigDecimal number = toBigDecimal(value);
    return op.apply(number);
  }

  /**
   * Convert a value to a {@link BigDecimal}.
   *
   * @param value
   *          the value to convert
   * @return a BigDecimal representation of the value or null if the value was
   *         null
   */
  public static BigDecimal toBigDecimal(Object value) {
    return switch (value) {
      case null -> null;
      case BigDecimal bd -> bd;
      case Number num -> {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
          yield BigDecimal.valueOf(num.longValue());
        }
        if (value instanceof BigInteger bi) {
          yield new BigDecimal(bi);
        }
        yield BigDecimal.valueOf(num.doubleValue());
      }
      default -> new BigDecimal(value.toString());
    };
  }

  private static Object roundFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Object value = args.getFirst();
    if (value == null) {
      return null;
    }
    BigDecimal number = toBigDecimal(value);
    int scale = 0;
    if (args.size() > 1) {
      Integer argScale = toInteger(args.get(1));
      if (argScale == null) {
        return null;
      }
      scale = argScale;
    }
    if (scale >= 0) {
      return number.setScale(scale, RoundingMode.HALF_UP);
    }
    BigDecimal factor = BigDecimal.TEN.pow(-scale);
    BigDecimal divided = number.divide(factor, 0, RoundingMode.HALF_UP);
    return divided.multiply(factor);
  }

  private static Object truncateFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Object value = args.getFirst();
    if (value == null) {
      return null;
    }
    BigDecimal number = toBigDecimal(value);
    int scale = 0;
    if (args.size() > 1) {
      Integer argScale = toInteger(args.get(1));
      if (argScale == null) {
        return null;
      }
      scale = argScale;
    }
    if (scale >= 0) {
      return number.setScale(scale, RoundingMode.DOWN);
    }
    BigDecimal factor = BigDecimal.TEN.pow(-scale);
    BigDecimal divided = number.divide(factor, 0, RoundingMode.DOWN);
    return divided.multiply(factor);
  }

  private static Object sqrtFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null || value < 0d) {
      return null;
    }
    return Math.sqrt(value);
  }

  private static Object modFunction(List<Object> args) {
    if (args.size() < 2) {
      return null;
    }
    Object left = args.get(0);
    Object right = args.get(1);
    if (left == null || right == null) {
      return null;
    }
    BigDecimal dividend = toBigDecimal(left);
    BigDecimal divisor = toBigDecimal(right);
    if (divisor.compareTo(BigDecimal.ZERO) == 0) {
      return null;
    }
    return dividend.remainder(divisor);
  }

  private static Object powerFunction(List<Object> args) {
    if (args.size() < 2) {
      return null;
    }
    Double base = toDouble(args.get(0));
    Double exponent = toDouble(args.get(1));
    if (base == null || exponent == null) {
      return null;
    }
    return Math.pow(base, exponent);
  }

  private static Object expFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null) {
      return null;
    }
    return Math.exp(value);
  }

  private static Object logFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Double first = toDouble(args.getFirst());
    if (first == null || first <= 0d) {
      return null;
    }
    if (args.size() == 1) {
      return Math.log(first);
    }
    Double base = first;
    Double value = toDouble(args.get(1));
    if (value == null || value <= 0d || base <= 0d || base.equals(1d)) {
      return null;
    }
    return Math.log(value) / Math.log(base);
  }

  private static Object log10Function(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null || value <= 0d) {
      return null;
    }
    return Math.log10(value);
  }

  private static Object randFunction(List<Object> args) {
    if (args.isEmpty()) {
      return Math.random();
    }
    Long seed = toLong(args.getFirst());
    if (seed == null) {
      return null;
    }
    return new Random(seed).nextDouble();
  }

  private static Object signFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Object value = args.getFirst();
    if (value == null) {
      return null;
    }
    BigDecimal number = toBigDecimal(value);
    return Integer.valueOf(number.signum());
  }

  private static Object trigFunction(List<Object> args, java.util.function.DoubleUnaryOperator operator) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null) {
      return null;
    }
    return operator.applyAsDouble(value);
  }

  private static Object cotFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null) {
      return null;
    }
    double tangent = Math.tan(value);
    if (Double.isNaN(tangent) || tangent == 0.0) {
      return null;
    }
    return 1d / tangent;
  }

  private static Object inverseTrigFunction(List<Object> args, java.util.function.DoubleUnaryOperator operator) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null) {
      return null;
    }
    return operator.applyAsDouble(value);
  }

  private static Object atan2Function(List<Object> args) {
    if (args.size() < 2) {
      return null;
    }
    Double y = toDouble(args.get(0));
    Double x = toDouble(args.get(1));
    if (y == null || x == null) {
      return null;
    }
    return Math.atan2(y, x);
  }

  private static Long toLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number num) {
      return num.longValue();
    }
    try {
      return new BigDecimal(value.toString()).longValue();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Expected numeric value but got " + value, e);
    }
  }

  private static Double toDouble(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number num) {
      return num.doubleValue();
    }
    try {
      return new BigDecimal(value.toString()).doubleValue();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Expected numeric value but got " + value, e);
    }
  }

  /**
   * Convert an Object to an Integer.
   *
   * @param value
   *          the object to convert
   * @return the Integer equivalent.
   */
  public static Integer toInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number num) {
      return num.intValue();
    }
    try {
      return new BigDecimal(value.toString()).intValue();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Expected numeric value but got " + value, e);
    }
  }
}
