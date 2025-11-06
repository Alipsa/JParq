package se.alipsa.jparq.engine.window;

import java.math.BigDecimal;
import java.math.BigInteger;

final class SumComputationState {
  private BigDecimal sum = BigDecimal.ZERO;
  private boolean seenValue;
  private Class<?> observedType;

  void add(Object value) {
    if (value == null) {
      return;
    }
    if (!seenValue) {
      seenValue = true;
    }
    trackType(value);
    sum = sum.add(toBigDecimal(value));
  }

  Object result() {
    if (!seenValue) {
      return null;
    }
    if (observedType == BigDecimal.class) {
      return sum;
    }
    if (observedType == Byte.class || observedType == Short.class || observedType == Integer.class
        || observedType == Long.class) {
      return sum.longValue();
    }
    if (observedType == Float.class || observedType == Double.class) {
      return sum.doubleValue();
    }
    return sum.doubleValue();
  }

  private void trackType(Object value) {
    Class<?> valueClass = normalizeType(value.getClass());
    if (observedType == null) {
      observedType = valueClass;
      return;
    }
    observedType = widenType(observedType, valueClass);
  }

  private BigDecimal toBigDecimal(Object value) {
    if (value instanceof BigDecimal bd) {
      return bd;
    }
    if (value instanceof BigInteger bi) {
      return new BigDecimal(bi);
    }
    if (value instanceof Number number) {
      if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
        return BigDecimal.valueOf(number.longValue());
      }
      return BigDecimal.valueOf(number.doubleValue());
    }
    throw new IllegalArgumentException(
        "SUM window functions require numeric inputs but received " + value.getClass().getName());
  }

  private Class<?> normalizeType(Class<?> type) {
    if (type == BigInteger.class) {
      return BigDecimal.class;
    }
    return type;
  }

  private Class<?> widenType(Class<?> current, Class<?> candidate) {
    if (current == BigDecimal.class || candidate == BigDecimal.class) {
      return BigDecimal.class;
    }
    if (isFloatingPoint(current) || isFloatingPoint(candidate)) {
      return Double.class;
    }
    if (isIntegral(current) && isIntegral(candidate)) {
      return Long.class;
    }
    return Double.class;
  }

  private boolean isFloatingPoint(Class<?> type) {
    return type == Float.class || type == Double.class;
  }

  private boolean isIntegral(Class<?> type) {
    return type == Byte.class || type == Short.class || type == Integer.class || type == Long.class;
  }
}