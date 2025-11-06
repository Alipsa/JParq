package se.alipsa.jparq.engine.window;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

final class AvgComputationState {

  private BigDecimal sum = BigDecimal.ZERO;
  private long count;

  void add(Object value) {
    if (value == null) {
      return;
    }
    sum = sum.add(toBigDecimal(value));
    count++;
  }

  Object result() {
    if (count == 0L) {
      return null;
    }
    return sum.divide(BigDecimal.valueOf(count), MathContext.DECIMAL64).doubleValue();
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
        "AVG window functions require numeric inputs but received " + value.getClass().getName());
  }
}
