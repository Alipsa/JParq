package se.alipsa.jparq.engine;

import java.io.Serializable;
import java.math.BigInteger;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.io.api.Binary;

/**
 * Compares binary decimal storage numerically, including negative
 * two's-complement values.
 */
final class DecimalBinaryPredicate extends UserDefinedPredicate<Binary> implements Serializable {
  private static final long serialVersionUID = 1L;
  private final BigInteger bound;
  private final String operator;

  /** Creates a comparison in the column's exact unscaled units. */
  DecimalBinaryPredicate(BigInteger bound, String operator) {
    this.bound = bound;
    this.operator = operator;
  }

  @Override
  public boolean keep(Binary value) {
    return value != null && matches(new BigInteger(value.getBytes()).compareTo(bound));
  }

  @Override
  public boolean canDrop(Statistics<Binary> statistics) {
    if (statistics.getMin() == null || statistics.getMax() == null) {
      return false;
    }
    int min = new BigInteger(statistics.getMin().getBytes()).compareTo(bound);
    int max = new BigInteger(statistics.getMax().getBytes()).compareTo(bound);
    return switch (operator) {
      case "=" -> min > 0 || max < 0;
      case "<>", "!=" -> min == 0 && max == 0;
      case "<" -> min >= 0;
      case "<=" -> min > 0;
      case ">" -> max <= 0;
      case ">=" -> max < 0;
      default -> false;
    };
  }

  @Override
  public boolean inverseCanDrop(Statistics<Binary> statistics) {
    // The planner never emits logical inversion of this predicate.
    return false;
  }

  /** Applies the SQL operator to a numeric comparison result. */
  private boolean matches(int comparison) {
    return switch (operator) {
      case "=" -> comparison == 0;
      case "<>", "!=" -> comparison != 0;
      case "<" -> comparison < 0;
      case "<=" -> comparison <= 0;
      case ">" -> comparison > 0;
      case ">=" -> comparison >= 0;
      default -> false;
    };
  }

  @Override
  public String toString() {
    return "decimal " + operator + " " + bound;
  }
}
