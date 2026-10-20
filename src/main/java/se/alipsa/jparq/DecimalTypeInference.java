package se.alipsa.jparq;

import java.math.BigDecimal;
import org.apache.avro.LogicalTypes;

/** Infers decimal bounds that accommodate every materialized value exactly. */
final class DecimalTypeInference {

  /** Prevents instantiation of this utility class. */
  private DecimalTypeInference() {
  }

  /**
   * Combines the largest integer-digit count with the largest nonnegative scale.
   *
   * @param values
   *          numeric values, allowing null elements or a null iterable
   * @return the decimal type, defaulting to precision one and scale zero without
   *         values
   * @throws NumberFormatException
   *           if a nonnull value cannot be represented as a decimal
   */
  static LogicalTypes.Decimal infer(Iterable<?> values) {
    int integerDigits = 0;
    int scale = 0;
    if (values != null) {
      for (Object value : values) {
        if (value == null) {
          continue;
        }
        BigDecimal decimal = value instanceof BigDecimal bd ? bd : new BigDecimal(value.toString());
        integerDigits = Math.max(integerDigits, decimal.precision() - decimal.scale());
        scale = Math.max(scale, Math.max(decimal.scale(), 0));
      }
    }
    return LogicalTypes.decimal(Math.max(1, integerDigits + scale), scale);
  }
}
