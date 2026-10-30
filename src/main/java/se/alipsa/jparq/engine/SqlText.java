package se.alipsa.jparq.engine;

import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Converts SQL values to text without exposing internal temporal differences.
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
   *           if the value contains an internal temporal difference, carrying
   *           SQLState 0A000
   */
  public static String valueOf(Object value) {
    validateOutput(value);
    return value == null ? null : value.toString();
  }

  /**
   * Checks values before text conversion, JDBC access, or materialization.
   * Existing interval literals retain their behavior; differences and interval
   * arithmetic involving differences have no supported output representation.
   *
   * @param value
   *          the value, including nested SQL arrays
   * @throws IllegalArgumentException
   *           if an internal temporal difference would escape, carrying SQLState
   *           0A000
   */
  public static void validateOutput(Object value) {
    if (value instanceof TemporalInterval interval && interval.isTemporalDifference()) {
      throw SqlExceptions
          .unsupportedFeature("Unsupported temporal difference output: no supported JDBC or text representation");
    }
    if (value instanceof Object[] array) {
      for (Object element : array) {
        validateOutput(element);
      }
    } else if (value instanceof Iterable<?> elements) {
      for (Object element : elements) {
        validateOutput(element);
      }
    }
  }
}
