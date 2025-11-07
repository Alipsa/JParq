package se.alipsa.jparq.engine.window;

/**
 * Stateful helper used while evaluating MAX window frames.
 */
final class MaxComputationState {

  private Object maxValue;
  private boolean seenValue;

  /**
   * Incorporate a candidate value into the running maximum.
   *
   * @param value
   *          the value to consider; {@code null} values are ignored
   */
  void add(Object value) {
    if (value == null) {
      return;
    }
    if (!seenValue) {
      maxValue = value;
      seenValue = true;
      return;
    }
    if (WindowFunctions.compareValues(value, maxValue) > 0) {
      maxValue = value;
    }
  }

  /**
   * Retrieve the computed maximum value.
   *
   * @return the highest non-null value encountered, or {@code null} when no
   *         non-null inputs were supplied
   */
  Object result() {
    return seenValue ? maxValue : null;
  }
}
