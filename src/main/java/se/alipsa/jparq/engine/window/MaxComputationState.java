package se.alipsa.jparq.engine.window;

import se.alipsa.jparq.helper.TemporalInterval;

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
    int comparison = WindowFunctions.compareValues(value, maxValue);
    if (comparison > 0
        || (comparison == 0 && value instanceof TemporalInterval interval && interval.isTemporalDifference())) {
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
