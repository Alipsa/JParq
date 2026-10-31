package se.alipsa.jparq.engine.window;

import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Stateful helper used while evaluating MIN window frames.
 */
final class MinComputationState {

  private Object minValue;
  private boolean seenValue;

  /**
   * Incorporate a candidate value into the running minimum.
   *
   * @param value
   *          the value to consider; {@code null} values are ignored
   */
  void add(Object value) {
    if (value == null) {
      return;
    }
    if (!seenValue) {
      minValue = value;
      seenValue = true;
      return;
    }
    int comparison = WindowFunctions.compareValues(value, minValue);
    if (comparison < 0
        || (comparison == 0 && value instanceof TemporalInterval interval && interval.isTemporalDifference())) {
      minValue = value;
    }
  }

  /**
   * Retrieve the computed minimum value.
   *
   * @return the lowest non-null value encountered, or {@code null} when no
   *         non-null inputs were supplied
   */
  Object result() {
    return seenValue ? minValue : null;
  }
}
