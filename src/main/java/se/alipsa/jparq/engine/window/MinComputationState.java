package se.alipsa.jparq.engine.window;

/**
 * Stateful helper used while evaluating MIN window frames.
 */
final class MinComputationState {

  private Object minValue;
  private boolean seenValue;

  /**
   * Incorporate a candidate value into the running minimum.
   *
   * @param value the value to consider; {@code null} values are ignored
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
    if (WindowFunctions.compareValues(value, minValue) < 0) {
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
