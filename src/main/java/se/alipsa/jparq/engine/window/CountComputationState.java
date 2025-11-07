package se.alipsa.jparq.engine.window;

/**
 * Mutable container that keeps track of COUNT window aggregation state.
 */
final class CountComputationState {

  private final boolean countStar;
  private long count;

  /**
   * Create a state holder for COUNT window aggregation.
   *
   * @param countStar
   *          {@code true} when evaluating COUNT(*) and {@code false} when
   *          counting an expression
   */
  CountComputationState(boolean countStar) {
    this.countStar = countStar;
  }

  /**
   * Incorporate a new value into the running count.
   *
   * @param value
   *          the evaluated expression value, ignored when {@code null}
   */
  void add(Object value) {
    if (countStar) {
      count++;
    } else if (value != null) {
      count++;
    }
  }

  /**
   * Retrieve the number of values observed so far.
   *
   * @return the computed count
   */
  long result() {
    return count;
  }
}
