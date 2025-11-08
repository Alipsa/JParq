package se.alipsa.jparq.engine.window;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects analytic window definitions discovered when scanning expressions for
 * planning.
 */
final class WindowCollector {

  private final List<RowNumberWindow> rowNumberWindows = new ArrayList<>();
  private final List<RankWindow> rankWindows = new ArrayList<>();
  private final List<DenseRankWindow> denseRankWindows = new ArrayList<>();
  private final List<PercentRankWindow> percentRankWindows = new ArrayList<>();
  private final List<CumeDistWindow> cumeDistWindows = new ArrayList<>();
  private final List<NtileWindow> ntileWindows = new ArrayList<>();
  private final List<CountWindow> countWindows = new ArrayList<>();
  private final List<SumWindow> sumWindows = new ArrayList<>();
  private final List<AvgWindow> avgWindows = new ArrayList<>();
  private final List<MinWindow> minWindows = new ArrayList<>();
  private final List<MaxWindow> maxWindows = new ArrayList<>();
  private final List<LagWindow> lagWindows = new ArrayList<>();
  private final List<LeadWindow> leadWindows = new ArrayList<>();

  /**
   * Create an immutable {@link WindowPlan} snapshot from the collected analytic
   * definitions.
   *
   * @return an immutable window plan capturing the accumulated analytic
   *         expressions
   */
  WindowPlan toWindowPlan() {
    return WindowPlan.builder().rowNumberWindows(List.copyOf(rowNumberWindows))
        .rankWindows(List.copyOf(rankWindows)).denseRankWindows(List.copyOf(denseRankWindows))
        .percentRankWindows(List.copyOf(percentRankWindows)).cumeDistWindows(List.copyOf(cumeDistWindows))
        .ntileWindows(List.copyOf(ntileWindows)).countWindows(List.copyOf(countWindows))
        .sumWindows(List.copyOf(sumWindows)).avgWindows(List.copyOf(avgWindows))
        .minWindows(List.copyOf(minWindows)).maxWindows(List.copyOf(maxWindows))
        .lagWindows(List.copyOf(lagWindows)).leadWindows(List.copyOf(leadWindows)).build();
  }

  /**
   * Determine whether any analytic expressions have been collected.
   *
   * @return {@code true} when no analytic expressions have been registered,
   *         otherwise {@code false}
   */
  boolean isEmpty() {
    return rowNumberWindows.isEmpty() && rankWindows.isEmpty() && denseRankWindows.isEmpty()
        && percentRankWindows.isEmpty() && cumeDistWindows.isEmpty() && ntileWindows.isEmpty()
        && countWindows.isEmpty() && sumWindows.isEmpty() && avgWindows.isEmpty()
        && minWindows.isEmpty() && maxWindows.isEmpty() && lagWindows.isEmpty() && leadWindows.isEmpty();
  }

  /**
   * Register a {@link RowNumberWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addRowNumberWindow(RowNumberWindow window) {
    rowNumberWindows.add(window);
  }

  /**
   * Register a {@link RankWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addRankWindow(RankWindow window) {
    rankWindows.add(window);
  }

  /**
   * Register a {@link DenseRankWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addDenseRankWindow(DenseRankWindow window) {
    denseRankWindows.add(window);
  }

  /**
   * Register a {@link PercentRankWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addPercentRankWindow(PercentRankWindow window) {
    percentRankWindows.add(window);
  }

  /**
   * Register a {@link CumeDistWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addCumeDistWindow(CumeDistWindow window) {
    cumeDistWindows.add(window);
  }

  /**
   * Register a {@link NtileWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addNtileWindow(NtileWindow window) {
    ntileWindows.add(window);
  }

  /**
   * Register a {@link CountWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addCountWindow(CountWindow window) {
    countWindows.add(window);
  }

  /**
   * Register a {@link SumWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addSumWindow(SumWindow window) {
    sumWindows.add(window);
  }

  /**
   * Register an {@link AvgWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addAvgWindow(AvgWindow window) {
    avgWindows.add(window);
  }

  /**
   * Register a {@link MinWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addMinWindow(MinWindow window) {
    minWindows.add(window);
  }

  /**
   * Register a {@link MaxWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addMaxWindow(MaxWindow window) {
    maxWindows.add(window);
  }

  /**
   * Register a {@link LagWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addLagWindow(LagWindow window) {
    lagWindows.add(window);
  }

  /**
   * Register a {@link LeadWindow} for later inclusion in a plan snapshot.
   *
   * @param window
   *          the window definition to retain
   */
  void addLeadWindow(LeadWindow window) {
    leadWindows.add(window);
  }
}
