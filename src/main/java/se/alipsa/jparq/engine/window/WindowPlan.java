package se.alipsa.jparq.engine.window;

import java.util.List;
/**
 * Description of analytic window operations that must be computed prior to
 * projection evaluation.
 */
public final class WindowPlan {

  private final List<RowNumberWindow> rowNumberWindows;
  private final List<RankWindow> rankWindows;
  private final List<DenseRankWindow> denseRankWindows;
  private final List<PercentRankWindow> percentRankWindows;
  private final List<CumeDistWindow> cumeDistWindows;
  private final List<NtileWindow> ntileWindows;

  WindowPlan(List<RowNumberWindow> rowNumberWindows, List<RankWindow> rankWindows,
      List<DenseRankWindow> denseRankWindows, List<PercentRankWindow> percentRankWindows,
      List<CumeDistWindow> cumeDistWindows, List<NtileWindow> ntileWindows) {
    this.rowNumberWindows = rowNumberWindows == null ? List.of() : rowNumberWindows;
    this.rankWindows = rankWindows == null ? List.of() : rankWindows;
    this.denseRankWindows = denseRankWindows == null ? List.of() : denseRankWindows;
    this.percentRankWindows = percentRankWindows == null ? List.of() : percentRankWindows;
    this.cumeDistWindows = cumeDistWindows == null ? List.of() : cumeDistWindows;
    this.ntileWindows = ntileWindows == null ? List.of() : ntileWindows;
  }

  /**
   * Determine whether the plan contains any analytic window functions.
   *
   * @return {@code true} when the plan includes pre-computed window functions,
   *         otherwise {@code false}
   */
  public boolean isEmpty() {
    return rowNumberWindows.isEmpty() && rankWindows.isEmpty() && denseRankWindows.isEmpty()
        && percentRankWindows.isEmpty() && cumeDistWindows.isEmpty() && ntileWindows.isEmpty();
  }

  /**
   * Access the ROW_NUMBER windows captured by this plan.
   *
   * @return immutable list of {@link RowNumberWindow} instances
   */
  public List<RowNumberWindow> rowNumberWindows() {
    return rowNumberWindows;
  }

  /**
   * Access the RANK windows captured by this plan.
   *
   * @return immutable list of {@link RankWindow} instances
   */
  public List<RankWindow> rankWindows() {
    return rankWindows;
  }

  /**
   * Access the DENSE_RANK windows captured by this plan.
   *
   * @return immutable list of {@link DenseRankWindow} instances
   */
  public List<DenseRankWindow> denseRankWindows() {
    return denseRankWindows;
  }

  /**
   * Access the PERCENT_RANK windows captured by this plan.
   *
   * @return immutable list of {@link PercentRankWindow} instances
   */
  public List<PercentRankWindow> percentRankWindows() {
    return percentRankWindows;
  }

  /**
   * Access the CUME_DIST windows captured by this plan.
   *
   * @return immutable list of {@link CumeDistWindow} instances
   */
  public List<CumeDistWindow> cumeDistWindows() {
    return cumeDistWindows;
  }

  /**
   * Access the NTILE windows captured by this plan.
   *
   * @return immutable list of {@link NtileWindow} instances
   */
  public List<NtileWindow> ntileWindows() {
    return ntileWindows;
  }
}