package se.alipsa.jparq.engine.window;

import java.util.ArrayList;
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
  private final List<CountWindow> countWindows;
  private final List<SumWindow> sumWindows;
  private final List<AvgWindow> avgWindows;
  private final List<MinWindow> minWindows;
  private final List<MaxWindow> maxWindows;
  private final List<LagWindow> lagWindows;
  private final List<LeadWindow> leadWindows;

  private WindowPlan(Builder builder) {
    this.rowNumberWindows = immutableList(builder.rowNumberWindows);
    this.rankWindows = immutableList(builder.rankWindows);
    this.denseRankWindows = immutableList(builder.denseRankWindows);
    this.percentRankWindows = immutableList(builder.percentRankWindows);
    this.cumeDistWindows = immutableList(builder.cumeDistWindows);
    this.ntileWindows = immutableList(builder.ntileWindows);
    this.countWindows = immutableList(builder.countWindows);
    this.sumWindows = immutableList(builder.sumWindows);
    this.avgWindows = immutableList(builder.avgWindows);
    this.minWindows = immutableList(builder.minWindows);
    this.maxWindows = immutableList(builder.maxWindows);
    this.lagWindows = immutableList(builder.lagWindows);
    this.leadWindows = immutableList(builder.leadWindows);
  }

  /**
   * Create an immutable view of the supplied list.
   *
   * @param <T>
   *          element type contained within the list
   * @param source
   *          the list to copy, may be {@code null}
   * @return an immutable list containing the supplied elements or an empty list
   *         when {@code source} is {@code null}
   */
  private static <T> List<T> immutableList(List<T> source) {
    return source == null ? List.of() : List.copyOf(source);
  }

  /**
   * Create a builder for assembling immutable {@link WindowPlan} instances.
   *
   * @return a new builder ready to accept analytic window collections
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Determine whether the plan contains any analytic window functions.
   *
   * @return {@code true} when the plan includes pre-computed window functions,
   *         otherwise {@code false}
   */
  public boolean isEmpty() {
    return rowNumberWindows.isEmpty() && rankWindows.isEmpty() && denseRankWindows.isEmpty()
        && percentRankWindows.isEmpty() && cumeDistWindows.isEmpty() && ntileWindows.isEmpty() && countWindows.isEmpty()
        && sumWindows.isEmpty() && avgWindows.isEmpty() && minWindows.isEmpty() && maxWindows.isEmpty()
        && lagWindows.isEmpty() && leadWindows.isEmpty();
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

  /**
   * Access the COUNT windows captured by this plan.
   *
   * @return immutable list of {@link CountWindow} instances
   */
  public List<CountWindow> countWindows() {
    return countWindows;
  }

  /**
   * Access the SUM windows captured by this plan.
   *
   * @return immutable list of {@link SumWindow} instances
   */
  public List<SumWindow> sumWindows() {
    return sumWindows;
  }

  /**
   * Access the AVG windows captured by this plan.
   *
   * @return immutable list of {@link AvgWindow} instances
   */
  public List<AvgWindow> avgWindows() {
    return avgWindows;
  }

  /**
   * Retrieve MIN analytic expression descriptors.
   *
   * @return immutable list of {@link MinWindow} instances
   */
  public List<MinWindow> minWindows() {
    return minWindows;
  }

  /**
   * Retrieve MAX analytic expression descriptors.
   *
   * @return immutable list of {@link MaxWindow} instances
   */
  public List<MaxWindow> maxWindows() {
    return maxWindows;
  }

  /**
   * Retrieve LAG analytic expression descriptors.
   *
   * @return immutable list of {@link LagWindow} instances
   */
  public List<LagWindow> lagWindows() {
    return lagWindows;
  }

  /**
   * Retrieve LEAD analytic expression descriptors.
   *
   * @return immutable list of {@link LeadWindow} instances
   */
  public List<LeadWindow> leadWindows() {
    return leadWindows;
  }

  /**
   * Builder for assembling immutable {@link WindowPlan} instances.
   */
  public static final class Builder {

    private List<RowNumberWindow> rowNumberWindows;
    private List<RankWindow> rankWindows;
    private List<DenseRankWindow> denseRankWindows;
    private List<PercentRankWindow> percentRankWindows;
    private List<CumeDistWindow> cumeDistWindows;
    private List<NtileWindow> ntileWindows;
    private List<CountWindow> countWindows;
    private List<SumWindow> sumWindows;
    private List<AvgWindow> avgWindows;
    private List<MinWindow> minWindows;
    private List<MaxWindow> maxWindows;
    private List<LagWindow> lagWindows;
    private List<LeadWindow> leadWindows;

    private Builder() {
      // Prevent external instantiation.
    }

    /**
     * Provide the ROW_NUMBER windows to include in the resulting plan.
     *
     * @param rowNumberWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder rowNumberWindows(List<RowNumberWindow> rowNumberWindows) {
      this.rowNumberWindows = copyOrNull(rowNumberWindows);
      return this;
    }

    /**
     * Provide the RANK windows to include in the resulting plan.
     *
     * @param rankWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder rankWindows(List<RankWindow> rankWindows) {
      this.rankWindows = copyOrNull(rankWindows);
      return this;
    }

    /**
     * Provide the DENSE_RANK windows to include in the resulting plan.
     *
     * @param denseRankWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder denseRankWindows(List<DenseRankWindow> denseRankWindows) {
      this.denseRankWindows = copyOrNull(denseRankWindows);
      return this;
    }

    /**
     * Provide the PERCENT_RANK windows to include in the resulting plan.
     *
     * @param percentRankWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder percentRankWindows(List<PercentRankWindow> percentRankWindows) {
      this.percentRankWindows = copyOrNull(percentRankWindows);
      return this;
    }

    /**
     * Provide the CUME_DIST windows to include in the resulting plan.
     *
     * @param cumeDistWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder cumeDistWindows(List<CumeDistWindow> cumeDistWindows) {
      this.cumeDistWindows = copyOrNull(cumeDistWindows);
      return this;
    }

    /**
     * Provide the NTILE windows to include in the resulting plan.
     *
     * @param ntileWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder ntileWindows(List<NtileWindow> ntileWindows) {
      this.ntileWindows = copyOrNull(ntileWindows);
      return this;
    }

    /**
     * Provide the COUNT windows to include in the resulting plan.
     *
     * @param countWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder countWindows(List<CountWindow> countWindows) {
      this.countWindows = copyOrNull(countWindows);
      return this;
    }

    /**
     * Provide the SUM windows to include in the resulting plan.
     *
     * @param sumWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder sumWindows(List<SumWindow> sumWindows) {
      this.sumWindows = copyOrNull(sumWindows);
      return this;
    }

    /**
     * Provide the AVG windows to include in the resulting plan.
     *
     * @param avgWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder avgWindows(List<AvgWindow> avgWindows) {
      this.avgWindows = copyOrNull(avgWindows);
      return this;
    }

    /**
     * Provide the MIN windows to include in the resulting plan.
     *
     * @param minWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder minWindows(List<MinWindow> minWindows) {
      this.minWindows = copyOrNull(minWindows);
      return this;
    }

    /**
     * Provide the MAX windows to include in the resulting plan.
     *
     * @param maxWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder maxWindows(List<MaxWindow> maxWindows) {
      this.maxWindows = copyOrNull(maxWindows);
      return this;
    }

    /**
     * Provide the LAG windows to include in the resulting plan.
     *
     * @param lagWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder lagWindows(List<LagWindow> lagWindows) {
      this.lagWindows = copyOrNull(lagWindows);
      return this;
    }

    /**
     * Provide the LEAD windows to include in the resulting plan.
     *
     * @param leadWindows
     *          the windows to capture, may be {@code null}
     * @return this builder instance for chaining
     */
    public Builder leadWindows(List<LeadWindow> leadWindows) {
      this.leadWindows = copyOrNull(leadWindows);
      return this;
    }

    /**
     * Create a mutable defensive copy of the supplied list.
     *
     * @param <T>
     *          element type contained within the list
     * @param source
     *          the list to copy, may be {@code null}
     * @return a mutable copy of {@code source} or {@code null} when {@code source}
     *         is {@code null}
     */
    private <T> List<T> copyOrNull(List<T> source) {
      if (source == null) {
        return null;
      }
      return new ArrayList<>(source);
    }

    /**
     * Assemble an immutable {@link WindowPlan} instance from the configured
     * collections.
     *
     * @return a new immutable plan
     */
    public WindowPlan build() {
      return new WindowPlan(this);
    }
  }
}