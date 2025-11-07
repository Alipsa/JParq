package se.alipsa.jparq.engine.window;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import net.sf.jsqlparser.expression.AnalyticExpression;
import org.apache.avro.generic.GenericRecord;

/**
 * Immutable container for precomputed window values.
 */
public final class WindowState {

  private static final WindowState EMPTY = WindowState.builder().build();

  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rankValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> denseRankValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> percentRankValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> cumeDistValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> ntileValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> countValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> sumValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> avgValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> minValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> maxValues;

  private WindowState(Builder builder) {
    this.rowNumberValues = immutableMap(builder.rowNumberValues);
    this.rankValues = immutableMap(builder.rankValues);
    this.denseRankValues = immutableMap(builder.denseRankValues);
    this.percentRankValues = immutableMap(builder.percentRankValues);
    this.cumeDistValues = immutableMap(builder.cumeDistValues);
    this.ntileValues = immutableMap(builder.ntileValues);
    this.countValues = immutableMap(builder.countValues);
    this.sumValues = immutableMap(builder.sumValues);
    this.avgValues = immutableMap(builder.avgValues);
    this.minValues = immutableMap(builder.minValues);
    this.maxValues = immutableMap(builder.maxValues);
  }

  /**
   * Create an immutable view of the supplied map.
   *
   * @param <V>
   *          value type stored within the inner {@link IdentityHashMap}
   * @param values
   *          the map to wrap, may be {@code null}
   * @return an immutable map or {@link Map#of()} when {@code values} is {@code null}
   */
  private static <V> Map<AnalyticExpression, IdentityHashMap<GenericRecord, V>> immutableMap(
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, V>> values) {
    if (values == null) {
      return Map.of();
    }
    return Collections.unmodifiableMap(values);
  }

  /**
   * Create a builder for assembling immutable {@link WindowState} instances.
   *
   * @return a new builder ready to accept computed analytic results
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Retrieve the shared empty window state instance.
   *
   * @return shared empty state
   */
  public static WindowState empty() {
    return EMPTY;
  }

  /**
   * Determine whether this state contains any window values.
   *
   * @return {@code true} when no window values are present
   */
  public boolean isEmpty() {
    return rowNumberValues.isEmpty() && rankValues.isEmpty() && denseRankValues.isEmpty() && percentRankValues.isEmpty()
        && cumeDistValues.isEmpty() && ntileValues.isEmpty() && countValues.isEmpty() && sumValues.isEmpty()
        && avgValues.isEmpty() && minValues.isEmpty() && maxValues.isEmpty();
  }

  /**
   * Obtain the precomputed ROW_NUMBER value for the supplied expression and
   * record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed row number value
   */
  public long rowNumber(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, Long> values = rowNumberValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No ROW_NUMBER values available for expression: " + expression);
    }
    Long value = values.get(record);
    if (value == null) {
      throw new IllegalArgumentException("No ROW_NUMBER value computed for record: " + record);
    }
    return value;
  }

  /**
   * Obtain the precomputed RANK value for the supplied expression and record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed rank value
   */
  public long rank(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, Long> values = rankValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No RANK values available for expression: " + expression);
    }
    Long value = values.get(record);
    if (value == null) {
      throw new IllegalArgumentException("No RANK value computed for record: " + record);
    }
    return value;
  }

  /**
   * Obtain the precomputed DENSE_RANK value for the supplied expression and
   * record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed dense rank value
   */
  public long denseRank(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, Long> values = denseRankValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No DENSE_RANK values available for expression: " + expression);
    }
    Long value = values.get(record);
    if (value == null) {
      throw new IllegalArgumentException("No DENSE_RANK value computed for record: " + record);
    }
    return value;
  }

  /**
   * Obtain the precomputed PERCENT_RANK value for the supplied expression and
   * record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed percent rank value
   */
  public BigDecimal percentRank(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, BigDecimal> values = percentRankValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No PERCENT_RANK values available for expression: " + expression);
    }
    BigDecimal value = values.get(record);
    if (value == null) {
      throw new IllegalArgumentException("No PERCENT_RANK value computed for record: " + record);
    }
    return value;
  }

  /**
   * Obtain the precomputed CUME_DIST value for the supplied expression and
   * record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed cumulative distribution value
   */
  public BigDecimal cumeDist(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, BigDecimal> values = cumeDistValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No CUME_DIST values available for expression: " + expression);
    }
    BigDecimal value = values.get(record);
    if (value == null) {
      throw new IllegalArgumentException("No CUME_DIST value computed for record: " + record);
    }
    return value;
  }

  /**
   * Obtain the precomputed NTILE value for the supplied expression and record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed tile index
   */
  public long ntile(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, Long> values = ntileValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No NTILE values available for expression: " + expression);
    }
    Long value = values.get(record);
    if (value == null) {
      throw new IllegalArgumentException("No NTILE value computed for record: " + record);
    }
    return value;
  }

  /**
   * Obtain the precomputed COUNT value for the supplied expression and record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed count value
   */
  public long count(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, Long> values = countValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No COUNT values available for expression: " + expression);
    }
    Long value = values.get(record);
    if (value == null) {
      throw new IllegalArgumentException("No COUNT value computed for record: " + record);
    }
    return value;
  }

  /**
   * Obtain the precomputed SUM value for the supplied expression and record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed sum value
   */
  public Object sum(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, Object> values = sumValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No SUM values available for expression: " + expression);
    }
    if (!values.containsKey(record)) {
      throw new IllegalArgumentException("No SUM value computed for record: " + record);
    }
    return values.get(record);
  }

  /**
   * Obtain the precomputed AVG value for the supplied expression and record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed average value
   */
  public Object avg(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, Object> values = avgValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No AVG values available for expression: " + expression);
    }
    if (!values.containsKey(record)) {
      throw new IllegalArgumentException("No AVG value computed for record: " + record);
    }
    return values.get(record);
  }

  /**
   * Obtain the precomputed MIN value for the supplied expression and record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed minimum value
   */
  public Object min(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, Object> values = minValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No MIN values available for expression: " + expression);
    }
    if (!values.containsKey(record)) {
      throw new IllegalArgumentException("No MIN value computed for record: " + record);
    }
    return values.get(record);
  }

  /**
   * Obtain the precomputed MAX value for the supplied expression and record.
   *
   * @param expression
   *          the analytic expression
   * @param record
   *          the current record
   * @return the computed maximum value
   */
  public Object max(AnalyticExpression expression, GenericRecord record) {
    IdentityHashMap<GenericRecord, Object> values = maxValues.get(expression);
    if (values == null) {
      throw new IllegalArgumentException("No MAX values available for expression: " + expression);
    }
    if (!values.containsKey(record)) {
      throw new IllegalArgumentException("No MAX value computed for record: " + record);
    }
    return values.get(record);
  }

  /**
   * Builder for assembling immutable {@link WindowState} instances.
   */
  public static final class Builder {

    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rankValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> denseRankValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> percentRankValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> cumeDistValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> ntileValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> countValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> sumValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> avgValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> minValues;
    private Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> maxValues;

    private Builder() {
      // Prevent external instantiation.
    }

    /**
     * Provide precomputed ROW_NUMBER results for the window state.
     *
     * @param rowNumberValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder rowNumberValues(
        Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues) {
      this.rowNumberValues = copyMap(rowNumberValues);
      return this;
    }

    /**
     * Provide precomputed RANK results for the window state.
     *
     * @param rankValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder rankValues(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rankValues) {
      this.rankValues = copyMap(rankValues);
      return this;
    }

    /**
     * Provide precomputed DENSE_RANK results for the window state.
     *
     * @param denseRankValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder denseRankValues(
        Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> denseRankValues) {
      this.denseRankValues = copyMap(denseRankValues);
      return this;
    }

    /**
     * Provide precomputed PERCENT_RANK results for the window state.
     *
     * @param percentRankValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder percentRankValues(
        Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> percentRankValues) {
      this.percentRankValues = copyMap(percentRankValues);
      return this;
    }

    /**
     * Provide precomputed CUME_DIST results for the window state.
     *
     * @param cumeDistValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder cumeDistValues(
        Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> cumeDistValues) {
      this.cumeDistValues = copyMap(cumeDistValues);
      return this;
    }

    /**
     * Provide precomputed NTILE results for the window state.
     *
     * @param ntileValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder ntileValues(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> ntileValues) {
      this.ntileValues = copyMap(ntileValues);
      return this;
    }

    /**
     * Provide precomputed COUNT results for the window state.
     *
     * @param countValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder countValues(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> countValues) {
      this.countValues = copyMap(countValues);
      return this;
    }

    /**
     * Provide precomputed SUM results for the window state.
     *
     * @param sumValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder sumValues(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> sumValues) {
      this.sumValues = copyMap(sumValues);
      return this;
    }

    /**
     * Provide precomputed AVG results for the window state.
     *
     * @param avgValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder avgValues(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> avgValues) {
      this.avgValues = copyMap(avgValues);
      return this;
    }

    /**
     * Provide precomputed MIN results for the window state.
     *
     * @param minValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder minValues(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> minValues) {
      this.minValues = copyMap(minValues);
      return this;
    }

    /**
     * Provide precomputed MAX results for the window state.
     *
     * @param maxValues
     *          values keyed by analytic expression, may be {@code null}
     * @return this builder for chaining
     */
    public Builder maxValues(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> maxValues) {
      this.maxValues = copyMap(maxValues);
      return this;
    }

    /**
     * Create a defensive copy of the supplied map while retaining identity semantics.
     *
     * @param <T>
     *          value type stored within the inner {@link IdentityHashMap}
     * @param values
     *          the map to copy, may be {@code null}
     * @return a copy of {@code values} or {@code null} when {@code values} is {@code null}
     */
    private <T> Map<AnalyticExpression, IdentityHashMap<GenericRecord, T>> copyMap(
        Map<AnalyticExpression, IdentityHashMap<GenericRecord, T>> values) {
      if (values == null) {
        return null;
      }
      return new IdentityHashMap<>(values);
    }

    /**
     * Assemble an immutable {@link WindowState} instance from the provided
     * analytic results.
     *
     * @return a new immutable window state
     */
    public WindowState build() {
      return new WindowState(this);
    }
  }
}
