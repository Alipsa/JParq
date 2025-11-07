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

  private static final WindowState EMPTY = new WindowState(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
      Map.of(), Map.of(), Map.of(), Map.of());

  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rankValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> denseRankValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> percentRankValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> cumeDistValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> ntileValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> sumValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> avgValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> minValues;
  private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> maxValues;

  WindowState(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues,
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rankValues,
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> denseRankValues,
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> percentRankValues,
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> cumeDistValues,
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> ntileValues,
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> sumValues,
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> avgValues,
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> minValues,
      Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> maxValues) {
    this.rowNumberValues = rowNumberValues == null ? Map.of() : Collections.unmodifiableMap(rowNumberValues);
    this.rankValues = rankValues == null ? Map.of() : Collections.unmodifiableMap(rankValues);
    this.denseRankValues = denseRankValues == null ? Map.of() : Collections.unmodifiableMap(denseRankValues);
    this.percentRankValues = percentRankValues == null ? Map.of() : Collections.unmodifiableMap(percentRankValues);
    this.cumeDistValues = cumeDistValues == null ? Map.of() : Collections.unmodifiableMap(cumeDistValues);
    this.ntileValues = ntileValues == null ? Map.of() : Collections.unmodifiableMap(ntileValues);
    this.sumValues = sumValues == null ? Map.of() : Collections.unmodifiableMap(sumValues);
    this.avgValues = avgValues == null ? Map.of() : Collections.unmodifiableMap(avgValues);
    this.minValues = minValues == null ? Map.of() : Collections.unmodifiableMap(minValues);
    this.maxValues = maxValues == null ? Map.of() : Collections.unmodifiableMap(maxValues);
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
        && cumeDistValues.isEmpty() && ntileValues.isEmpty() && sumValues.isEmpty() && avgValues.isEmpty()
        && minValues.isEmpty() && maxValues.isEmpty();
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
}
