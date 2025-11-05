package se.alipsa.jparq.engine.window;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Utility methods for planning and evaluating SQL window functions that appear
 * in SELECT lists.
 */
public final class WindowFunctions {

  private WindowFunctions() {
  }

  /**
   * Create a plan describing analytic window functions that require
   * pre-computation.
   *
   * @param expressions
   *          expressions present in the SELECT list
   * @return a window plan capturing supported analytic expressions or
   *         {@code null} when none are present
   */
  public static WindowPlan plan(List<Expression> expressions) {
    if (expressions == null || expressions.isEmpty()) {
      return null;
    }
    List<RowNumberWindow> rowNumberWindows = new ArrayList<>();
    List<RankWindow> rankWindows = new ArrayList<>();
    List<DenseRankWindow> denseRankWindows = new ArrayList<>();
    List<PercentRankWindow> percentRankWindows = new ArrayList<>();
    List<CumeDistWindow> cumeDistWindows = new ArrayList<>();
    for (Expression expression : expressions) {
      if (expression == null) {
        continue;
      }
      expression.accept(new ExpressionVisitorAdapter<Void>() {
        @Override
        public <S> Void visit(AnalyticExpression analytic, S context) {
          registerAnalyticExpression(analytic, rowNumberWindows, rankWindows, denseRankWindows, percentRankWindows,
              cumeDistWindows);
          return super.visit(analytic, context);
        }
      });
    }
    if (rowNumberWindows.isEmpty() && rankWindows.isEmpty() && denseRankWindows.isEmpty()
        && percentRankWindows.isEmpty() && cumeDistWindows.isEmpty()) {
      return null;
    }
    return new WindowPlan(List.copyOf(rowNumberWindows), List.copyOf(rankWindows), List.copyOf(denseRankWindows),
        List.copyOf(percentRankWindows), List.copyOf(cumeDistWindows));
  }

  private static void registerAnalyticExpression(AnalyticExpression analytic, List<RowNumberWindow> rowNumberWindows,
      List<RankWindow> rankWindows, List<DenseRankWindow> denseRankWindows,
      List<PercentRankWindow> percentRankWindows, List<CumeDistWindow> cumeDistWindows) {
    if (analytic == null) {
      return;
    }
    String name = analytic.getName();
    if (name == null) {
      return;
    }
    if (!"ROW_NUMBER".equalsIgnoreCase(name) && !"RANK".equalsIgnoreCase(name) && !"DENSE_RANK".equalsIgnoreCase(name)
        && !"PERCENT_RANK".equalsIgnoreCase(name) && !"CUME_DIST".equalsIgnoreCase(name)) {
      return;
    }
    if (analytic.getExpression() != null) {
      throw new IllegalArgumentException(name + " must not include an argument expression: " + analytic);
    }
    if (analytic.isDistinct() || analytic.isUnique()) {
      throw new IllegalArgumentException(name + " does not support DISTINCT or UNIQUE modifiers: " + analytic);
    }
    if (analytic.getKeep() != null) {
      throw new IllegalArgumentException(name + " does not support KEEP clause: " + analytic);
    }
    ExpressionList<?> partitionList = analytic.getPartitionExpressionList();
    List<Expression> partitions = new ArrayList<>();
    if (partitionList != null) {
      for (Expression partitionExpr : partitionList) {
        partitions.add(partitionExpr);
      }
    }
    List<OrderByElement> orderBy = analytic.getOrderByElements();
    List<OrderByElement> orderElements = orderBy == null ? List.of() : List.copyOf(orderBy);
    if ("ROW_NUMBER".equalsIgnoreCase(name)) {
      rowNumberWindows.add(new RowNumberWindow(analytic, List.copyOf(partitions), orderElements));
      return;
    }
    if (orderElements.isEmpty()) {
      throw new IllegalArgumentException(name + " requires an ORDER BY clause: " + analytic);
    }
    if ("RANK".equalsIgnoreCase(name)) {
      rankWindows.add(new RankWindow(analytic, List.copyOf(partitions), orderElements));
      return;
    }
    if ("DENSE_RANK".equalsIgnoreCase(name)) {
      denseRankWindows.add(new DenseRankWindow(analytic, List.copyOf(partitions), orderElements));
      return;
    }
    if ("PERCENT_RANK".equalsIgnoreCase(name)) {
      percentRankWindows.add(new PercentRankWindow(analytic, List.copyOf(partitions), orderElements));
      return;
    }
    if ("CUME_DIST".equalsIgnoreCase(name)) {
      cumeDistWindows.add(new CumeDistWindow(analytic, List.copyOf(partitions), orderElements));
    }
  }

  /**
   * Compute window function values for the supplied records.
   *
   * @param plan
   *          the window plan describing required computations
   * @param records
   *          filtered records participating in the query
   * @param schema
   *          schema describing the records
   * @param subqueryExecutor
   *          executor used for correlated subqueries
   * @param outerQualifiers
   *          qualifiers belonging to the outer query scope
   * @param qualifierColumnMapping
   *          mapping of table aliases to canonical column names
   * @param unqualifiedColumnMapping
   *          mapping for unqualified column references
   * @return window state containing the computed values
   */
  public static WindowState compute(WindowPlan plan, List<GenericRecord> records, Schema schema,
      SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping) {
    if (plan == null || plan.isEmpty() || records == null || records.isEmpty() || schema == null) {
      return WindowState.empty();
    }
    ValueExpressionEvaluator evaluator = new ValueExpressionEvaluator(schema, subqueryExecutor, outerQualifiers,
        qualifierColumnMapping, unqualifiedColumnMapping, WindowState.empty());
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues = new IdentityHashMap<>();
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rankValues = new IdentityHashMap<>();
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> denseRankValues = new IdentityHashMap<>();
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> cumeDistValues = new IdentityHashMap<>();
    for (RowNumberWindow window : plan.rowNumberWindows()) {
      IdentityHashMap<GenericRecord, Long> values = computeRowNumbers(window, records, evaluator);
      rowNumberValues.put(window.expression(), values);
    }
    for (RankWindow window : plan.rankWindows()) {
      IdentityHashMap<GenericRecord, Long> values = computeRank(window, records, evaluator);
      rankValues.put(window.expression(), values);
    }
    for (DenseRankWindow window : plan.denseRankWindows()) {
      IdentityHashMap<GenericRecord, Long> values = computeDenseRank(window, records, evaluator);
      denseRankValues.put(window.expression(), values);
    }
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> percentRankValues;
    percentRankValues = new IdentityHashMap<>();
    for (PercentRankWindow window : plan.percentRankWindows()) {
      IdentityHashMap<GenericRecord, BigDecimal> values = computePercentRank(window, records, evaluator);
      percentRankValues.put(window.expression(), values);
    }
    for (CumeDistWindow window : plan.cumeDistWindows()) {
      IdentityHashMap<GenericRecord, BigDecimal> values = computeCumeDist(window, records, evaluator);
      cumeDistValues.put(window.expression(), values);
    }
    return new WindowState(rowNumberValues, rankValues, denseRankValues, percentRankValues, cumeDistValues);
  }

  private static IdentityHashMap<GenericRecord, Long> computeRowNumbers(RowNumberWindow window,
      List<GenericRecord> records, ValueExpressionEvaluator evaluator) {
    List<RowContext> contexts = buildSortedContexts(window.partitionExpressions(), window.orderByElements(), records,
        evaluator);

    IdentityHashMap<GenericRecord, Long> values = new IdentityHashMap<>();
    List<Object> previousPartition = null;
    long rowNumber = 0L;
    for (RowContext context : contexts) {
      if (!Objects.equals(previousPartition, context.partitionValues())) {
        previousPartition = context.partitionValues();
        rowNumber = 1L;
      } else {
        rowNumber++;
      }
      values.put(context.record(), rowNumber);
    }
    return values;
  }

  private static IdentityHashMap<GenericRecord, Long> computeRank(RankWindow window, List<GenericRecord> records,
      ValueExpressionEvaluator evaluator) {
    List<RowContext> contexts = buildSortedContexts(window.partitionExpressions(), window.orderByElements(), records,
        evaluator);

    IdentityHashMap<GenericRecord, Long> values = new IdentityHashMap<>();
    List<Object> previousPartition = null;
    List<OrderComponent> previousOrder = null;
    long processedInPartition = 0L;
    long currentRank = 0L;
    for (RowContext context : contexts) {
      if (!Objects.equals(previousPartition, context.partitionValues())) {
        previousPartition = context.partitionValues();
        previousOrder = null;
        processedInPartition = 0L;
        currentRank = 0L;
      }
      processedInPartition++;
      if (previousOrder == null) {
        currentRank = 1L;
      } else if (!orderComponentsEqual(previousOrder, context.orderComponents())) {
        currentRank = processedInPartition;
      }
      values.put(context.record(), currentRank);
      previousOrder = context.orderComponents();
    }
    return values;
  }

  private static IdentityHashMap<GenericRecord, Long> computeDenseRank(DenseRankWindow window,
      List<GenericRecord> records, ValueExpressionEvaluator evaluator) {
    List<RowContext> contexts = buildSortedContexts(window.partitionExpressions(), window.orderByElements(), records,
        evaluator);

    IdentityHashMap<GenericRecord, Long> values = new IdentityHashMap<>();
    List<Object> previousPartition = null;
    List<OrderComponent> previousOrder = null;
    long currentRank = 0L;
    for (RowContext context : contexts) {
      if (!Objects.equals(previousPartition, context.partitionValues())) {
        previousPartition = context.partitionValues();
        previousOrder = null;
        currentRank = 0L;
      }
      if (previousOrder == null) {
        currentRank = 1L;
      } else if (!orderComponentsEqual(previousOrder, context.orderComponents())) {
        currentRank++;
      }
      values.put(context.record(), currentRank);
      previousOrder = context.orderComponents();
    }
    return values;
  }

  /**
   * Compute the SQL standard {@code PERCENT_RANK} values for the supplied window
   * definition.
   *
   * @param window
   *          analytic window specification
   * @param records
   *          records participating in the computation
   * @param evaluator
   *          evaluator for partition and ordering expressions
   * @return mapping from {@link GenericRecord} to the computed percent rank value
   */
  private static IdentityHashMap<GenericRecord, BigDecimal> computePercentRank(PercentRankWindow window,
      List<GenericRecord> records, ValueExpressionEvaluator evaluator) {
    List<RowContext> contexts = buildSortedContexts(window.partitionExpressions(), window.orderByElements(), records,
        evaluator);

    IdentityHashMap<GenericRecord, BigDecimal> values = new IdentityHashMap<>();

    int i = 0;
    final int n = contexts.size();
    while (i < n) {
      // Identify the partition starting at i
      List<Object> partitionValues = contexts.get(i).partitionValues();
      int partitionStart = i;
      int partitionEnd = i;
      while (partitionEnd < n && Objects.equals(contexts.get(partitionEnd).partitionValues(), partitionValues)) {
        partitionEnd++;
      }
      long totalRows = partitionEnd - partitionStart;

      List<OrderComponent> previousOrder = null;
      long processedInPartition = 0L;
      long currentRank = 0L;
      for (int j = partitionStart; j < partitionEnd; j++) {
        RowContext context = contexts.get(j);
        processedInPartition++;
        if (previousOrder == null) {
          currentRank = 1L;
        } else if (!orderComponentsEqual(previousOrder, context.orderComponents())) {
          currentRank = processedInPartition;
        }
        BigDecimal percentRank;
        if (totalRows <= 1L) {
          percentRank = BigDecimal.ZERO;
        } else {
          BigDecimal numerator = BigDecimal.valueOf(currentRank - 1L);
          BigDecimal denominator = BigDecimal.valueOf(totalRows - 1L);
          percentRank = numerator.divide(denominator, MathContext.DECIMAL128);
        }
        values.put(context.record(), percentRank);
        previousOrder = context.orderComponents();
      }
      i = partitionEnd;
    }
    return values;
  }

  /**
   * Compute the SQL standard {@code CUME_DIST} values for the supplied window
   * definition.
   *
   * @param window
   *          analytic window specification
   * @param records
   *          records participating in the computation
   * @param evaluator
   *          evaluator for partition and ordering expressions
   * @return mapping from {@link GenericRecord} to the computed cumulative
   *         distribution value
   */
  private static IdentityHashMap<GenericRecord, BigDecimal> computeCumeDist(CumeDistWindow window,
      List<GenericRecord> records, ValueExpressionEvaluator evaluator) {
    List<RowContext> contexts = buildSortedContexts(window.partitionExpressions(), window.orderByElements(), records,
        evaluator);

    IdentityHashMap<GenericRecord, BigDecimal> values = new IdentityHashMap<>();

    int index = 0;
    final int totalContexts = contexts.size();
    while (index < totalContexts) {
      List<Object> partitionValues = contexts.get(index).partitionValues();
      int partitionStart = index;
      int partitionEnd = index;
      while (partitionEnd < totalContexts
          && Objects.equals(contexts.get(partitionEnd).partitionValues(), partitionValues)) {
        partitionEnd++;
      }

      long totalRows = partitionEnd - partitionStart;
      if (totalRows == 0L) {
        index = partitionEnd;
        continue;
      }

      BigDecimal denominator = BigDecimal.valueOf(totalRows);
      long processed = 0L;
      int groupStart = partitionStart;
      while (groupStart < partitionEnd) {
        List<OrderComponent> orderComponents = contexts.get(groupStart).orderComponents();
        int groupEnd = groupStart;
        while (groupEnd < partitionEnd && orderComponentsEqual(orderComponents, contexts.get(groupEnd).orderComponents())) {
          groupEnd++;
        }
        processed += groupEnd - groupStart;
        BigDecimal cumeDist = BigDecimal.valueOf(processed).divide(denominator, MathContext.DECIMAL128);
        for (int i = groupStart; i < groupEnd; i++) {
          RowContext context = contexts.get(i);
          values.put(context.record(), cumeDist);
        }
        groupStart = groupEnd;
      }
      index = partitionEnd;
    }
    return values;
  }

  private static List<RowContext> buildSortedContexts(List<Expression> partitionExpressions,
      List<OrderByElement> orderByElements, List<GenericRecord> records, ValueExpressionEvaluator evaluator) {
    List<RowContext> contexts = new ArrayList<>(records.size());
    int index = 0;
    for (GenericRecord record : records) {
      List<Object> partitionValues = evaluateAll(partitionExpressions, evaluator, record);
      List<OrderComponent> orderValues = buildOrderComponents(orderByElements, evaluator, record);
      contexts.add(new RowContext(record, partitionValues, orderValues, index++));
    }
    contexts.sort((left, right) -> compareContexts(left, right, orderByElements));
    return contexts;
  }

  private static List<Object> evaluateAll(List<Expression> expressions, ValueExpressionEvaluator evaluator,
      GenericRecord record) {
    if (expressions == null || expressions.isEmpty()) {
      return List.of();
    }
    List<Object> values = new ArrayList<>(expressions.size());
    for (Expression expression : expressions) {
      values.add(evaluator.eval(expression, record));
    }
    return List.copyOf(values);
  }

  private static List<OrderComponent> buildOrderComponents(List<OrderByElement> elements,
      ValueExpressionEvaluator evaluator, GenericRecord record) {
    if (elements == null || elements.isEmpty()) {
      return List.of();
    }
    List<OrderComponent> components = new ArrayList<>(elements.size());
    for (OrderByElement element : elements) {
      boolean asc = element.isAsc();
      OrderByElement.NullOrdering nullOrdering = element.getNullOrdering();
      Object value = evaluator.eval(element.getExpression(), record);
      components.add(new OrderComponent(value, asc, nullOrdering));
    }
    return List.copyOf(components);
  }

  private static boolean orderComponentsEqual(List<OrderComponent> left, List<OrderComponent> right) {
    if (left == right) {
      return true;
    }
    if (left == null || right == null) {
      return (left == null || left.isEmpty()) && (right == null || right.isEmpty());
    }
    if (left.size() != right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); i++) {
      OrderComponent lc = left.get(i);
      OrderComponent rc = right.get(i);
      if (compareOrderValues(lc, rc) != 0) {
        return false;
      }
    }
    return true;
  }

  private static int compareContexts(RowContext left, RowContext right, List<OrderByElement> orderElements) {
    int partitionCompare = comparePartitions(left.partitionValues(), right.partitionValues());
    if (partitionCompare != 0) {
      return partitionCompare;
    }
    if (orderElements == null || orderElements.isEmpty()) {
      return Integer.compare(left.originalIndex(), right.originalIndex());
    }
    for (int i = 0; i < orderElements.size(); i++) {
      OrderComponent leftComponent = left.orderComponents().get(i);
      OrderComponent rightComponent = right.orderComponents().get(i);
      int cmp = compareOrderValues(leftComponent, rightComponent);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(left.originalIndex(), right.originalIndex());
  }

  private static int comparePartitions(List<Object> left, List<Object> right) {
    int size = Math.min(left.size(), right.size());
    for (int i = 0; i < size; i++) {
      Object lv = left.get(i);
      Object rv = right.get(i);
      if (lv == rv) {
        continue;
      }
      if (lv == null && rv == null) {
        continue;
      }
      if (lv == null) {
        return -1;
      }
      if (rv == null) {
        return 1;
      }
      int cmp = compareValues(lv, rv);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(left.size(), right.size());
  }

  private static int compareOrderValues(OrderComponent left, OrderComponent right) {
    Object lv = left.value();
    Object rv = right.value();
    if (lv == rv) {
      return 0;
    }
    if (lv == null || rv == null) {
      return compareNulls(lv, rv, left.ascending(), left.nullOrdering());
    }
    int cmp = compareValues(lv, rv);
    return left.ascending() ? cmp : -cmp;
  }

  private static int compareNulls(Object left, Object right, boolean asc, OrderByElement.NullOrdering nullOrdering) {
    if (left == null && right == null) {
      return 0;
    }
    if (nullOrdering == OrderByElement.NullOrdering.NULLS_FIRST) {
      return left == null ? -1 : 1;
    }
    if (nullOrdering == OrderByElement.NullOrdering.NULLS_LAST) {
      return left == null ? 1 : -1;
    }
    // Default ordering: NULLS LAST for ascending, NULLS FIRST for descending
    if (asc) {
      return left == null ? 1 : -1;
    }
    return left == null ? -1 : 1;
  }

  private static int compareValues(Object left, Object right) {
    if (left instanceof Number && right instanceof Number) {
      return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString()));
    }
    if (left instanceof Boolean && right instanceof Boolean) {
      return Boolean.compare((Boolean) left, (Boolean) right);
    }
    if (left instanceof byte[] && right instanceof byte[]) {
      return compareBinary((byte[]) left, (byte[]) right);
    }
    if (left instanceof ByteBuffer lb && right instanceof ByteBuffer rb) {
      byte[] la = new byte[lb.remaining()];
      byte[] ra = new byte[rb.remaining()];
      lb.duplicate().get(la);
      rb.duplicate().get(ra);
      return compareBinary(la, ra);
    }
    if (left instanceof Timestamp && right instanceof Timestamp) {
      return Long.compare(((Timestamp) left).getTime(), ((Timestamp) right).getTime());
    }
    if (left instanceof Date && right instanceof Date) {
      return Long.compare(((Date) left).getTime(), ((Date) right).getTime());
    }
    if (left instanceof TemporalInterval li && right instanceof TemporalInterval ri) {
      return li.compareTo(ri);
    }
    return left.toString().compareTo(right.toString());
  }

  /**
   * Compare two byte sequences using lexicographic ordering.
   *
   * @param left
   *          left-hand binary value (may be {@code null})
   * @param right
   *          right-hand binary value (may be {@code null})
   * @return negative when {@code left < right}, zero when equal, otherwise
   *         positive
   */
  private static int compareBinary(byte[] left, byte[] right) {
    if (left == right) {
      return 0;
    }
    if (left == null) {
      return -1;
    }
    if (right == null) {
      return 1;
    }
    return Arrays.compare(left, right);
  }

  /**
   * Description of analytic window operations that must be computed prior to
   * projection evaluation.
   */
  public static final class WindowPlan {

    private final List<RowNumberWindow> rowNumberWindows;
    private final List<RankWindow> rankWindows;
    private final List<DenseRankWindow> denseRankWindows;
    private final List<PercentRankWindow> percentRankWindows;
    private final List<CumeDistWindow> cumeDistWindows;

    WindowPlan(List<RowNumberWindow> rowNumberWindows, List<RankWindow> rankWindows,
        List<DenseRankWindow> denseRankWindows, List<PercentRankWindow> percentRankWindows,
        List<CumeDistWindow> cumeDistWindows) {
      this.rowNumberWindows = rowNumberWindows == null ? List.of() : rowNumberWindows;
      this.rankWindows = rankWindows == null ? List.of() : rankWindows;
      this.denseRankWindows = denseRankWindows == null ? List.of() : denseRankWindows;
      this.percentRankWindows = percentRankWindows == null ? List.of() : percentRankWindows;
      this.cumeDistWindows = cumeDistWindows == null ? List.of() : cumeDistWindows;
    }

    /**
     * Determine whether the plan contains any analytic window functions.
     *
     * @return {@code true} when the plan includes pre-computed window functions,
     *         otherwise {@code false}
     */
    public boolean isEmpty() {
      return rowNumberWindows.isEmpty() && rankWindows.isEmpty() && denseRankWindows.isEmpty()
          && percentRankWindows.isEmpty() && cumeDistWindows.isEmpty();
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
  }

  /**
   * Representation of a ROW_NUMBER analytic expression.
   */
  public static final class RowNumberWindow {

    private final AnalyticExpression expression;
    private final List<Expression> partitionExpressions;
    private final List<OrderByElement> orderByElements;

    RowNumberWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
        List<OrderByElement> orderByElements) {
      this.expression = expression;
      this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
      this.orderByElements = orderByElements == null ? List.of() : orderByElements;
    }

    /**
     * Retrieve the underlying analytic expression.
     *
     * @return the {@link AnalyticExpression} represented by this window
     */
    public AnalyticExpression expression() {
      return expression;
    }

    /**
     * Retrieve expressions defining the PARTITION BY clause.
     *
     * @return immutable list of partition expressions
     */
    public List<Expression> partitionExpressions() {
      return partitionExpressions;
    }

    /**
     * Retrieve ORDER BY elements defining the ordering within each partition.
     *
     * @return immutable list of {@link OrderByElement} descriptors
     */
    public List<OrderByElement> orderByElements() {
      return orderByElements;
    }
  }

  /**
   * Representation of a RANK analytic expression.
   */
  public static final class RankWindow {

    private final AnalyticExpression expression;
    private final List<Expression> partitionExpressions;
    private final List<OrderByElement> orderByElements;

    RankWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
        List<OrderByElement> orderByElements) {
      this.expression = expression;
      this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
      this.orderByElements = orderByElements == null ? List.of() : orderByElements;
    }

    /**
     * Retrieve the underlying analytic expression.
     *
     * @return the {@link AnalyticExpression} represented by this window
     */
    public AnalyticExpression expression() {
      return expression;
    }

    /**
     * Retrieve expressions defining the PARTITION BY clause.
     *
     * @return immutable list of partition expressions
     */
    public List<Expression> partitionExpressions() {
      return partitionExpressions;
    }

    /**
     * Retrieve ORDER BY elements defining the ordering within each partition.
     *
     * @return immutable list of {@link OrderByElement} descriptors
     */
    public List<OrderByElement> orderByElements() {
      return orderByElements;
    }
  }

  /**
   * Representation of a DENSE_RANK analytic expression.
   */
  public static final class DenseRankWindow {

    private final AnalyticExpression expression;
    private final List<Expression> partitionExpressions;
    private final List<OrderByElement> orderByElements;

    DenseRankWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
        List<OrderByElement> orderByElements) {
      this.expression = expression;
      this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
      this.orderByElements = orderByElements == null ? List.of() : orderByElements;
    }

    /**
     * Retrieve the underlying analytic expression.
     *
     * @return the {@link AnalyticExpression} represented by this window
     */
    public AnalyticExpression expression() {
      return expression;
    }

    /**
     * Retrieve expressions defining the PARTITION BY clause.
     *
     * @return immutable list of partition expressions
     */
    public List<Expression> partitionExpressions() {
      return partitionExpressions;
    }

    /**
     * Retrieve ORDER BY elements defining the ordering within each partition.
     *
     * @return immutable list of {@link OrderByElement} descriptors
     */
    public List<OrderByElement> orderByElements() {
      return orderByElements;
    }
  }

  /**
   * Representation of a PERCENT_RANK analytic expression.
   */
  public static final class PercentRankWindow {

    private final AnalyticExpression expression;
    private final List<Expression> partitionExpressions;
    private final List<OrderByElement> orderByElements;

    PercentRankWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
        List<OrderByElement> orderByElements) {
      this.expression = expression;
      this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
      this.orderByElements = orderByElements == null ? List.of() : orderByElements;
    }

    /**
     * Retrieve the underlying analytic expression.
     *
     * @return the {@link AnalyticExpression} represented by this window
     */
    public AnalyticExpression expression() {
      return expression;
    }

    /**
     * Retrieve expressions defining the PARTITION BY clause.
     *
     * @return immutable list of partition expressions
     */
    public List<Expression> partitionExpressions() {
      return partitionExpressions;
    }

    /**
     * Retrieve ORDER BY elements defining the ordering within each partition.
     *
     * @return immutable list of {@link OrderByElement} descriptors
     */
    public List<OrderByElement> orderByElements() {
      return orderByElements;
    }
  }

  /**
   * Representation of a CUME_DIST analytic expression.
   */
  public static final class CumeDistWindow {

    private final AnalyticExpression expression;
    private final List<Expression> partitionExpressions;
    private final List<OrderByElement> orderByElements;

    CumeDistWindow(AnalyticExpression expression, List<Expression> partitionExpressions,
        List<OrderByElement> orderByElements) {
      this.expression = expression;
      this.partitionExpressions = partitionExpressions == null ? List.of() : partitionExpressions;
      this.orderByElements = orderByElements == null ? List.of() : orderByElements;
    }

    /**
     * Retrieve the underlying analytic expression.
     *
     * @return the {@link AnalyticExpression} represented by this window
     */
    public AnalyticExpression expression() {
      return expression;
    }

    /**
     * Retrieve expressions defining the PARTITION BY clause.
     *
     * @return immutable list of partition expressions
     */
    public List<Expression> partitionExpressions() {
      return partitionExpressions;
    }

    /**
     * Retrieve ORDER BY elements defining the ordering within each partition.
     *
     * @return immutable list of {@link OrderByElement} descriptors
     */
    public List<OrderByElement> orderByElements() {
      return orderByElements;
    }
  }

  private record RowContext(GenericRecord record, List<Object> partitionValues, List<OrderComponent> orderComponents,
      int originalIndex) {
  }

  private record OrderComponent(Object value, boolean ascending, OrderByElement.NullOrdering nullOrdering) {
  }

  /**
   * Immutable container for precomputed window values.
   */
  public static final class WindowState {

    private static final WindowState EMPTY = new WindowState(Map.of(), Map.of(), Map.of(), Map.of(), Map.of());

    private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues;
    private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rankValues;
    private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> denseRankValues;
    private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> percentRankValues;
    private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> cumeDistValues;

    WindowState(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues,
        Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rankValues,
        Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> denseRankValues,
        Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> percentRankValues,
        Map<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> cumeDistValues) {
      this.rowNumberValues = rowNumberValues == null ? Map.of() : Collections.unmodifiableMap(rowNumberValues);
      this.rankValues = rankValues == null ? Map.of() : Collections.unmodifiableMap(rankValues);
      this.denseRankValues = denseRankValues == null ? Map.of() : Collections.unmodifiableMap(denseRankValues);
      this.percentRankValues = percentRankValues == null ? Map.of() : Collections.unmodifiableMap(percentRankValues);
      this.cumeDistValues = cumeDistValues == null ? Map.of() : Collections.unmodifiableMap(cumeDistValues);
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
      return rowNumberValues.isEmpty() && rankValues.isEmpty() && denseRankValues.isEmpty()
          && percentRankValues.isEmpty() && cumeDistValues.isEmpty();
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
  }
}
