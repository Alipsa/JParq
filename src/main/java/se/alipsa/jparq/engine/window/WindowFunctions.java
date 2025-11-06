package se.alipsa.jparq.engine.window;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.expression.WindowOffset;
import net.sf.jsqlparser.expression.WindowRange;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.helper.LiteralConverter;
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
    List<NtileWindow> ntileWindows = new ArrayList<>();
    List<SumWindow> sumWindows = new ArrayList<>();
    List<AvgWindow> avgWindows = new ArrayList<>();
    List<MinWindow> minWindows = new ArrayList<>();
    for (Expression expression : expressions) {
      if (expression == null) {
        continue;
      }
      expression.accept(new ExpressionVisitorAdapter<Void>() {
        @Override
        public <S> Void visit(AnalyticExpression analytic, S context) {
          registerAnalyticExpression(analytic, rowNumberWindows, rankWindows, denseRankWindows, percentRankWindows,
              cumeDistWindows, ntileWindows, sumWindows, avgWindows, minWindows);
          return super.visit(analytic, context);
        }
      });
    }
    if (rowNumberWindows.isEmpty() && rankWindows.isEmpty() && denseRankWindows.isEmpty()
        && percentRankWindows.isEmpty() && cumeDistWindows.isEmpty() && ntileWindows.isEmpty() && sumWindows.isEmpty()
        && avgWindows.isEmpty() && minWindows.isEmpty()) {
      return null;
    }
    return new WindowPlan(List.copyOf(rowNumberWindows), List.copyOf(rankWindows), List.copyOf(denseRankWindows),
        List.copyOf(percentRankWindows), List.copyOf(cumeDistWindows), List.copyOf(ntileWindows),
        List.copyOf(sumWindows), List.copyOf(avgWindows), List.copyOf(minWindows));
  }

  private static void registerAnalyticExpression(AnalyticExpression analytic, List<RowNumberWindow> rowNumberWindows,
      List<RankWindow> rankWindows, List<DenseRankWindow> denseRankWindows, List<PercentRankWindow> percentRankWindows,
      List<CumeDistWindow> cumeDistWindows, List<NtileWindow> ntileWindows, List<SumWindow> sumWindows,
      List<AvgWindow> avgWindows, List<MinWindow> minWindows) {
    if (analytic == null) {
      return;
    }
    String name = analytic.getName();
    if (name == null) {
      return;
    }
    if (!"ROW_NUMBER".equalsIgnoreCase(name) && !"RANK".equalsIgnoreCase(name) && !"DENSE_RANK".equalsIgnoreCase(name)
        && !"PERCENT_RANK".equalsIgnoreCase(name) && !"CUME_DIST".equalsIgnoreCase(name)
        && !"NTILE".equalsIgnoreCase(name) && !"SUM".equalsIgnoreCase(name) && !"AVG".equalsIgnoreCase(name)
        && !"MIN".equalsIgnoreCase(name)) {
      return;
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
      if (analytic.getExpression() != null) {
        throw new IllegalArgumentException("ROW_NUMBER must not include an argument expression: " + analytic);
      }
      rowNumberWindows.add(new RowNumberWindow(analytic, List.copyOf(partitions), orderElements));
      return;
    }
    if ("RANK".equalsIgnoreCase(name)) {
      if (orderElements.isEmpty()) {
        throw new IllegalArgumentException("RANK requires an ORDER BY clause: " + analytic);
      }
      if (analytic.getExpression() != null) {
        throw new IllegalArgumentException("RANK must not include an argument expression: " + analytic);
      }
      rankWindows.add(new RankWindow(analytic, List.copyOf(partitions), orderElements));
      return;
    }
    if ("DENSE_RANK".equalsIgnoreCase(name)) {
      if (orderElements.isEmpty()) {
        throw new IllegalArgumentException("DENSE_RANK requires an ORDER BY clause: " + analytic);
      }
      if (analytic.getExpression() != null) {
        throw new IllegalArgumentException("DENSE_RANK must not include an argument expression: " + analytic);
      }
      denseRankWindows.add(new DenseRankWindow(analytic, List.copyOf(partitions), orderElements));
      return;
    }
    if ("PERCENT_RANK".equalsIgnoreCase(name)) {
      if (orderElements.isEmpty()) {
        throw new IllegalArgumentException("PERCENT_RANK requires an ORDER BY clause: " + analytic);
      }
      if (analytic.getExpression() != null) {
        throw new IllegalArgumentException("PERCENT_RANK must not include an argument expression: " + analytic);
      }
      percentRankWindows.add(new PercentRankWindow(analytic, List.copyOf(partitions), orderElements));
      return;
    }
    if ("CUME_DIST".equalsIgnoreCase(name)) {
      if (orderElements.isEmpty()) {
        throw new IllegalArgumentException("CUME_DIST requires an ORDER BY clause: " + analytic);
      }
      if (analytic.getExpression() != null) {
        throw new IllegalArgumentException("CUME_DIST must not include an argument expression: " + analytic);
      }
      cumeDistWindows.add(new CumeDistWindow(analytic, List.copyOf(partitions), orderElements));
      return;
    }
    if ("NTILE".equalsIgnoreCase(name)) {
      Expression bucketExpression = analytic.getExpression();
      if (bucketExpression == null) {
        throw new IllegalArgumentException("NTILE requires exactly one argument expression: " + analytic);
      }
      ntileWindows.add(new NtileWindow(analytic, List.copyOf(partitions), orderElements, bucketExpression));
      return;
    }
    if ("SUM".equalsIgnoreCase(name)) {
      Expression argument = analytic.getExpression();
      if (argument == null) {
        throw new IllegalArgumentException("SUM requires an argument expression: " + analytic);
      }
      sumWindows.add(new SumWindow(analytic, List.copyOf(partitions), orderElements, argument,
          analytic.isDistinct() || analytic.isUnique(), analytic.getWindowElement()));
      return;
    }
    if ("AVG".equalsIgnoreCase(name)) {
      Expression argument = analytic.getExpression();
      if (argument == null) {
        throw new IllegalArgumentException("AVG requires an argument expression: " + analytic);
      }
      avgWindows.add(new AvgWindow(analytic, List.copyOf(partitions), orderElements, argument,
          analytic.isDistinct() || analytic.isUnique(), analytic.getWindowElement()));
      return;
    }
    if ("MIN".equalsIgnoreCase(name)) {
      Expression argument = analytic.getExpression();
      if (argument == null) {
        throw new IllegalArgumentException("MIN requires an argument expression: " + analytic);
      }
      minWindows.add(new MinWindow(analytic, List.copyOf(partitions), orderElements, argument,
          analytic.isDistinct() || analytic.isUnique(), analytic.getWindowElement()));
      // return;
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

    final IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues;
    rowNumberValues = new IdentityHashMap<>();
    for (RowNumberWindow window : plan.rowNumberWindows()) {
      IdentityHashMap<GenericRecord, Long> values = computeRowNumbers(window, records, evaluator);
      rowNumberValues.put(window.expression(), values);
    }
    final IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rankValues;
    rankValues = new IdentityHashMap<>();
    for (RankWindow window : plan.rankWindows()) {
      IdentityHashMap<GenericRecord, Long> values = computeRank(window, records, evaluator);
      rankValues.put(window.expression(), values);
    }
    final IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> denseRankValues;
    denseRankValues = new IdentityHashMap<>();
    for (DenseRankWindow window : plan.denseRankWindows()) {
      IdentityHashMap<GenericRecord, Long> values = computeDenseRank(window, records, evaluator);
      denseRankValues.put(window.expression(), values);
    }
    final IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> percentRankValues;
    percentRankValues = new IdentityHashMap<>();
    for (PercentRankWindow window : plan.percentRankWindows()) {
      IdentityHashMap<GenericRecord, BigDecimal> values = computePercentRank(window, records, evaluator);
      percentRankValues.put(window.expression(), values);
    }
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, BigDecimal>> cumeDistValues;
    cumeDistValues = new IdentityHashMap<>();
    for (CumeDistWindow window : plan.cumeDistWindows()) {
      IdentityHashMap<GenericRecord, BigDecimal> values = computeCumeDist(window, records, evaluator);
      cumeDistValues.put(window.expression(), values);
    }
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> ntileValues;
    ntileValues = new IdentityHashMap<>();
    for (NtileWindow window : plan.ntileWindows()) {
      IdentityHashMap<GenericRecord, Long> values = computeNtile(window, records, evaluator);
      ntileValues.put(window.expression(), values);
    }
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> sumValues;
    sumValues = new IdentityHashMap<>();
    for (SumWindow window : plan.sumWindows()) {
      IdentityHashMap<GenericRecord, Object> values = computeSum(window, records, evaluator);
      sumValues.put(window.expression(), values);
    }
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> avgValues;
    avgValues = new IdentityHashMap<>();
    for (AvgWindow window : plan.avgWindows()) {
      IdentityHashMap<GenericRecord, Object> values = computeAvg(window, records, evaluator);
      avgValues.put(window.expression(), values);
    }
    IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> minValues;
    minValues = new IdentityHashMap<>();
    for (MinWindow window : plan.minWindows()) {
      IdentityHashMap<GenericRecord, Object> values = computeMin(window, records, evaluator);
      minValues.put(window.expression(), values);
    }
    return new WindowState(rowNumberValues, rankValues, denseRankValues, percentRankValues, cumeDistValues, ntileValues,
        sumValues, avgValues, minValues);
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
        while (groupEnd < partitionEnd
            && orderComponentsEqual(orderComponents, contexts.get(groupEnd).orderComponents())) {
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

  /**
   * Compute the SQL standard {@code NTILE} values for the supplied window
   * definition.
   *
   * @param window
   *          analytic window specification
   * @param records
   *          records participating in the computation
   * @param evaluator
   *          evaluator for partition, ordering and bucket expressions
   * @return mapping from {@link GenericRecord} to the computed tile index
   */
  private static IdentityHashMap<GenericRecord, Long> computeNtile(NtileWindow window, List<GenericRecord> records,
      ValueExpressionEvaluator evaluator) {
    List<RowContext> contexts = buildSortedContexts(window.partitionExpressions(), window.orderByElements(), records,
        evaluator);

    IdentityHashMap<GenericRecord, Long> values = new IdentityHashMap<>();

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

      RowContext firstContext = contexts.get(partitionStart);
      Object bucketValue = evaluator.eval(window.bucketExpression(), firstContext.record());
      long bucketCount = resolvePositiveBucketCount(bucketValue, window.expression());

      long baseSize = totalRows / bucketCount;
      long remainder = totalRows % bucketCount;
      long threshold = remainder * (baseSize + 1L);

      long processed = 0L;
      for (int j = partitionStart; j < partitionEnd; j++) {
        processed++;
        long tile;
        if (processed <= threshold) {
          long groupSize = baseSize + 1L;
          tile = (processed - 1L) / groupSize + 1L;
        } else {
          if (baseSize == 0L) {
            tile = remainder + 1L;
          } else {
            long indexAfterThreshold = processed - threshold;
            tile = remainder + (indexAfterThreshold - 1L) / baseSize + 1L;
          }
        }
        if (tile < 1L || tile > bucketCount) {
          throw new IllegalStateException(
              "Computed NTILE value out of bounds: " + tile + " for expression " + window.expression());
        }
        values.put(contexts.get(j).record(), tile);
      }

      index = partitionEnd;
    }

    return values;
  }

  private static IdentityHashMap<GenericRecord, Object> computeSum(SumWindow window, List<GenericRecord> records,
      ValueExpressionEvaluator evaluator) {

    List<RowContext> contexts = buildSortedContexts(window.partitionExpressions(), window.orderByElements(), records,
        evaluator);
    IdentityHashMap<GenericRecord, Object> values = new IdentityHashMap<>();
    if (contexts.isEmpty()) {
      return values;
    }

    List<Object> argumentValues = new ArrayList<>(contexts.size());
    for (RowContext context : contexts) {
      Object value = evaluator.eval(window.argument(), context.record());
      argumentValues.add(value);
    }

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
      applySumForPartition(window, contexts, argumentValues, partitionStart, partitionEnd, values);
      index = partitionEnd;
    }

    return values;
  }

  private static IdentityHashMap<GenericRecord, Object> computeAvg(AvgWindow window, List<GenericRecord> records,
      ValueExpressionEvaluator evaluator) {

    List<RowContext> contexts = buildSortedContexts(window.partitionExpressions(), window.orderByElements(), records,
        evaluator);
    IdentityHashMap<GenericRecord, Object> values = new IdentityHashMap<>();
    if (contexts.isEmpty()) {
      return values;
    }

    List<Object> argumentValues = new ArrayList<>(contexts.size());
    for (RowContext context : contexts) {
      Object value = evaluator.eval(window.argument(), context.record());
      argumentValues.add(value);
    }

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
      applyAvgForPartition(window, contexts, argumentValues, partitionStart, partitionEnd, values);
      index = partitionEnd;
    }

    return values;
  }

  private static void applySumForPartition(SumWindow window, List<RowContext> contexts, List<Object> argumentValues,
      int partitionStart, int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    if (partitionStart >= partitionEnd) {
      return;
    }
    boolean hasOrder = window.orderByElements() != null && !window.orderByElements().isEmpty();
    WindowElement element = window.windowElement();

    if (!hasOrder) {
      SumComputationState state = new SumComputationState();
      for (int i = partitionStart; i < partitionEnd; i++) {
        state.add(argumentValues.get(i));
      }
      Object result = state.result();
      for (int i = partitionStart; i < partitionEnd; i++) {
        values.put(contexts.get(i).record(), result);
      }
      return;
    }

    if (element == null) {
      computeDefaultRangeSum(contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }

    if (element.getType() == WindowElement.Type.RANGE) {
      computeRangeSum(element, contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }

    if (element.getType() == WindowElement.Type.ROWS) {
      computeRowsSum(element, contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }

    throw new IllegalArgumentException("Unsupported window frame type for SUM: " + element);
  }

  private static void computeDefaultRangeSum(List<RowContext> contexts, List<Object> argumentValues, int partitionStart,
      int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    SumComputationState running = new SumComputationState();
    int groupStart = partitionStart;
    while (groupStart < partitionEnd) {
      List<OrderComponent> order = contexts.get(groupStart).orderComponents();
      int groupEnd = groupStart;
      while (groupEnd < partitionEnd && orderComponentsEqual(order, contexts.get(groupEnd).orderComponents())) {
        Object value = argumentValues.get(groupEnd);
        running.add(value);
        groupEnd++;
      }
      Object result = running.result();
      for (int i = groupStart; i < groupEnd; i++) {
        values.put(contexts.get(i).record(), result);
      }
      groupStart = groupEnd;
    }
  }

  private static void applyAvgForPartition(AvgWindow window, List<RowContext> contexts, List<Object> argumentValues,
      int partitionStart, int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    if (partitionStart >= partitionEnd) {
      return;
    }
    boolean hasOrder = window.orderByElements() != null && !window.orderByElements().isEmpty();
    WindowElement element = window.windowElement();

    if (!hasOrder) {
      AvgComputationState state = new AvgComputationState();
      for (int i = partitionStart; i < partitionEnd; i++) {
        state.add(argumentValues.get(i));
      }
      Object result = state.result();
      for (int i = partitionStart; i < partitionEnd; i++) {
        values.put(contexts.get(i).record(), result);
      }
      return;
    }

    if (element == null) {
      computeDefaultRangeAvg(contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }

    if (element.getType() == WindowElement.Type.RANGE) {
      computeRangeAvg(element, contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }

    if (element.getType() == WindowElement.Type.ROWS) {
      computeRowsAvg(element, contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }

    throw new IllegalArgumentException("Unsupported window frame type for AVG: " + element);
  }

  private static void computeDefaultRangeAvg(List<RowContext> contexts, List<Object> argumentValues, int partitionStart,
      int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    AvgComputationState running = new AvgComputationState();
    int groupStart = partitionStart;
    while (groupStart < partitionEnd) {
      List<OrderComponent> order = contexts.get(groupStart).orderComponents();
      int groupEnd = groupStart;
      while (groupEnd < partitionEnd && orderComponentsEqual(order, contexts.get(groupEnd).orderComponents())) {
        Object value = argumentValues.get(groupEnd);
        running.add(value);
        groupEnd++;
      }
      Object result = running.result();
      for (int i = groupStart; i < groupEnd; i++) {
        values.put(contexts.get(i).record(), result);
      }
      groupStart = groupEnd;
    }
  }

  private static void computeRangeAvg(WindowElement element, List<RowContext> contexts, List<Object> argumentValues,
      int partitionStart, int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    WindowOffset offset = element.getOffset();
    WindowRange range = element.getRange();
    if (offset != null) {
      WindowOffset.Type type = offset.getType();
      if (type == WindowOffset.Type.PRECEDING || type == WindowOffset.Type.CURRENT) {
        computeDefaultRangeAvg(contexts, argumentValues, partitionStart, partitionEnd, values);
        return;
      }
      if (type == WindowOffset.Type.FOLLOWING) {
        AvgComputationState state = new AvgComputationState();
        for (int i = partitionStart; i < partitionEnd; i++) {
          state.add(argumentValues.get(i));
        }
        Object total = state.result();
        for (int i = partitionStart; i < partitionEnd; i++) {
          values.put(contexts.get(i).record(), total);
        }
        return;
      }
      throw new IllegalArgumentException("Unsupported RANGE frame specification for AVG: " + element);
    }
    if (range == null) {
      computeDefaultRangeAvg(contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }
    FrameBoundary start = rangeBoundary(range.getStart(), true);
    FrameBoundary end = rangeBoundary(range.getEnd(), false);
    if (start.type() == FrameBoundType.UNBOUNDED_PRECEDING && end.type() == FrameBoundType.CURRENT_ROW) {
      computeDefaultRangeAvg(contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }
    if (start.type() == FrameBoundType.UNBOUNDED_PRECEDING && end.type() == FrameBoundType.UNBOUNDED_FOLLOWING) {
      AvgComputationState state = new AvgComputationState();
      for (int i = partitionStart; i < partitionEnd; i++) {
        state.add(argumentValues.get(i));
      }
      Object total = state.result();
      for (int i = partitionStart; i < partitionEnd; i++) {
        values.put(contexts.get(i).record(), total);
      }
      return;
    }
    if (start.type() == FrameBoundType.CURRENT_ROW && end.type() == FrameBoundType.CURRENT_ROW) {
      int groupStart = partitionStart;
      while (groupStart < partitionEnd) {
        List<OrderComponent> order = contexts.get(groupStart).orderComponents();
        AvgComputationState state = new AvgComputationState();
        int groupEnd = groupStart;
        while (groupEnd < partitionEnd && orderComponentsEqual(order, contexts.get(groupEnd).orderComponents())) {
          state.add(argumentValues.get(groupEnd));
          groupEnd++;
        }
        Object total = state.result();
        for (int i = groupStart; i < groupEnd; i++) {
          values.put(contexts.get(i).record(), total);
        }
        groupStart = groupEnd;
      }
      return;
    }
    throw new IllegalArgumentException("Unsupported RANGE frame boundaries for AVG: " + element);
  }

  private static void computeRowsAvg(WindowElement element, List<RowContext> contexts, List<Object> argumentValues,
      int partitionStart, int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    FrameSpecification spec = rowsFrameSpecification(element);
    int partitionSize = partitionEnd - partitionStart;
    for (int i = 0; i < partitionSize; i++) {
      int absoluteIndex = partitionStart + i;
      int startIndex = resolveRowsStartIndex(spec.start(), i, partitionSize);
      int endIndex = resolveRowsEndIndex(spec.end(), i, partitionSize);
      if (startIndex > endIndex || startIndex >= partitionSize || endIndex < 0) {
        values.put(contexts.get(absoluteIndex).record(), null);
        continue;
      }
      int boundedStart = Math.max(0, startIndex);
      int boundedEnd = Math.min(partitionSize - 1, endIndex);
      AvgComputationState state = new AvgComputationState();
      for (int j = boundedStart; j <= boundedEnd; j++) {
        state.add(argumentValues.get(partitionStart + j));
      }
      values.put(contexts.get(absoluteIndex).record(), state.result());
    }
  }

  private static IdentityHashMap<GenericRecord, Object> computeMin(MinWindow window, List<GenericRecord> records,
      ValueExpressionEvaluator evaluator) {

    List<RowContext> contexts = buildSortedContexts(window.partitionExpressions(), window.orderByElements(), records,
        evaluator);
    IdentityHashMap<GenericRecord, Object> values = new IdentityHashMap<>();
    if (contexts.isEmpty()) {
      return values;
    }

    List<Object> argumentValues = new ArrayList<>(contexts.size());
    for (RowContext context : contexts) {
      Object value = evaluator.eval(window.argument(), context.record());
      argumentValues.add(value);
    }

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
      applyMinForPartition(window, contexts, argumentValues, partitionStart, partitionEnd, values);
      index = partitionEnd;
    }

    return values;
  }

  private static void applyMinForPartition(MinWindow window, List<RowContext> contexts, List<Object> argumentValues,
      int partitionStart, int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    if (partitionStart >= partitionEnd) {
      return;
    }
    boolean hasOrder = window.orderByElements() != null && !window.orderByElements().isEmpty();
    WindowElement element = window.windowElement();

    if (!hasOrder) {
      MinComputationState state = new MinComputationState();
      for (int i = partitionStart; i < partitionEnd; i++) {
        state.add(argumentValues.get(i));
      }
      Object result = state.result();
      for (int i = partitionStart; i < partitionEnd; i++) {
        values.put(contexts.get(i).record(), result);
      }
      return;
    }

    if (element == null) {
      computeDefaultRangeMin(contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }

    if (element.getType() == WindowElement.Type.RANGE) {
      computeRangeMin(element, contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }

    if (element.getType() == WindowElement.Type.ROWS) {
      computeRowsMin(element, contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }

    throw new IllegalArgumentException("Unsupported window frame type for MIN: " + element);
  }

  private static void computeDefaultRangeMin(List<RowContext> contexts, List<Object> argumentValues, int partitionStart,
      int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    MinComputationState running = new MinComputationState();
    int groupStart = partitionStart;
    while (groupStart < partitionEnd) {
      List<OrderComponent> order = contexts.get(groupStart).orderComponents();
      int groupEnd = groupStart;
      while (groupEnd < partitionEnd && orderComponentsEqual(order, contexts.get(groupEnd).orderComponents())) {
        Object value = argumentValues.get(groupEnd);
        running.add(value);
        groupEnd++;
      }
      Object result = running.result();
      for (int i = groupStart; i < groupEnd; i++) {
        values.put(contexts.get(i).record(), result);
      }
      groupStart = groupEnd;
    }
  }

  private static void computeRangeMin(WindowElement element, List<RowContext> contexts, List<Object> argumentValues,
      int partitionStart, int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    WindowOffset offset = element.getOffset();
    WindowRange range = element.getRange();
    if (offset != null) {
      WindowOffset.Type type = offset.getType();
      if (type == WindowOffset.Type.PRECEDING || type == WindowOffset.Type.CURRENT) {
        computeDefaultRangeMin(contexts, argumentValues, partitionStart, partitionEnd, values);
        return;
      }
      if (type == WindowOffset.Type.FOLLOWING) {
        MinComputationState state = new MinComputationState();
        for (int i = partitionStart; i < partitionEnd; i++) {
          state.add(argumentValues.get(i));
        }
        Object result = state.result();
        for (int i = partitionStart; i < partitionEnd; i++) {
          values.put(contexts.get(i).record(), result);
        }
        return;
      }
      throw new IllegalArgumentException("Unsupported RANGE frame specification for MIN: " + element);
    }
    if (range == null) {
      computeDefaultRangeMin(contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }
    FrameBoundary start = rangeBoundary(range.getStart(), true);
    FrameBoundary end = rangeBoundary(range.getEnd(), false);
    if (start.type() == FrameBoundType.UNBOUNDED_PRECEDING && end.type() == FrameBoundType.CURRENT_ROW) {
      computeDefaultRangeMin(contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }
    if (start.type() == FrameBoundType.UNBOUNDED_PRECEDING && end.type() == FrameBoundType.UNBOUNDED_FOLLOWING) {
      MinComputationState state = new MinComputationState();
      for (int i = partitionStart; i < partitionEnd; i++) {
        state.add(argumentValues.get(i));
      }
      Object result = state.result();
      for (int i = partitionStart; i < partitionEnd; i++) {
        values.put(contexts.get(i).record(), result);
      }
      return;
    }
    if (start.type() == FrameBoundType.CURRENT_ROW && end.type() == FrameBoundType.CURRENT_ROW) {
      int groupStart = partitionStart;
      while (groupStart < partitionEnd) {
        List<OrderComponent> order = contexts.get(groupStart).orderComponents();
        MinComputationState state = new MinComputationState();
        int groupEnd = groupStart;
        while (groupEnd < partitionEnd && orderComponentsEqual(order, contexts.get(groupEnd).orderComponents())) {
          state.add(argumentValues.get(groupEnd));
          groupEnd++;
        }
        Object result = state.result();
        for (int i = groupStart; i < groupEnd; i++) {
          values.put(contexts.get(i).record(), result);
        }
        groupStart = groupEnd;
      }
      return;
    }
    throw new IllegalArgumentException("Unsupported RANGE frame boundaries for MIN: " + element);
  }

  private static void computeRowsMin(WindowElement element, List<RowContext> contexts, List<Object> argumentValues,
      int partitionStart, int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    FrameSpecification spec = rowsFrameSpecification(element);
    int partitionSize = partitionEnd - partitionStart;
    for (int i = 0; i < partitionSize; i++) {
      int absoluteIndex = partitionStart + i;
      int startIndex = resolveRowsStartIndex(spec.start(), i, partitionSize);
      int endIndex = resolveRowsEndIndex(spec.end(), i, partitionSize);
      if (startIndex > endIndex || startIndex >= partitionSize || endIndex < 0) {
        values.put(contexts.get(absoluteIndex).record(), null);
        continue;
      }
      int boundedStart = Math.max(0, startIndex);
      int boundedEnd = Math.min(partitionSize - 1, endIndex);
      MinComputationState state = new MinComputationState();
      for (int j = boundedStart; j <= boundedEnd; j++) {
        state.add(argumentValues.get(partitionStart + j));
      }
      values.put(contexts.get(absoluteIndex).record(), state.result());
    }
  }

  private static void computeRangeSum(WindowElement element, List<RowContext> contexts, List<Object> argumentValues,
      int partitionStart, int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    WindowOffset offset = element.getOffset();
    WindowRange range = element.getRange();
    if (offset != null) {
      WindowOffset.Type type = offset.getType();
      if (type == WindowOffset.Type.PRECEDING || type == WindowOffset.Type.CURRENT) {
        computeDefaultRangeSum(contexts, argumentValues, partitionStart, partitionEnd, values);
        return;
      }
      if (type == WindowOffset.Type.FOLLOWING) {
        // UNBOUNDED FOLLOWING results in full partition sum
        SumComputationState state = new SumComputationState();
        for (int i = partitionStart; i < partitionEnd; i++) {
          state.add(argumentValues.get(i));
        }
        Object total = state.result();
        for (int i = partitionStart; i < partitionEnd; i++) {
          values.put(contexts.get(i).record(), total);
        }
        return;
      }
      throw new IllegalArgumentException("Unsupported RANGE frame specification for SUM: " + element);
    }
    if (range == null) {
      computeDefaultRangeSum(contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }
    FrameBoundary start = rangeBoundary(range.getStart(), true);
    FrameBoundary end = rangeBoundary(range.getEnd(), false);
    if (start.type() == FrameBoundType.UNBOUNDED_PRECEDING && end.type() == FrameBoundType.CURRENT_ROW) {
      computeDefaultRangeSum(contexts, argumentValues, partitionStart, partitionEnd, values);
      return;
    }
    if (start.type() == FrameBoundType.UNBOUNDED_PRECEDING && end.type() == FrameBoundType.UNBOUNDED_FOLLOWING) {
      SumComputationState state = new SumComputationState();
      for (int i = partitionStart; i < partitionEnd; i++) {
        state.add(argumentValues.get(i));
      }
      Object total = state.result();
      for (int i = partitionStart; i < partitionEnd; i++) {
        values.put(contexts.get(i).record(), total);
      }
      return;
    }
    if (start.type() == FrameBoundType.CURRENT_ROW && end.type() == FrameBoundType.CURRENT_ROW) {
      int groupStart = partitionStart;
      while (groupStart < partitionEnd) {
        List<OrderComponent> order = contexts.get(groupStart).orderComponents();
        SumComputationState state = new SumComputationState();
        int groupEnd = groupStart;
        while (groupEnd < partitionEnd && orderComponentsEqual(order, contexts.get(groupEnd).orderComponents())) {
          state.add(argumentValues.get(groupEnd));
          groupEnd++;
        }
        Object total = state.result();
        for (int i = groupStart; i < groupEnd; i++) {
          values.put(contexts.get(i).record(), total);
        }
        groupStart = groupEnd;
      }
      return;
    }
    throw new IllegalArgumentException("Unsupported RANGE frame boundaries for SUM: " + element);
  }

  private static FrameBoundary rangeBoundary(WindowOffset offset, boolean start) {
    if (offset == null) {
      return start ? FrameBoundary.unboundedPreceding() : FrameBoundary.currentRow();
    }
    if (offset.getExpression() != null) {
      throw new IllegalArgumentException("Only UNBOUNDED or CURRENT range offsets are supported: " + offset);
    }
    WindowOffset.Type type = offset.getType();
    if (type == WindowOffset.Type.PRECEDING) {
      return FrameBoundary.unboundedPreceding();
    }
    if (type == WindowOffset.Type.FOLLOWING) {
      return FrameBoundary.unboundedFollowing();
    }
    if (type == WindowOffset.Type.CURRENT) {
      return FrameBoundary.currentRow();
    }
    throw new IllegalArgumentException("Unsupported RANGE frame offset: " + offset);
  }

  private static void computeRowsSum(WindowElement element, List<RowContext> contexts, List<Object> argumentValues,
      int partitionStart, int partitionEnd, IdentityHashMap<GenericRecord, Object> values) {
    FrameSpecification spec = rowsFrameSpecification(element);
    int partitionSize = partitionEnd - partitionStart;
    for (int i = 0; i < partitionSize; i++) {
      int absoluteIndex = partitionStart + i;
      int startIndex = resolveRowsStartIndex(spec.start(), i, partitionSize);
      int endIndex = resolveRowsEndIndex(spec.end(), i, partitionSize);
      if (startIndex > endIndex || startIndex >= partitionSize || endIndex < 0) {
        values.put(contexts.get(absoluteIndex).record(), null);
        continue;
      }
      int boundedStart = Math.max(0, startIndex);
      int boundedEnd = Math.min(partitionSize - 1, endIndex);
      SumComputationState state = new SumComputationState();
      for (int j = boundedStart; j <= boundedEnd; j++) {
        state.add(argumentValues.get(partitionStart + j));
      }
      values.put(contexts.get(absoluteIndex).record(), state.result());
    }
  }

  private static FrameSpecification rowsFrameSpecification(WindowElement element) {
    WindowOffset offset = element.getOffset();
    WindowRange range = element.getRange();
    if (offset != null) {
      WindowOffset.Type type = offset.getType();
      FrameBoundary start;
      FrameBoundary end;
      if (type == WindowOffset.Type.FOLLOWING) {
        start = FrameBoundary.currentRow();
        end = rowsBoundary(offset, false);
      } else {
        start = rowsBoundary(offset, true);
        end = FrameBoundary.currentRow();
      }
      return new FrameSpecification(start, end);
    }
    if (range != null) {
      FrameBoundary start = rowsBoundary(range.getStart(), true);
      FrameBoundary end = rowsBoundary(range.getEnd(), false);
      return new FrameSpecification(start, end);
    }
    return new FrameSpecification(FrameBoundary.unboundedPreceding(), FrameBoundary.currentRow());
  }

  private static FrameBoundary rowsBoundary(WindowOffset offset, boolean start) {
    if (offset == null) {
      return start ? FrameBoundary.unboundedPreceding() : FrameBoundary.currentRow();
    }
    WindowOffset.Type type = offset.getType();
    if (offset.getExpression() == null) {
      if (type == WindowOffset.Type.PRECEDING) {
        return FrameBoundary.unboundedPreceding();
      }
      if (type == WindowOffset.Type.FOLLOWING) {
        return FrameBoundary.unboundedFollowing();
      }
      if (type == WindowOffset.Type.CURRENT) {
        return FrameBoundary.currentRow();
      }
      throw new IllegalArgumentException("Unsupported ROWS frame offset: " + offset);
    }
    Object literal = LiteralConverter.toLiteral(offset.getExpression());
    if (!(literal instanceof Number number)) {
      throw new IllegalArgumentException("ROWS frame offset must evaluate to a numeric literal: " + offset);
    }
    long value = number.longValue();
    if (value < 0L) {
      throw new IllegalArgumentException("ROWS frame offset must be non-negative: " + offset);
    }
    if (type == WindowOffset.Type.PRECEDING || type == WindowOffset.Type.EXPR) {
      return FrameBoundary.offsetPreceding(value);
    }
    if (type == WindowOffset.Type.FOLLOWING) {
      return FrameBoundary.offsetFollowing(value);
    }
    if (type == WindowOffset.Type.CURRENT) {
      return FrameBoundary.currentRow();
    }
    throw new IllegalArgumentException("Unsupported ROWS frame offset: " + offset);
  }

  private static int resolveRowsStartIndex(FrameBoundary boundary, int position, int partitionSize) {
    return switch (boundary.type()) {
      case UNBOUNDED_PRECEDING -> 0;
      case UNBOUNDED_FOLLOWING -> partitionSize;
      case CURRENT_ROW -> position;
      case OFFSET_PRECEDING -> position - safeLongToInt(boundary.offset());
      case OFFSET_FOLLOWING -> position + safeLongToInt(boundary.offset());
    };
  }

  private static int resolveRowsEndIndex(FrameBoundary boundary, int position, int partitionSize) {
    return switch (boundary.type()) {
      case UNBOUNDED_PRECEDING -> -1;
      case UNBOUNDED_FOLLOWING -> partitionSize - 1;
      case CURRENT_ROW -> position;
      case OFFSET_PRECEDING -> position - safeLongToInt(boundary.offset());
      case OFFSET_FOLLOWING -> position + safeLongToInt(boundary.offset());
    };
  }

  private static int safeLongToInt(long value) {
    if (value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Window frame offset exceeds supported range: " + value);
    }
    return (int) value;
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

  static int compareValues(Object left, Object right) {
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

  private static long resolvePositiveBucketCount(Object bucketValue, AnalyticExpression expression) {
    if (bucketValue == null) {
      throw new IllegalArgumentException("NTILE bucket expression must evaluate to a positive integer: " + expression);
    }
    BigDecimal decimalValue;
    try {
      decimalValue = bucketValue instanceof BigDecimal bd ? bd : new BigDecimal(bucketValue.toString());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "NTILE bucket expression produced a non-numeric value for expression " + expression + ": " + bucketValue, e);
    }
    long count;
    try {
      count = decimalValue.longValueExact();
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "NTILE bucket expression must resolve to an integer value for expression " + expression + ": " + bucketValue,
          e);
    }
    if (count <= 0L) {
      throw new IllegalArgumentException(
          "NTILE bucket expression must evaluate to a strictly positive integer for expression " + expression + ": "
              + bucketValue);
    }
    return count;
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

  private enum FrameBoundType {
    UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING, CURRENT_ROW, OFFSET_PRECEDING, OFFSET_FOLLOWING
  }

  private record FrameBoundary(FrameBoundType type, long offset) {
    static FrameBoundary unboundedPreceding() {
      return new FrameBoundary(FrameBoundType.UNBOUNDED_PRECEDING, 0L);
    }

    static FrameBoundary unboundedFollowing() {
      return new FrameBoundary(FrameBoundType.UNBOUNDED_FOLLOWING, 0L);
    }

    static FrameBoundary currentRow() {
      return new FrameBoundary(FrameBoundType.CURRENT_ROW, 0L);
    }

    static FrameBoundary offsetPreceding(long offset) {
      return new FrameBoundary(FrameBoundType.OFFSET_PRECEDING, offset);
    }

    static FrameBoundary offsetFollowing(long offset) {
      return new FrameBoundary(FrameBoundType.OFFSET_FOLLOWING, offset);
    }
  }

  private record FrameSpecification(FrameBoundary start, FrameBoundary end) {
  }

  private record RowContext(GenericRecord record, List<Object> partitionValues, List<OrderComponent> orderComponents,
      int originalIndex) {
  }

  private record OrderComponent(Object value, boolean ascending, OrderByElement.NullOrdering nullOrdering) {
  }
}
