package se.alipsa.jparq.engine.window;

import java.math.BigDecimal;
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
 * Utility methods for planning and evaluating SQL window functions that appear in SELECT lists.
 */
public final class WindowFunctions {

  private WindowFunctions() {
  }

  /**
   * Create a plan describing analytic window functions that require pre-computation.
   *
   * @param expressions
   *          expressions present in the SELECT list
   * @return a window plan capturing supported analytic expressions or {@code null} when none are present
   */
  public static WindowPlan plan(List<Expression> expressions) {
    if (expressions == null || expressions.isEmpty()) {
      return null;
    }
    List<RowNumberWindow> rowNumberWindows = new ArrayList<>();
    for (Expression expression : expressions) {
      if (expression == null) {
        continue;
      }
      expression.accept(new ExpressionVisitorAdapter<Void>() {
        @Override
        public <S> Void visit(AnalyticExpression analytic, S context) {
          registerAnalyticExpression(analytic, rowNumberWindows);
          return super.visit(analytic, context);
        }
      });
    }
    if (rowNumberWindows.isEmpty()) {
      return null;
    }
    return new WindowPlan(List.copyOf(rowNumberWindows));
  }

  private static void registerAnalyticExpression(AnalyticExpression analytic, List<RowNumberWindow> windows) {
    if (analytic == null) {
      return;
    }
    String name = analytic.getName();
    if (name == null || !"ROW_NUMBER".equalsIgnoreCase(name)) {
      return;
    }
    if (analytic.getExpression() != null) {
      throw new IllegalArgumentException("ROW_NUMBER must not include an argument expression: " + analytic);
    }
    if (analytic.isDistinct() || analytic.isUnique()) {
      throw new IllegalArgumentException("ROW_NUMBER does not support DISTINCT or UNIQUE modifiers: " + analytic);
    }
    if (analytic.getKeep() != null) {
      throw new IllegalArgumentException("ROW_NUMBER does not support KEEP clause: " + analytic);
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
    windows.add(new RowNumberWindow(analytic, List.copyOf(partitions), orderElements));
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
    for (RowNumberWindow window : plan.rowNumberWindows()) {
      IdentityHashMap<GenericRecord, Long> values = computeRowNumbers(window, records, evaluator);
      rowNumberValues.put(window.expression(), values);
    }
    return new WindowState(rowNumberValues);
  }

  private static IdentityHashMap<GenericRecord, Long> computeRowNumbers(RowNumberWindow window,
      List<GenericRecord> records, ValueExpressionEvaluator evaluator) {
    List<RowContext> contexts = new ArrayList<>(records.size());
    int index = 0;
    for (GenericRecord record : records) {
      List<Object> partitionValues = evaluateAll(window.partitionExpressions(), evaluator, record);
      List<OrderComponent> orderValues = buildOrderComponents(window.orderByElements(), evaluator, record);
      contexts.add(new RowContext(record, partitionValues, orderValues, index++));
    }
    contexts.sort((left, right) -> compareContexts(left, right, window.orderByElements()));

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
      boolean asc = !element.isAscDescPresent() || element.isAsc();
      OrderByElement.NullOrdering nullOrdering = element.getNullOrdering();
      Object value = evaluator.eval(element.getExpression(), record);
      components.add(new OrderComponent(value, asc, nullOrdering));
    }
    return List.copyOf(components);
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
      return compareNulls(lv, rv, left.ascending(), right.nullOrdering());
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
   * @return negative when {@code left < right}, zero when equal, otherwise positive
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
    int cmp = Arrays.compare(left, right);
    if (cmp != 0) {
      return cmp;
    }
    return Integer.compare(left.length, right.length);
  }

  /**
   * Description of analytic window operations that must be computed prior to projection evaluation.
   */
  public static final class WindowPlan {

    private final List<RowNumberWindow> rowNumberWindows;

    WindowPlan(List<RowNumberWindow> rowNumberWindows) {
      this.rowNumberWindows = rowNumberWindows == null ? List.of() : rowNumberWindows;
    }

    /**
     * Determine whether the plan contains any analytic window functions.
     *
     * @return {@code true} when the plan includes pre-computed window functions, otherwise {@code false}
     */
    public boolean isEmpty() {
      return rowNumberWindows.isEmpty();
    }

    /**
     * Access the ROW_NUMBER windows captured by this plan.
     *
     * @return immutable list of {@link RowNumberWindow} instances
     */
    public List<RowNumberWindow> rowNumberWindows() {
      return rowNumberWindows;
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

  private record RowContext(GenericRecord record, List<Object> partitionValues, List<OrderComponent> orderComponents,
      int originalIndex) {
  }

  private record OrderComponent(Object value, boolean ascending, OrderByElement.NullOrdering nullOrdering) {
  }

  /**
   * Immutable container for precomputed window values.
   */
  public static final class WindowState {

    private static final WindowState EMPTY = new WindowState(new IdentityHashMap<>());

    private final Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues;

    WindowState(Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberValues) {
      this.rowNumberValues = rowNumberValues == null ? Map.of() : Collections.unmodifiableMap(rowNumberValues);
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
      return rowNumberValues.isEmpty();
    }

    /**
     * Obtain the precomputed ROW_NUMBER value for the supplied expression and record.
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
  }
}
