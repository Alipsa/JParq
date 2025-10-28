package se.alipsa.jparq.engine;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Utilities for detecting and evaluating aggregate functions in SELECT lists.
 */
public final class AggregateFunctions {

  private AggregateFunctions() {
  }

  /** Supported aggregate function names. */
  private enum AggregateType {
    COUNT, SUM, AVG, MAX, MIN, STRING_AGG;

    static AggregateType from(String name) {
      if (name == null) {
        return null;
      }
      return switch (name.toUpperCase(Locale.ROOT)) {
        case "COUNT" -> COUNT;
        case "SUM" -> SUM;
        case "AVG", "AVERAGE" -> AVG;
        case "MAX" -> MAX;
        case "MIN" -> MIN;
        case "STRING_AGG" -> STRING_AGG;
        default -> null;
      };
    }
  }

  /**
   * Plan describing aggregate functions present in a SELECT list.
   *
   * @param specs
   *          aggregate specifications in projection order
   */
  public record AggregatePlan(List<AggregateSpec> specs) {
    /**
     * Canonical constructor validating provided specification list.
     *
     * @param specs
     *          aggregate specifications in projection order
     */
    public AggregatePlan {
      Objects.requireNonNull(specs, "specs");
    }

    /**
     * Projection labels in the same order as aggregates.
     *
     * @return immutable list of labels
     */
    public List<String> labels() {
      List<String> labels = new ArrayList<>(specs.size());
      for (AggregateSpec spec : specs) {
        labels.add(spec.label());
      }
      return List.copyOf(labels);
    }
  }

  /**
   * Description of a single aggregate invocation.
   *
   * @param type
   *          aggregate function type
   * @param arguments
   *          function argument expressions (empty for COUNT(*))
   * @param label
   *          projection label exposed to JDBC
   * @param countStar
   *          true when representing COUNT(*)
   */
  public record AggregateSpec(AggregateType type, List<Expression> arguments, String label, boolean countStar) {
    /**
     * Canonical constructor enforcing invariant constraints for an aggregate
     * specification.
     *
     * @param type
     *          aggregate function type
     * @param arguments
     *          function argument expressions (empty for COUNT(*))
     * @param label
     *          projection label exposed to JDBC
     * @param countStar
     *          true when representing COUNT(*)
     */
    public AggregateSpec {
      Objects.requireNonNull(type, "type");
      Objects.requireNonNull(label, "label");
      arguments = arguments == null ? List.of() : List.copyOf(arguments);
      if (type == AggregateType.COUNT && countStar) {
        if (!arguments.isEmpty()) {
          throw new IllegalArgumentException("COUNT(*) cannot have arguments");
        }
      } else {
        if (arguments.isEmpty()) {
          throw new IllegalArgumentException("Aggregate function " + type + " requires arguments");
        }
      }
    }

    /**
     * Convenience accessor for the first argument.
     *
     * @return first argument expression or {@code null}
     */
    public Expression argument() {
      return arguments.isEmpty() ? null : arguments.getFirst();
    }
  }

  /**
   * Result of evaluating aggregates.
   *
   * @param values
   *          computed aggregate values
   * @param sqlTypes
   *          SQL types associated with each aggregate
   */
  public record AggregateResult(List<Object> values, List<Integer> sqlTypes) {
    /**
     * Canonical constructor ensuring result collections are present.
     *
     * @param values
     *          computed aggregate values
     * @param sqlTypes
     *          SQL types associated with each aggregate
     */
    public AggregateResult {
      Objects.requireNonNull(values, "values");
      Objects.requireNonNull(sqlTypes, "sqlTypes");
    }
  }

  /**
   * Attempt to build an {@link AggregatePlan} for the provided SELECT. Returns
   * {@code null} if the SELECT list does not consist solely of supported
   * aggregate functions.
   *
   * @param select
   *          parsed SELECT statement
   * @return aggregate plan for the select list, or {@code null} when not purely
   *         aggregate
   */
  public static AggregatePlan plan(SqlParser.Select select) {
    List<Expression> expressions = select.expressions();
    if (expressions.isEmpty()) {
      return null;
    }

    List<String> labels = select.labels();
    List<AggregateSpec> specs = new ArrayList<>(expressions.size());

    for (int i = 0; i < expressions.size(); i++) {
      Expression expr = expressions.get(i);
      if (!(expr instanceof Function func)) {
        return null; // Non-aggregate expression present
      }

      AggregateType type = AggregateType.from(func.getName());
      if (type == null) {
        return null; // Unsupported function
      }
      if (func.isDistinct()) {
        throw new IllegalArgumentException("DISTINCT aggregates are not supported yet");
      }

      boolean countStar = false;
      List<Expression> args = List.of();

      if (type == AggregateType.COUNT && func.isAllColumns()) {
        countStar = true;
      } else {
        args = parameters(func);
        if (args.isEmpty()) {
          throw new IllegalArgumentException("Aggregate function " + func + " requires an argument");
        }
        if (type == AggregateType.STRING_AGG) {
          if (args.size() != 2) {
            throw new IllegalArgumentException("STRING_AGG requires two arguments (value, separator)");
          }
        } else if (args.size() != 1) {
          throw new IllegalArgumentException("Only single-argument aggregate functions are supported");
        }
      }

      String label = (labels != null && i < labels.size() && labels.get(i) != null) ? labels.get(i) : func.toString();

      specs.add(new AggregateSpec(type, args, label, countStar));
    }

    return new AggregatePlan(List.copyOf(specs));
  }

  private static List<Expression> parameters(Function func) {
    ExpressionList<?> list = func.getParameters();
    if (list == null || list.isEmpty()) {
      return List.of();
    }
    List<Expression> params = new ArrayList<>(list.size());
    for (Expression expr : list) {
      params.add(expr);
    }
    return params;
  }

  /**
   * Evaluate aggregates by streaming the Parquet reader.
   *
   * @param reader
   *          parquet reader
   * @param plan
   *          aggregate plan
   * @param residual
   *          residual WHERE expression (may be null)
   * @return aggregate results and column metadata
   * @throws IOException
   *           if reading the parquet file fails
   */
  public static AggregateResult evaluate(ParquetReader<GenericRecord> reader, AggregatePlan plan, Expression residual)
      throws IOException {
    List<AggregateAccumulator> accs = new ArrayList<>(plan.specs().size());
    for (AggregateSpec spec : plan.specs()) {
      accs.add(AggregateAccumulator.create(spec));
    }

    try (ParquetReader<GenericRecord> autoClose = reader) {
      GenericRecord rec = autoClose.read();
      Schema schema = null;
      ExpressionEvaluator whereEval = null;
      ValueExpressionEvaluator valueEval = null;

      while (rec != null) {
        if (schema == null) {
          schema = rec.getSchema();
          if (residual != null) {
            whereEval = new ExpressionEvaluator(schema);
          }
          valueEval = new ValueExpressionEvaluator(schema);
        }

        boolean matches = residual == null || (whereEval != null && whereEval.eval(residual, rec));
        if (matches) {
          for (AggregateAccumulator acc : accs) {
            acc.add(valueEval, rec);
          }
        }

        rec = autoClose.read();
      }
    }

    List<Object> values = new ArrayList<>(accs.size());
    List<Integer> sqlTypes = new ArrayList<>(accs.size());
    for (AggregateAccumulator acc : accs) {
      values.add(acc.result());
      sqlTypes.add(acc.sqlType());
    }
    List<Object> valueView = List.copyOf(values);
    List<Integer> sqlTypeView = List.copyOf(sqlTypes);
    return new AggregateResult(valueView, sqlTypeView);
  }

  private abstract static class AggregateAccumulator {
    final AggregateSpec spec;
    Class<?> observedType;

    AggregateAccumulator(AggregateSpec spec) {
      this.spec = spec;
    }

    static AggregateAccumulator create(AggregateSpec spec) {
      return switch (spec.type()) {
        case COUNT -> new CountAccumulator(spec);
        case SUM -> new SumAccumulator(spec);
        case AVG -> new AvgAccumulator(spec);
        case MAX -> new ExtremumAccumulator(spec, true);
        case MIN -> new ExtremumAccumulator(spec, false);
        case STRING_AGG -> new StringAggAccumulator(spec);
      };
    }

    abstract void add(ValueExpressionEvaluator eval, GenericRecord record);

    abstract Object result();

    int sqlType() {
      return sqlTypeForClass(observedType);
    }

    Object evaluate(ValueExpressionEvaluator eval, GenericRecord record) {
      if (spec.countStar() || spec.arguments().isEmpty()) {
        return null;
      }
      return evaluate(eval, record, spec.arguments().getFirst());
    }

    Object evaluate(ValueExpressionEvaluator eval, GenericRecord record, Expression argument) {
      return argument == null ? null : eval.eval(argument, record);
    }

    void trackType(Object value) {
      if (value != null && observedType == null) {
        observedType = value.getClass();
      }
    }
  }

  private static final class CountAccumulator extends AggregateAccumulator {
    private long count = 0L;

    CountAccumulator(AggregateSpec spec) {
      super(spec);
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      if (spec.countStar()) {
        count++;
        return;
      }
      Object value = evaluate(eval, record);
      if (value != null) {
        trackType(value);
        count++;
      }
    }

    @Override
    Object result() {
      return count;
    }

    @Override
    int sqlType() {
      return Types.BIGINT;
    }
  }

  private static final class SumAccumulator extends AggregateAccumulator {
    private BigDecimal sum = BigDecimal.ZERO;
    private boolean seenValue = false;

    SumAccumulator(AggregateSpec spec) {
      super(spec);
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      Object value = evaluate(eval, record);
      if (value == null) {
        return;
      }
      trackType(value);
      sum = sum.add(toBigDecimal(value));
      seenValue = true;
    }

    @Override
    Object result() {
      if (!seenValue) {
        return null;
      }
      Class<?> type = observedType;
      if (type == null) {
        return sum.doubleValue();
      }
      if (type == Byte.class || type == Short.class || type == Integer.class || type == Long.class) {
        return sum.longValue();
      }
      if (type == Float.class || type == Double.class) {
        return sum.doubleValue();
      }
      if (type == BigDecimal.class) {
        return sum;
      }
      return sum.doubleValue();
    }

    @Override
    int sqlType() {
      Class<?> type = observedType;
      if (type == null) {
        return Types.DOUBLE;
      }
      if (type == Byte.class || type == Short.class || type == Integer.class || type == Long.class) {
        return Types.BIGINT;
      }
      if (type == Float.class) {
        return Types.REAL;
      }
      if (type == Double.class) {
        return Types.DOUBLE;
      }
      if (type == BigDecimal.class) {
        return Types.DECIMAL;
      }
      return Types.DOUBLE;
    }
  }

  private static final class AvgAccumulator extends AggregateAccumulator {
    private BigDecimal sum = BigDecimal.ZERO;
    private long count = 0L;

    AvgAccumulator(AggregateSpec spec) {
      super(spec);
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      Object value = evaluate(eval, record);
      if (value == null) {
        return;
      }
      trackType(value);
      sum = sum.add(toBigDecimal(value));
      count++;
    }

    @Override
    Object result() {
      if (count == 0L) {
        return null;
      }
      return sum.divide(BigDecimal.valueOf(count), java.math.MathContext.DECIMAL64).doubleValue();
    }

    @Override
    int sqlType() {
      return Types.DOUBLE;
    }
  }

  private static final class ExtremumAccumulator extends AggregateAccumulator {
    private Object extremum;
    private final boolean isMax;

    ExtremumAccumulator(AggregateSpec spec, boolean isMax) {
      super(spec);
      this.isMax = isMax;
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      Object value = evaluate(eval, record);
      if (value == null) {
        return;
      }
      trackType(value);
      if (extremum == null) {
        extremum = value;
        return;
      }
      int cmp = ExpressionEvaluator.typedCompare(value, extremum);
      if ((isMax && cmp > 0) || (!isMax && cmp < 0)) {
        extremum = value;
      }
    }

    @Override
    Object result() {
      return extremum;
    }
  }

  private static final class StringAggAccumulator extends AggregateAccumulator {
    private final List<String> parts = new ArrayList<>();
    private String separator;

    StringAggAccumulator(AggregateSpec spec) {
      super(spec);
      observedType = String.class;
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      List<Expression> arguments = spec.arguments();
      Object value = evaluate(eval, record, arguments.getFirst());
      if (value == null) {
        return;
      }
      if (arguments.size() > 1) {
        Object sep = evaluate(eval, record, arguments.get(1));
        if (sep != null) {
          separator = sep.toString();
        }
      }
      parts.add(value.toString());
    }

    @Override
    Object result() {
      if (parts.isEmpty()) {
        return null;
      }
      String sep = separator == null ? "" : separator;
      return String.join(sep, parts);
    }

    @Override
    int sqlType() {
      return Types.VARCHAR;
    }
  }

  private static BigDecimal toBigDecimal(Object value) {
    if (value instanceof BigDecimal bd) {
      return bd;
    }
    if (value instanceof Number num) {
      if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
        return BigDecimal.valueOf(num.longValue());
      }
      return BigDecimal.valueOf(num.doubleValue());
    }
    return new BigDecimal(value.toString());
  }

  private static int sqlTypeForClass(Class<?> cls) {
    if (cls == null) {
      return Types.OTHER;
    }
    if (cls == Byte.class || cls == Short.class || cls == Integer.class) {
      return Types.INTEGER;
    }
    if (cls == Long.class) {
      return Types.BIGINT;
    }
    if (cls == Float.class) {
      return Types.REAL;
    }
    if (cls == Double.class) {
      return Types.DOUBLE;
    }
    if (cls == BigDecimal.class) {
      return Types.DECIMAL;
    }
    if (cls == Boolean.class) {
      return Types.BOOLEAN;
    }
    if (cls == String.class || CharSequence.class.isAssignableFrom(cls)) {
      return Types.VARCHAR;
    }
    if (Date.class.isAssignableFrom(cls) || LocalDate.class.isAssignableFrom(cls)) {
      return Types.DATE;
    }
    if (Time.class.isAssignableFrom(cls) || LocalTime.class.isAssignableFrom(cls)) {
      return Types.TIME;
    }
    if (Timestamp.class.isAssignableFrom(cls) || LocalDateTime.class.isAssignableFrom(cls)) {
      return Types.TIMESTAMP;
    }
    return Types.OTHER;
  }
}
