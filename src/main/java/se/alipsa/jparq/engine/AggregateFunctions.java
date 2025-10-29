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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
   * @param hasRow
   *          whether the aggregate result represents a row to emit
   */
  public record AggregateResult(List<Object> values, List<Integer> sqlTypes, boolean hasRow) {
    /**
     * Canonical constructor ensuring result collections are present.
     *
     * @param values
     *          computed aggregate values
     * @param sqlTypes
     *          SQL types associated with each aggregate
     * @param hasRow
     *          whether the aggregate result should be exposed as a row
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
  public static AggregateResult evaluate(ParquetReader<GenericRecord> reader, AggregatePlan plan, Expression residual,
      Expression having, SubqueryExecutor subqueryExecutor) throws IOException {
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
            whereEval = new ExpressionEvaluator(schema, subqueryExecutor);
          }
          valueEval = new ValueExpressionEvaluator(schema, subqueryExecutor);
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

    boolean hasRow = true;
    if (having != null) {
      HavingEvaluator evaluator = new HavingEvaluator(plan, valueView, subqueryExecutor);
      hasRow = evaluator.eval(having);
    }

    return new AggregateResult(valueView, sqlTypeView, hasRow);
  }

  private static final class HavingEvaluator {
    private final AggregatePlan plan;
    private final List<Object> values;
    private final SubqueryExecutor subqueryExecutor;
    private final Map<String, Object> labelLookup;

    HavingEvaluator(AggregatePlan plan, List<Object> values, SubqueryExecutor subqueryExecutor) {
      this.plan = plan;
      this.values = values;
      this.subqueryExecutor = subqueryExecutor;
      this.labelLookup = buildLabelLookup(plan, values);
    }

    boolean eval(Expression expression) {
      Expression expr = ExpressionEvaluator.unwrapParenthesis(expression);
      if (expr == null) {
        return true;
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression and) {
        return eval(and.getLeftExpression()) && eval(and.getRightExpression());
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.conditional.OrExpression or) {
        return eval(or.getLeftExpression()) || eval(or.getRightExpression());
      }
      if (expr instanceof net.sf.jsqlparser.expression.NotExpression not) {
        return !eval(not.getExpression());
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.IsNullExpression isNull) {
        Object operand = value(isNull.getLeftExpression());
        boolean isNullVal = operand == null;
        return isNull.isNot() != isNullVal;
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.Between between) {
        Object val = value(between.getLeftExpression());
        Object low = value(between.getBetweenExpressionStart());
        Object high = value(between.getBetweenExpressionEnd());
        if (val == null || low == null || high == null) {
          return false;
        }
        int cmpLow = ExpressionEvaluator.typedCompare(val, low);
        int cmpHigh = ExpressionEvaluator.typedCompare(val, high);
        boolean in = cmpLow >= 0 && cmpHigh <= 0;
        return between.isNot() != in;
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.InExpression in) {
        return evalIn(in);
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.ExistsExpression exists) {
        return evalExists(exists);
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo eq) {
        return compare(eq.getLeftExpression(), eq.getRightExpression()) == 0;
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.GreaterThan gt) {
        return compare(gt.getLeftExpression(), gt.getRightExpression()) > 0;
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals ge) {
        return compare(ge.getLeftExpression(), ge.getRightExpression()) >= 0;
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.MinorThan lt) {
        return compare(lt.getLeftExpression(), lt.getRightExpression()) < 0;
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.MinorThanEquals le) {
        return compare(le.getLeftExpression(), le.getRightExpression()) <= 0;
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.SimilarToExpression) {
        throw new IllegalArgumentException("SIMILAR TO is not supported in HAVING clauses");
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.LikeExpression like) {
        return evalLike(like);
      }
      if (expr instanceof net.sf.jsqlparser.expression.BinaryExpression be) {
        return evalBinary(be);
      }
      throw new IllegalArgumentException("Unsupported HAVING expression: " + expr);
    }

    private boolean evalIn(net.sf.jsqlparser.expression.operators.relational.InExpression in) {
      Object leftVal = value(in.getLeftExpression());
      if (leftVal == null) {
        return false;
      }
      Expression right = in.getRightExpression();
      if (right instanceof ExpressionList<?> list) {
        boolean found = false;
        for (Expression e : list) {
          Object candidate = value(e);
          if (candidate != null && ExpressionEvaluator.typedCompare(leftVal, candidate) == 0) {
            found = true;
            break;
          }
        }
        return in.isNot() != found;
      }
      if (right instanceof net.sf.jsqlparser.statement.select.Select subSelect) {
        if (subqueryExecutor == null) {
          throw new IllegalStateException("IN subqueries require a subquery executor");
        }
        List<Object> subqueryValues = subqueryExecutor.execute(subSelect).firstColumnValues();
        boolean found = false;
        for (Object candidate : subqueryValues) {
          if (candidate != null && ExpressionEvaluator.typedCompare(leftVal, candidate) == 0) {
            found = true;
            break;
          }
        }
        return in.isNot() != found;
      }
      throw new IllegalArgumentException("Unsupported IN expression in HAVING clause: " + in);
    }

    private boolean evalExists(net.sf.jsqlparser.expression.operators.relational.ExistsExpression exists) {
      if (subqueryExecutor == null) {
        throw new IllegalStateException("EXISTS subqueries require a subquery executor");
      }
      if (!(exists.getRightExpression() instanceof net.sf.jsqlparser.statement.select.Select subSelect)) {
        throw new IllegalArgumentException("EXISTS requires a subquery");
      }
      boolean hasRows = !subqueryExecutor.execute(subSelect).rows().isEmpty();
      return exists.isNot() != hasRows;
    }

    private boolean evalLike(net.sf.jsqlparser.expression.operators.relational.LikeExpression like) {
      Object left = value(like.getLeftExpression());
      Object right = value(like.getRightExpression());
      if (left == null || right == null) {
        return false;
      }
      String leftText = left.toString();
      String pattern = right.toString();
      net.sf.jsqlparser.expression.operators.relational.LikeExpression.KeyWord keyWord = like.getLikeKeyWord();
      net.sf.jsqlparser.expression.operators.relational.LikeExpression.KeyWord effective = keyWord == null
          ? net.sf.jsqlparser.expression.operators.relational.LikeExpression.KeyWord.LIKE
          : keyWord;
      Character escapeChar = null;
      if (like.getEscape() != null) {
        Object escapeVal = value(like.getEscape());
        if (escapeVal != null) {
          String escape = escapeVal.toString();
          if (!escape.isEmpty()) {
            if (escape.length() != 1) {
              throw new IllegalArgumentException("LIKE escape clause must be a single character");
            }
            escapeChar = escape.charAt(0);
          }
        }
      }
      boolean matches;
      if (effective == net.sf.jsqlparser.expression.operators.relational.LikeExpression.KeyWord.SIMILAR_TO) {
        matches = se.alipsa.jparq.helper.StringExpressions.similarTo(leftText, pattern, escapeChar);
      } else {
        boolean caseInsensitive = effective
            == net.sf.jsqlparser.expression.operators.relational.LikeExpression.KeyWord.ILIKE;
        matches = se.alipsa.jparq.helper.StringExpressions.like(leftText, pattern, caseInsensitive, escapeChar);
      }
      return like.isNot() != matches;
    }

    private boolean evalBinary(net.sf.jsqlparser.expression.BinaryExpression be) {
      String op = be.getStringExpression();
      int cmp = compare(be.getLeftExpression(), be.getRightExpression());
      return switch (op) {
        case "=" -> cmp == 0;
        case "<" -> cmp < 0;
        case ">" -> cmp > 0;
        case "<=" -> cmp <= 0;
        case ">=" -> cmp >= 0;
        case "<>", "!=" -> cmp != 0;
        default -> throw new IllegalArgumentException("Unsupported operator in HAVING clause: " + op);
      };
    }

    private int compare(Expression left, Expression right) {
      Object leftVal = value(left);
      Object rightVal = value(right);
      if (leftVal == null || rightVal == null) {
        return -1;
      }
      return ExpressionEvaluator.typedCompare(leftVal, rightVal);
    }

    private Object value(Expression expression) {
      Expression expr = ExpressionEvaluator.unwrapParenthesis(expression);
      if (expr instanceof net.sf.jsqlparser.statement.select.Select subSelect) {
        return evaluateScalarSubquery(subSelect);
      }
      if (expr instanceof net.sf.jsqlparser.expression.SignedExpression signed) {
        Object inner = value(signed.getExpression());
        if (inner == null) {
          return null;
        }
        java.math.BigDecimal numeric = toBigDecimal(inner);
        return signed.getSign() == '-' ? numeric.negate() : numeric;
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.arithmetic.Addition add) {
        return arithmetic(add.getLeftExpression(), add.getRightExpression(), Operation.ADD);
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.arithmetic.Subtraction sub) {
        return arithmetic(sub.getLeftExpression(), sub.getRightExpression(), Operation.SUB);
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.arithmetic.Multiplication mul) {
        return arithmetic(mul.getLeftExpression(), mul.getRightExpression(), Operation.MUL);
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.arithmetic.Division div) {
        return arithmetic(div.getLeftExpression(), div.getRightExpression(), Operation.DIV);
      }
      if (expr instanceof net.sf.jsqlparser.expression.operators.arithmetic.Modulo mod) {
        return arithmetic(mod.getLeftExpression(), mod.getRightExpression(), Operation.MOD);
      }
      if (expr instanceof net.sf.jsqlparser.expression.Function func) {
        return aggregateValue(func);
      }
      if (expr instanceof net.sf.jsqlparser.schema.Column col) {
        return aliasValue(col.getColumnName());
      }
      return se.alipsa.jparq.helper.LiteralConverter.toLiteral(expr);
    }

    private Object arithmetic(Expression left, Expression right, Operation op) {
      Object leftVal = value(left);
      Object rightVal = value(right);
      if (leftVal == null || rightVal == null) {
        return null;
      }
      java.math.BigDecimal leftNum = toBigDecimal(leftVal);
      java.math.BigDecimal rightNum = toBigDecimal(rightVal);
      return switch (op) {
        case ADD -> leftNum.add(rightNum);
        case SUB -> leftNum.subtract(rightNum);
        case MUL -> leftNum.multiply(rightNum);
        case DIV -> rightNum.compareTo(java.math.BigDecimal.ZERO) == 0 ? null
            : leftNum.divide(rightNum, java.math.MathContext.DECIMAL64);
        case MOD -> rightNum.compareTo(java.math.BigDecimal.ZERO) == 0 ? null : leftNum.remainder(rightNum);
      };
    }

    private Object evaluateScalarSubquery(net.sf.jsqlparser.statement.select.Select subSelect) {
      if (subqueryExecutor == null) {
        throw new IllegalStateException("Scalar subqueries require a subquery executor");
      }
      SubqueryExecutor.SubqueryResult result = subqueryExecutor.execute(subSelect);
      if (result.rows().isEmpty()) {
        return null;
      }
      if (result.rows().size() > 1) {
        throw new IllegalArgumentException("Scalar subquery returned more than one row");
      }
      List<Object> row = result.rows().getFirst();
      if (row.isEmpty()) {
        return null;
      }
      if (row.size() > 1) {
        throw new IllegalArgumentException("Scalar subquery returned more than one column");
      }
      return row.getFirst();
    }

    private Object aggregateValue(net.sf.jsqlparser.expression.Function func) {
      AggregateType type = AggregateType.from(func.getName());
      if (type == null) {
        throw new IllegalArgumentException("Unsupported function in HAVING clause: " + func.getName());
      }
      for (int i = 0; i < plan.specs().size(); i++) {
        AggregateSpec spec = plan.specs().get(i);
        if (spec.type() != type) {
          continue;
        }
        if (spec.countStar() != (type == AggregateType.COUNT && func.isAllColumns())) {
          continue;
        }
        if (!spec.countStar()) {
          ExpressionList<?> params = func.getParameters();
          if (params == null || params.size() != spec.arguments().size()) {
            continue;
          }
          boolean matches = true;
          for (int j = 0; j < spec.arguments().size(); j++) {
            Expression expected = spec.arguments().get(j);
            Expression actual = params.get(j);
            if (!expected.toString().equals(actual.toString())) {
              matches = false;
              break;
            }
          }
          if (!matches) {
            continue;
          }
        }
        return values.get(i);
      }
      throw new IllegalArgumentException("HAVING references aggregate not present in SELECT: " + func);
    }

    private Object aliasValue(String name) {
      if (name == null) {
        return null;
      }
      return labelLookup.get(name.toLowerCase(Locale.ROOT));
    }

    private static Map<String, Object> buildLabelLookup(AggregatePlan plan, List<Object> values) {
      Map<String, Object> map = new HashMap<>();
      List<AggregateSpec> specs = plan.specs();
      for (int i = 0; i < specs.size() && i < values.size(); i++) {
        AggregateSpec spec = specs.get(i);
        String label = spec.label();
        if (label != null && !label.isBlank()) {
          map.put(label.toLowerCase(Locale.ROOT), values.get(i));
        }
      }
      return map;
    }

    private java.math.BigDecimal toBigDecimal(Object value) {
      if (value instanceof java.math.BigDecimal bd) {
        return bd;
      }
      if (value instanceof Number num) {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
          return java.math.BigDecimal.valueOf(num.longValue());
        }
        return java.math.BigDecimal.valueOf(num.doubleValue());
      }
      return new java.math.BigDecimal(value.toString());
    }

    private enum Operation {
      ADD, SUB, MUL, DIV, MOD
    }
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
