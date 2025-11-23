package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JsonFunction;
import net.sf.jsqlparser.expression.JsonFunctionExpression;
import net.sf.jsqlparser.expression.JsonFunctionType;
import net.sf.jsqlparser.expression.JsonKeyValuePair;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.window.WindowState;
import se.alipsa.jparq.helper.DateTimeExpressions;
import se.alipsa.jparq.helper.JParqUtil;
import se.alipsa.jparq.helper.JsonExpressions;
import se.alipsa.jparq.helper.LiteralConverter;
import se.alipsa.jparq.helper.StringExpressions;

/**
 * Evaluates SELECT-list expressions (e.g. computed columns, {@code CASE}
 * expressions and supported SQL functions such as {@code COALESCE},
 * {@code CAST}, and SQL numeric functions) against a {@link GenericRecord}.
 */
public final class ValueExpressionEvaluator {

  private final Map<String, Schema> fieldSchemas;
  private final Map<String, String> caseInsensitiveIndex;
  private final Schema schema;
  private final SubqueryExecutor subqueryExecutor;
  private final List<String> outerQualifiers;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;
  private final Map<String, Map<String, String>> correlationContext;
  private final WindowState windowState;
  private ExpressionEvaluator conditionEvaluator;

  /**
   * Create an evaluator bound to the supplied Avro {@link Schema}.
   *
   * @param schema
   *          the Avro schema describing the available columns
   */
  public ValueExpressionEvaluator(Schema schema) {
    this(schema, null, List.of(), Map.of(), Map.of(), Map.of(), WindowState.empty());
  }

  /**
   * Create an evaluator bound to the supplied Avro {@link Schema} with optional
   * subquery execution support.
   *
   * @param schema
   *          the Avro schema describing the available columns
   * @param subqueryExecutor
   *          executor used for scalar subqueries (may be {@code null})
   */
  public ValueExpressionEvaluator(Schema schema, SubqueryExecutor subqueryExecutor) {
    this(schema, subqueryExecutor, List.of(), Map.of(), Map.of(), Map.of(), WindowState.empty());
  }

  /**
   * Create an evaluator bound to the supplied Avro {@link Schema} with optional
   * subquery execution support and correlated outer qualifiers.
   *
   * @param schema
   *          the Avro schema describing the available columns
   * @param subqueryExecutor
   *          executor used for scalar subqueries (may be {@code null})
   * @param outerQualifiers
   *          table names or aliases that belong to the outer query scope
   * @param qualifierColumnMapping
   *          mapping of qualifier (table/alias) to canonical column names used
   *          when resolving {@link Column} references inside expressions (may be
   *          {@code null})
   * @param unqualifiedColumnMapping
   *          mapping of unqualified column names to canonical names for
   *          expressions referencing columns that are unique across all tables
   *          (may be {@code null})
   */
  public ValueExpressionEvaluator(Schema schema, SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping) {
    this(schema, subqueryExecutor, outerQualifiers, qualifierColumnMapping, unqualifiedColumnMapping, Map.of(),
        WindowState.empty());
  }

  /**
   * Create an evaluator bound to the supplied Avro {@link Schema} with optional
   * subquery execution support, correlated outer qualifiers, and precomputed
   * analytic window state.
   *
   * @param schema
   *          the Avro schema describing the available columns
   * @param subqueryExecutor
   *          executor used for scalar subqueries (may be {@code null})
   * @param outerQualifiers
   *          table names or aliases that belong to the outer query scope
   * @param qualifierColumnMapping
   *          mapping of qualifier (table/alias) to canonical column names used
   *          when resolving {@link Column} references inside expressions (may be
   *          {@code null})
   * @param unqualifiedColumnMapping
   *          mapping of unqualified column names to canonical names for
   *          expressions referencing columns that are unique across all tables
   *          (may be {@code null})
   * @param correlationContext
   *          qualifier-aware correlation context used to resolve correlated
   *          columns in subqueries. This context is built from projection labels
   *          and canonical column names so that scalar and EXISTS subqueries can
   *          reference outer aliases even when they differ from the underlying
   *          field names.
   * @param windowState
   *          precomputed analytic function results available to projection
   *          expressions
   */
  public ValueExpressionEvaluator(Schema schema, SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping,
      Map<String, Map<String, String>> correlationContext, WindowState windowState) {
    Map<String, Schema> fs = new HashMap<>();
    Map<String, String> ci = new HashMap<>();
    for (Schema.Field f : schema.getFields()) {
      fs.put(f.name(), f.schema());
      ci.put(f.name().toLowerCase(Locale.ROOT), f.name());
    }
    this.fieldSchemas = Map.copyOf(fs);
    this.caseInsensitiveIndex = Map.copyOf(ci);
    this.schema = schema;
    this.subqueryExecutor = subqueryExecutor;
    this.outerQualifiers = outerQualifiers == null ? List.of() : List.copyOf(outerQualifiers);
    this.qualifierColumnMapping = ColumnMappingUtil.normaliseQualifierMapping(qualifierColumnMapping);
    this.unqualifiedColumnMapping = ColumnMappingUtil.normaliseUnqualifiedMapping(unqualifiedColumnMapping);
    Map<String, Map<String, String>> ctx = correlationContext == null ? Map.of() : correlationContext;
    this.correlationContext = ColumnMappingUtil.normaliseQualifierMapping(ctx);
    this.windowState = windowState == null ? WindowState.empty() : windowState;
  }

  /**
   * Evaluate a projection expression and return the resulting value.
   *
   * @param expression
   *          the expression to evaluate
   * @param record
   *          the current {@link GenericRecord}
   * @return the computed value (may be {@code null})
   */
  public Object eval(Expression expression, GenericRecord record) {
    return evalInternal(ExpressionEvaluator.unwrapParenthesis(expression), record);
  }

  private Object evalInternal(Expression expression, GenericRecord record) {
    if (expression instanceof ParenthesedExpressionList<?> pel) {
      int n = pel.size();
      if (n == 0) {
        return null;
      }
      if (n == 1) {
        return evalInternal(pel.getFirst(), record);
      }
      List<Object> vals = new ArrayList<>(n);
      for (Expression e : pel) {
        vals.add(evalInternal(e, record));
      }
      return vals;
    }

    if (expression instanceof CaseExpression caseExpr) {
      return evaluateCase(caseExpr, record);
    }

    if (expression instanceof TimeKeyExpression tk) {
      return DateTimeExpressions.evaluateTimeKey(tk);
    }
    if (expression instanceof CastExpression cast) {
      Object inner = evalInternal(cast.getLeftExpression(), record);
      if (inner == null) {
        return null;
      }
      return DateTimeExpressions.castLiteral(cast, inner);
    }
    if (expression instanceof SignedExpression se) {
      Object inner = evalInternal(se.getExpression(), record);
      if (inner == null) {
        return null;
      }
      if (!(inner instanceof Number) && !(inner instanceof BigDecimal)) {
        return LiteralConverter.toLiteral(se);
      }
      BigDecimal value = toBigDecimal(inner);
      return se.getSign() == '-' ? value.negate() : value;
    }
    if (expression instanceof Addition add) {
      return arithmetic(add.getLeftExpression(), add.getRightExpression(), record, Operation.ADD);
    }
    if (expression instanceof Subtraction sub) {
      return arithmetic(sub.getLeftExpression(), sub.getRightExpression(), record, Operation.SUB);
    }
    if (expression instanceof Multiplication mul) {
      return arithmetic(mul.getLeftExpression(), mul.getRightExpression(), record, Operation.MUL);
    }
    if (expression instanceof Division div) {
      return arithmetic(div.getLeftExpression(), div.getRightExpression(), record, Operation.DIV);
    }
    if (expression instanceof Modulo mod) {
      return arithmetic(mod.getLeftExpression(), mod.getRightExpression(), record, Operation.MOD);
    }
    if (expression instanceof Column col) {
      return columnValue(col, record);
    }
    if (expression instanceof ExtractExpression extract) {
      Object value = evalInternal(extract.getExpression(), record);
      return DateTimeExpressions.extract(extract.getName(), value);
    }
    if (expression instanceof IntervalExpression interval) {
      return DateTimeExpressions.toInterval(interval);
    }
    if (expression instanceof TrimFunction trim) {
      return evaluateTrim(trim, record);
    }
    if (expression instanceof CollateExpression collate) {
      return evalInternal(collate.getLeftExpression(), record);
    }
    if (expression instanceof ArrayConstructor array) {
      return evaluateArrayConstructor(array, record);
    }
    if (expression instanceof LikeExpression like) {
      return evaluateLike(like, record);
    }
    if (expression instanceof SimilarToExpression similar) {
      return evaluateSimilar(similar, record);
    }
    if (expression instanceof JsonFunction json) {
      return evaluateJsonFunction(json, record);
    }
    if (expression instanceof Function func) {
      return evaluateFunction(func, record);
    }
    if (expression instanceof AnalyticExpression analytic) {
      return evaluateAnalytic(analytic, record);
    }
    if (expression instanceof net.sf.jsqlparser.statement.select.Select subSelect) {
      return evaluateScalarSubquery(subSelect, record);
    }
    return LiteralConverter.toLiteral(expression);
  }

  private Object evaluateAnalytic(AnalyticExpression analytic, GenericRecord record) {
    if (windowState == null || windowState.isEmpty()) {
      throw new IllegalArgumentException("Analytic functions require precomputed window state: " + analytic);
    }
    String name = analytic.getName();
    if (name == null) {
      throw new IllegalArgumentException("Analytic function is missing a name: " + analytic);
    }
    if ("ROW_NUMBER".equalsIgnoreCase(name)) {
      return windowState.rowNumber(analytic, record);
    }
    if ("RANK".equalsIgnoreCase(name)) {
      return windowState.rank(analytic, record);
    }
    if ("DENSE_RANK".equalsIgnoreCase(name)) {
      return windowState.denseRank(analytic, record);
    }
    if ("PERCENT_RANK".equalsIgnoreCase(name)) {
      return windowState.percentRank(analytic, record);
    }
    if ("CUME_DIST".equalsIgnoreCase(name)) {
      return windowState.cumeDist(analytic, record);
    }
    if ("NTILE".equalsIgnoreCase(name)) {
      return windowState.ntile(analytic, record);
    }
    if ("COUNT".equalsIgnoreCase(name)) {
      return windowState.count(analytic, record);
    }
    if ("SUM".equalsIgnoreCase(name)) {
      return windowState.sum(analytic, record);
    }
    if ("AVG".equalsIgnoreCase(name)) {
      return windowState.avg(analytic, record);
    }
    if ("MIN".equalsIgnoreCase(name)) {
      return windowState.min(analytic, record);
    }
    if ("MAX".equalsIgnoreCase(name)) {
      return windowState.max(analytic, record);
    }
    if ("LAG".equalsIgnoreCase(name)) {
      return windowState.lag(analytic, record);
    }
    if ("LEAD".equalsIgnoreCase(name)) {
      return windowState.lead(analytic, record);
    }
    if ("NTH_VALUE".equalsIgnoreCase(name)) {
      return windowState.nthValue(analytic, record);
    }
    if ("FIRST_VALUE".equalsIgnoreCase(name)) {
      return windowState.firstValue(analytic, record);
    }
    if ("LAST_VALUE".equalsIgnoreCase(name)) {
      return windowState.lastValue(analytic, record);
    }
    throw new IllegalArgumentException("Unsupported analytic function: " + analytic);
  }

  private Object evaluateArrayConstructor(ArrayConstructor array, GenericRecord record) {
    ExpressionList<? extends Expression> expressionItems = array.getExpressions();
    if (expressionItems == null || expressionItems.isEmpty()) {
      return List.of();
    }
    List<Object> values = new ArrayList<>(expressionItems.size());
    for (Expression element : expressionItems) {
      values.add(evalInternal(element, record));
    }
    return homogenizeArrayValues(values);
  }

  private Object evaluateArrayFunction(Function func, GenericRecord record) {
    ExpressionList<? extends Expression> parameterExpressions = func.getParameters();
    if (parameterExpressions == null || parameterExpressions.isEmpty()) {
      return List.of();
    }
    Expression first = parameterExpressions.getFirst();
    if (first instanceof net.sf.jsqlparser.statement.select.Select select) {
      return homogenizeArrayValues(executeArraySubquery(select, record));
    }
    List<Object> values = new ArrayList<>(parameterExpressions.size());
    for (Expression element : parameterExpressions) {
      values.add(evalInternal(element, record));
    }
    return homogenizeArrayValues(values);
  }

  private List<Object> executeArraySubquery(net.sf.jsqlparser.statement.select.Select subSelect, GenericRecord record) {
    if (subqueryExecutor == null) {
      throw new IllegalStateException("ARRAY subqueries require a subquery executor");
    }
    CorrelatedSubqueryRewriter.Result rewritten = SubqueryCorrelatedFiltersIsolator.isolate(subSelect,
        correlationQualifiers(), correlationColumns(), correlationContext,
        (qualifier, column) -> resolveCorrelatedValue(qualifier, column, record));
    SubqueryExecutor.SubqueryResult result = rewritten.correlated()
        ? subqueryExecutor.executeRaw(rewritten.sql())
        : subqueryExecutor.execute(subSelect);
    if (result.columnLabels().isEmpty()) {
      return List.of();
    }
    if (result.columnLabels().size() > 1) {
      throw new IllegalArgumentException("ARRAY subquery must return exactly one column: " + subSelect);
    }
    return result.firstColumnValues();
  }

  private List<Object> homogenizeArrayValues(List<Object> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    NumericType numericTarget = NumericType.NONE;
    Class<?> objectTarget = null;
    for (Object value : values) {
      if (value == null) {
        continue;
      }
      if (value instanceof Number number) {
        numericTarget = NumericType.promote(numericTarget, NumericType.of(number));
        continue;
      }
      if (value instanceof CharSequence) {
        objectTarget = determineObjectTarget(objectTarget, String.class);
        continue;
      }
      if (value instanceof List<?>) {
        objectTarget = determineObjectTarget(objectTarget, List.class);
        continue;
      }
      objectTarget = determineObjectTarget(objectTarget, value.getClass());
    }
    if (numericTarget != NumericType.NONE) {
      return convertNumericValues(values, numericTarget);
    }
    if (objectTarget == null) {
      return List.copyOf(values);
    }
    List<Object> coerced = new ArrayList<>(values.size());
    for (Object value : values) {
      if (value == null) {
        coerced.add(null);
        continue;
      }
      if (objectTarget == String.class) {
        coerced.add(value.toString());
        continue;
      }
      if (objectTarget == List.class) {
        if (value instanceof List<?>) {
          coerced.add(value);
          continue;
        }
        throw new IllegalArgumentException("ARRAY elements must all be lists or scalars of the same type");
      }
      if (!objectTarget.equals(value.getClass())) {
        throw new IllegalArgumentException("ARRAY elements must share a common type; found " + objectTarget.getName()
            + " and " + value.getClass().getName());
      }
      coerced.add(value);
    }
    return List.copyOf(coerced);
  }

  private Class<?> determineObjectTarget(Class<?> current, Class<?> candidate) {
    if (current == null || current == candidate) {
      return candidate;
    }
    throw new IllegalArgumentException(
        "ARRAY elements must share a common type; found " + current.getName() + " and " + candidate.getName());
  }

  private List<Object> convertNumericValues(List<Object> values, NumericType target) {
    List<Object> converted = new ArrayList<>(values.size());
    for (Object value : values) {
      if (value == null) {
        converted.add(null);
        continue;
      }
      Number number = (Number) value;
      converted.add(switch (target) {
        case BIG_DECIMAL -> toBigDecimal(number);
        case DOUBLE -> number.doubleValue();
        case FLOAT -> number.floatValue();
        case LONG -> number.longValue();
        case INTEGER -> number.intValue();
        case SHORT -> number.shortValue();
        case BYTE -> number.byteValue();
        case NONE -> number;
      });
    }
    return List.copyOf(converted);
  }

  private enum NumericType {
    NONE, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BIG_DECIMAL;

    static NumericType of(Number value) {
      if (value instanceof BigDecimal) {
        return BIG_DECIMAL;
      }
      if (value instanceof Double) {
        return DOUBLE;
      }
      if (value instanceof Float) {
        return FLOAT;
      }
      if (value instanceof Long) {
        return LONG;
      }
      if (value instanceof Integer) {
        return INTEGER;
      }
      if (value instanceof Short) {
        return SHORT;
      }
      if (value instanceof Byte) {
        return BYTE;
      }
      if (value instanceof java.math.BigInteger) {
        return BIG_DECIMAL;
      }
      return DOUBLE;
    }

    static NumericType promote(NumericType current, NumericType candidate) {
      if (candidate == null || candidate == NONE) {
        return current;
      }
      if (current == NONE) {
        return candidate;
      }
      return values()[Math.max(current.ordinal(), candidate.ordinal())];
    }
  }

  private Object evaluateScalarSubquery(net.sf.jsqlparser.statement.select.Select subSelect, GenericRecord record) {
    if (subqueryExecutor == null) {
      throw new IllegalStateException("Scalar subqueries require a subquery executor");
    }
    CorrelatedSubqueryRewriter.Result rewritten = SubqueryCorrelatedFiltersIsolator.isolate(subSelect,
        correlationQualifiers(), correlationColumns(), correlationContext,
        (qualifier, column) -> resolveCorrelatedValue(qualifier, column, record));
    SubqueryExecutor.SubqueryResult result = rewritten.correlated()
        ? subqueryExecutor.executeRaw(rewritten.sql())
        : subqueryExecutor.execute(subSelect);
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

  private Object evaluateCase(CaseExpression caseExpr, GenericRecord record) {
    boolean isSimpleCase = caseExpr.getSwitchExpression() != null;
    Object switchValue = isSimpleCase ? evalInternal(caseExpr.getSwitchExpression(), record) : null;

    List<WhenClause> whenClauses = caseExpr.getWhenClauses();
    if (whenClauses != null) {
      for (WhenClause clause : whenClauses) {
        if (isSimpleCase) {
          Object whenValue = evalInternal(clause.getWhenExpression(), record);
          if (valuesEqual(switchValue, whenValue)) {
            return evalInternal(clause.getThenExpression(), record);
          }
        } else if (evaluateCondition(clause.getWhenExpression(), record)) {
          return evalInternal(clause.getThenExpression(), record);
        }
      }
    }

    Expression elseExpr = caseExpr.getElseExpression();
    if (elseExpr == null) {
      return null;
    }
    return evalInternal(elseExpr, record);
  }

  private boolean evaluateCondition(Expression condition, GenericRecord record) {
    if (condition == null) {
      return false;
    }
    if (conditionEvaluator == null) {
      Schema schemaToUse = schema != null ? schema : record.getSchema();
      conditionEvaluator = new ExpressionEvaluator(schemaToUse, subqueryExecutor, outerQualifiers,
          qualifierColumnMapping, unqualifiedColumnMapping);
    }
    return conditionEvaluator.eval(condition, record);
  }

  private static boolean valuesEqual(Object left, Object right) {
    if (left == null || right == null) {
      return false;
    }
    try {
      return ExpressionEvaluator.typedCompare(left, right) == 0;
    } catch (Exception e) {
      return left.equals(right);
    }
  }

  /**
   * Evaluate a SQL function call. Supports COALESCE, numeric functions (ABS,
   * CEIL, FLOOR, ROUND, SQRT, TRUNCATE, MOD, POWER, EXP, LOG, RAND, SIGN,
   * trigonometric variants, etc.), character functions, trimming/padding helpers,
   * pattern matching utilities, JSON helpers and Unicode conversions.
   *
   * @param func
   *          the function expression to evaluate
   * @param record
   *          the current record
   * @return the computed function result (may be {@code null})
   */
  private Object evaluateFunction(Function func, GenericRecord record) {
    String name = func.getName();
    if (name == null) {
      return null;
    }
    String upper = name.toUpperCase(Locale.ROOT);
    return switch (upper) {
      case "COALESCE" -> evaluateCoalesce(func, record);
      case "CHAR_LENGTH", "CHARACTER_LENGTH" -> StringExpressions.charLength(firstArgument(func, record));
      case "OCTET_LENGTH" -> StringExpressions.octetLength(firstArgument(func, record));
      case "POSITION" -> evaluatePosition(func, record);
      case "SUBSTRING" -> evaluateSubstring(func, record);
      case "LEFT" -> evaluateLeftOrRight(func, record, true);
      case "RIGHT" -> evaluateLeftOrRight(func, record, false);
      case "CONCAT" -> StringExpressions.concat(positionalArgs(func, record));
      case "UPPER" -> StringExpressions.upper(firstArgument(func, record));
      case "LOWER" -> StringExpressions.lower(firstArgument(func, record));
      case "LTRIM" -> evaluateTrimFunction(func, record, StringExpressions.TrimMode.LEADING);
      case "RTRIM" -> evaluateTrimFunction(func, record, StringExpressions.TrimMode.TRAILING);
      case "LPAD" -> evaluatePad(func, record, true);
      case "RPAD" -> evaluatePad(func, record, false);
      case "OVERLAY" -> evaluateOverlay(func, record);
      case "REPLACE" -> evaluateReplace(func, record);
      case "CHAR" -> evaluateChar(func, record);
      case "UNICODE" -> evaluateUnicode(func, record);
      case "NORMALIZE" -> evaluateNormalize(func, record);
      case "REGEXP_LIKE" -> evaluateRegexpLike(func, record);
      case "JSON_VALUE" -> evaluateJsonValue(func, record);
      case "JSON_QUERY" -> evaluateJsonQuery(func, record);
      case "JSON_OBJECT" -> JsonExpressions.jsonObject(positionalArgs(func, record));
      case "JSON_ARRAY" -> JsonExpressions.jsonArray(positionalArgs(func, record));
      case "ARRAY" -> evaluateArrayFunction(func, record);
      case "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "SQRT", "TRUNC", "TRUNCATE", "MOD", "POWER", "POW", "EXP", "LOG",
          "LOG10", "RAND", "RANDOM", "SIGN", "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2", "DEGREES",
          "RADIANS" ->
        evaluateNumericFunction(upper, func, record);
      default -> LiteralConverter.toLiteral(func);
    };
  }

  /**
   * Implementation of the COALESCE(expr1, expr2, ...) function.
   *
   * @param func
   *          the COALESCE function expression
   * @param record
   *          the current record
   * @return the first non-null argument value, or {@code null} if all are null
   */
  private Object evaluateCoalesce(Function func, GenericRecord record) {
    if (!(func.getParameters() instanceof ExpressionList<?> params) || params.isEmpty()) {
      return null;
    }
    for (Expression expr : params) {
      Object value = evalInternal(expr, record);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private Object evaluatePosition(Function func, GenericRecord record) {
    NamedArgResult named = namedArgs(func, record);
    Object substring;
    Object source;
    if (named != null) {
      substring = named.values().isEmpty() ? null : named.values().getFirst();
      source = named.values().size() > 1 ? named.values().get(1) : null;
    } else {
      List<Object> args = positionalArgs(func, record);
      if (args.size() < 2) {
        return null;
      }
      substring = args.get(0);
      source = args.get(1);
    }
    return StringExpressions.position(substring, source);
  }

  private Object evaluateSubstring(Function func, GenericRecord record) {
    NamedArgResult named = namedArgs(func, record);
    String input;
    Integer start = null;
    Integer length = null;
    if (named != null) {
      input = toStringValue(named.values().isEmpty() ? null : named.values().getFirst());
      List<String> names = named.names();
      List<Object> values = named.values();
      for (int i = 1; i < names.size(); i++) {
        String label = names.get(i);
        Object value = values.get(i);
        if ("FROM".equalsIgnoreCase(label)) {
          start = toInteger(value);
        } else if ("FOR".equalsIgnoreCase(label)) {
          length = toInteger(value);
        }
      }
    } else {
      List<Object> args = positionalArgs(func, record);
      if (args.isEmpty()) {
        return null;
      }
      input = toStringValue(args.get(0));
      if (args.size() > 1) {
        start = toInteger(args.get(1));
      }
      if (args.size() > 2) {
        length = toInteger(args.get(2));
      }
    }
    if (input == null || start == null) {
      return null;
    }
    return StringExpressions.substring(input, start, length);
  }

  private Object evaluateLeftOrRight(Function func, GenericRecord record, boolean left) {
    List<Object> args = positionalArgs(func, record);
    if (args.size() < 2) {
      return null;
    }
    String input = toStringValue(args.get(0));
    Integer count = toInteger(args.get(1));
    if (input == null || count == null) {
      return null;
    }
    return left ? StringExpressions.left(input, count) : StringExpressions.right(input, count);
  }

  private Object evaluateTrimFunction(Function func, GenericRecord record, StringExpressions.TrimMode mode) {
    List<Object> args = positionalArgs(func, record);
    if (args.isEmpty()) {
      return null;
    }
    String input = toStringValue(args.get(0));
    String chars = args.size() > 1 ? toStringValue(args.get(1)) : null;
    return StringExpressions.trim(input, chars, mode);
  }

  private Object evaluatePad(Function func, GenericRecord record, boolean left) {
    List<Object> args = positionalArgs(func, record);
    if (args.size() < 2) {
      return null;
    }
    String input = toStringValue(args.get(0));
    Integer length = toInteger(args.get(1));
    String fill = args.size() > 2 ? toStringValue(args.get(2)) : null;
    if (input == null || length == null) {
      return null;
    }
    return left ? StringExpressions.lpad(input, length, fill) : StringExpressions.rpad(input, length, fill);
  }

  private Object evaluateOverlay(Function func, GenericRecord record) {
    NamedArgResult named = namedArgs(func, record);
    String input;
    String replacement = null;
    Integer start = null;
    Integer length = null;
    if (named != null) {
      input = toStringValue(named.values().isEmpty() ? null : named.values().getFirst());
      List<String> names = named.names();
      List<Object> values = named.values();
      for (int i = 1; i < names.size(); i++) {
        String label = names.get(i);
        Object value = values.get(i);
        if ("PLACING".equalsIgnoreCase(label)) {
          replacement = toStringValue(value);
        } else if ("FROM".equalsIgnoreCase(label)) {
          start = toInteger(value);
        } else if ("FOR".equalsIgnoreCase(label)) {
          length = toInteger(value);
        }
      }
    } else {
      List<Object> args = positionalArgs(func, record);
      if (args.size() < 3) {
        return null;
      }
      input = toStringValue(args.get(0));
      replacement = toStringValue(args.get(1));
      start = toInteger(args.get(2));
      if (args.size() > 3) {
        length = toInteger(args.get(3));
      }
    }
    if (input == null || replacement == null || start == null) {
      return null;
    }
    return StringExpressions.overlay(input, replacement, start, length);
  }

  private Object evaluateReplace(Function func, GenericRecord record) {
    List<Object> args = positionalArgs(func, record);
    if (args.size() < 3) {
      return null;
    }
    String input = toStringValue(args.get(0));
    String search = toStringValue(args.get(1));
    String replacement = toStringValue(args.get(2));
    if (input == null || search == null || replacement == null) {
      return null;
    }
    return StringExpressions.replace(input, search, replacement);
  }

  private Object evaluateChar(Function func, GenericRecord record) {
    List<Object> args = positionalArgs(func, record);
    if (args.isEmpty()) {
      return null;
    }
    List<Object> codes = new ArrayList<>(args.size());
    for (Object arg : args) {
      Integer value = toInteger(arg);
      if (value != null) {
        codes.add(value);
      }
    }
    return StringExpressions.charFromCodes(codes);
  }

  private Object evaluateUnicode(Function func, GenericRecord record) {
    return StringExpressions.unicode(firstArgument(func, record));
  }

  private Object evaluateNumericFunction(String name, Function func, GenericRecord record) {
    List<Object> args = positionalArgs(func, record);
    return switch (name) {
      case "ABS" -> unaryBigDecimal(args, BigDecimal::abs);
      case "CEIL", "CEILING" -> unaryBigDecimal(args, v -> v.setScale(0, RoundingMode.CEILING));
      case "FLOOR" -> unaryBigDecimal(args, v -> v.setScale(0, RoundingMode.FLOOR));
      case "ROUND" -> roundFunction(args);
      case "SQRT" -> sqrtFunction(args);
      case "TRUNC", "TRUNCATE" -> truncateFunction(args);
      case "MOD" -> modFunction(args);
      case "POWER", "POW" -> powerFunction(args);
      case "EXP" -> expFunction(args);
      case "LOG" -> logFunction(args);
      case "LOG10" -> log10Function(args);
      case "RAND", "RANDOM" -> randFunction(args);
      case "SIGN" -> signFunction(args);
      case "SIN" -> trigFunction(args, Math::sin);
      case "COS" -> trigFunction(args, Math::cos);
      case "TAN" -> trigFunction(args, Math::tan);
      case "ASIN" -> inverseTrigFunction(args, Math::asin);
      case "ACOS" -> inverseTrigFunction(args, Math::acos);
      case "ATAN" -> inverseTrigFunction(args, Math::atan);
      case "ATAN2" -> atan2Function(args);
      case "DEGREES" -> trigFunction(args, Math::toDegrees);
      case "RADIANS" -> trigFunction(args, Math::toRadians);
      default -> null;
    };
  }

  private Object unaryBigDecimal(List<Object> args, java.util.function.Function<BigDecimal, BigDecimal> op) {
    if (args.isEmpty()) {
      return null;
    }
    Object value = args.getFirst();
    if (value == null) {
      return null;
    }
    BigDecimal number = toBigDecimal(value);
    return op.apply(number);
  }

  private Object roundFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Object value = args.getFirst();
    if (value == null) {
      return null;
    }
    BigDecimal number = toBigDecimal(value);
    int scale = 0;
    if (args.size() > 1) {
      Integer argScale = toInteger(args.get(1));
      if (argScale == null) {
        return null;
      }
      scale = argScale.intValue();
    }
    if (scale >= 0) {
      return number.setScale(scale, RoundingMode.HALF_UP);
    }
    BigDecimal factor = BigDecimal.TEN.pow(-scale);
    BigDecimal divided = number.divide(factor, 0, RoundingMode.HALF_UP);
    return divided.multiply(factor);
  }

  private Object truncateFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Object value = args.getFirst();
    if (value == null) {
      return null;
    }
    BigDecimal number = toBigDecimal(value);
    int scale = 0;
    if (args.size() > 1) {
      Integer argScale = toInteger(args.get(1));
      if (argScale == null) {
        return null;
      }
      scale = argScale.intValue();
    }
    if (scale >= 0) {
      return number.setScale(scale, RoundingMode.DOWN);
    }
    BigDecimal factor = BigDecimal.TEN.pow(-scale);
    BigDecimal divided = number.divide(factor, 0, RoundingMode.DOWN);
    return divided.multiply(factor);
  }

  private Object sqrtFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null || value < 0d) {
      return null;
    }
    return Math.sqrt(value);
  }

  private Object modFunction(List<Object> args) {
    if (args.size() < 2) {
      return null;
    }
    Object left = args.get(0);
    Object right = args.get(1);
    if (left == null || right == null) {
      return null;
    }
    BigDecimal dividend = toBigDecimal(left);
    BigDecimal divisor = toBigDecimal(right);
    if (divisor.compareTo(BigDecimal.ZERO) == 0) {
      return null;
    }
    return dividend.remainder(divisor);
  }

  private Object powerFunction(List<Object> args) {
    if (args.size() < 2) {
      return null;
    }
    Double base = toDouble(args.get(0));
    Double exponent = toDouble(args.get(1));
    if (base == null || exponent == null) {
      return null;
    }
    return Math.pow(base, exponent);
  }

  private Object expFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null) {
      return null;
    }
    return Math.exp(value);
  }

  private Object logFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Double first = toDouble(args.getFirst());
    if (first == null || first <= 0d) {
      return null;
    }
    if (args.size() == 1) {
      return Math.log(first);
    }
    Double base = first;
    Double value = toDouble(args.get(1));
    if (value == null || value <= 0d || base <= 0d || base.equals(1d)) {
      return null;
    }
    return Math.log(value) / Math.log(base);
  }

  private Object log10Function(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null || value <= 0d) {
      return null;
    }
    return Math.log10(value);
  }

  private Object randFunction(List<Object> args) {
    if (args.isEmpty()) {
      return Math.random();
    }
    Long seed = toLong(args.getFirst());
    if (seed == null) {
      return null;
    }
    return new Random(seed).nextDouble();
  }

  private Object signFunction(List<Object> args) {
    if (args.isEmpty()) {
      return null;
    }
    Object value = args.getFirst();
    if (value == null) {
      return null;
    }
    BigDecimal number = toBigDecimal(value);
    return Integer.valueOf(number.signum());
  }

  private Object trigFunction(List<Object> args, java.util.function.DoubleUnaryOperator operator) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null) {
      return null;
    }
    return operator.applyAsDouble(value);
  }

  private Object inverseTrigFunction(List<Object> args, java.util.function.DoubleUnaryOperator operator) {
    if (args.isEmpty()) {
      return null;
    }
    Double value = toDouble(args.getFirst());
    if (value == null) {
      return null;
    }
    return operator.applyAsDouble(value);
  }

  private Object atan2Function(List<Object> args) {
    if (args.size() < 2) {
      return null;
    }
    Double y = toDouble(args.get(0));
    Double x = toDouble(args.get(1));
    if (y == null || x == null) {
      return null;
    }
    return Math.atan2(y, x);
  }

  private Object evaluateNormalize(Function func, GenericRecord record) {
    List<Object> args = positionalArgs(func, record);
    if (args.isEmpty()) {
      return null;
    }
    Object value = args.get(0);
    Object form = args.size() > 1 ? args.get(1) : null;
    return StringExpressions.normalize(value, form);
  }

  private Object evaluateRegexpLike(Function func, GenericRecord record) {
    List<Object> args = positionalArgs(func, record);
    if (args.size() < 2) {
      return null;
    }
    String input = toStringValue(args.get(0));
    String pattern = toStringValue(args.get(1));
    if (input == null || pattern == null) {
      return null;
    }
    int flags = 0;
    if (args.size() > 2 && args.get(2) != null) {
      String opts = args.get(2).toString().toLowerCase(Locale.ROOT);
      for (int i = 0; i < opts.length(); i++) {
        char opt = opts.charAt(i);
        switch (opt) {
          case 'i' -> flags |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
          case 'm' -> flags |= Pattern.MULTILINE;
          case 'n', 's' -> flags |= Pattern.DOTALL;
          case 'x' -> flags |= Pattern.COMMENTS;
          case 'c' -> {
            // explicit case-sensitive flag, ignore since default
          }
          default -> {
            // ignore unknown flags
          }
        }
      }
    }
    Pattern compiled = Pattern.compile(pattern, flags);
    return compiled.matcher(input).find();
  }

  private Object evaluateLike(LikeExpression like, GenericRecord record) {
    Object left = evalInternal(like.getLeftExpression(), record);
    Object right = evalInternal(like.getRightExpression(), record);
    String input = toStringValue(left);
    String pattern = toStringValue(right);
    if (input == null || pattern == null) {
      return false;
    }
    LikeExpression.KeyWord keyWord = like.getLikeKeyWord();
    LikeExpression.KeyWord effectiveKeyword = keyWord == null ? LikeExpression.KeyWord.LIKE : keyWord;
    Character escapeChar = null;
    if (like.getEscape() != null) {
      Object escapeVal = evalInternal(like.getEscape(), record);
      String escape = toStringValue(escapeVal);
      if (escape != null && !escape.isEmpty()) {
        if (escape.length() != 1) {
          throw new IllegalArgumentException("LIKE escape clause must be a single character");
        }
        escapeChar = escape.charAt(0);
      }
    }
    if (effectiveKeyword == LikeExpression.KeyWord.SIMILAR_TO) {
      boolean matches = StringExpressions.similarTo(input, pattern, escapeChar);
      return like.isNot() ? !matches : matches;
    }
    boolean caseInsensitive = effectiveKeyword == LikeExpression.KeyWord.ILIKE;
    boolean matches = StringExpressions.like(input, pattern, caseInsensitive, escapeChar);
    return like.isNot() ? !matches : matches;
  }

  private Object evaluateSimilar(SimilarToExpression similar, GenericRecord record) {
    Object left = evalInternal(similar.getLeftExpression(), record);
    Object right = evalInternal(similar.getRightExpression(), record);
    String input = toStringValue(left);
    String pattern = toStringValue(right);
    if (input == null || pattern == null) {
      return false;
    }
    String escape = similar.getEscape();
    Character escapeChar = null;
    if (escape != null && !escape.isEmpty()) {
      if (escape.length() != 1) {
        throw new IllegalArgumentException("SIMILAR TO escape clause must be a single character");
      }
      escapeChar = escape.charAt(0);
    }
    boolean matches = StringExpressions.similarTo(input, pattern, escapeChar);
    return similar.isNot() ? !matches : matches;
  }

  private Object evaluateJsonValue(Function func, GenericRecord record) {
    List<Object> args = positionalArgs(func, record);
    if (args.size() < 2) {
      return null;
    }
    return JsonExpressions.jsonValue(args.get(0), args.get(1));
  }

  private Object evaluateJsonQuery(Function func, GenericRecord record) {
    List<Object> args = positionalArgs(func, record);
    if (args.size() < 2) {
      return null;
    }
    return JsonExpressions.jsonQuery(args.get(0), args.get(1));
  }

  private Object evaluateTrim(TrimFunction trim, GenericRecord record) {
    TrimFunction.TrimSpecification specValue = trim.getTrimSpecification();
    StringExpressions.TrimMode mode;
    if (specValue == TrimFunction.TrimSpecification.LEADING) {
      mode = StringExpressions.TrimMode.LEADING;
    } else if (specValue == TrimFunction.TrimSpecification.TRAILING) {
      mode = StringExpressions.TrimMode.TRAILING;
    } else {
      mode = StringExpressions.TrimMode.BOTH;
    }
    String characters = null;
    String target;
    if (trim.isUsingFromKeyword()) {
      characters = toStringValue(trim.getExpression() == null ? null : evalInternal(trim.getExpression(), record));
      target = toStringValue(trim.getFromExpression() == null ? null : evalInternal(trim.getFromExpression(), record));
    } else {
      target = toStringValue(trim.getExpression() == null ? null : evalInternal(trim.getExpression(), record));
    }
    return StringExpressions.trim(target, characters, mode);
  }

  private Object evaluateJsonFunction(JsonFunction json, GenericRecord record) {
    JsonFunctionType type = json.getType();
    if (type == JsonFunctionType.OBJECT || type == JsonFunctionType.POSTGRES_OBJECT
        || type == JsonFunctionType.MYSQL_OBJECT) {
      List<Object> args = new ArrayList<>();
      for (JsonKeyValuePair pair : json.getKeyValuePairs()) {
        Object keyObj = pair.getKey();
        String key = null;
        if (keyObj instanceof Expression keyExpr) {
          key = toStringValue(evalInternal(keyExpr, record));
        } else if (keyObj != null) {
          key = keyObj.toString();
        }
        if (key == null) {
          continue;
        }
        Object valueObj = pair.getValue();
        Object value;
        if (valueObj instanceof Expression valueExpr) {
          value = evalInternal(valueExpr, record);
        } else {
          value = valueObj;
        }
        args.add(key);
        args.add(value);
      }
      return JsonExpressions.jsonObject(args);
    }
    if (type == JsonFunctionType.ARRAY) {
      List<Object> values = new ArrayList<>();
      for (JsonFunctionExpression expr : json.getExpressions()) {
        values.add(evalInternal(expr.getExpression(), record));
      }
      return JsonExpressions.jsonArray(values);
    }
    return LiteralConverter.toLiteral(json);
  }

  private Object firstArgument(Function func, GenericRecord record) {
    List<Object> args = positionalArgs(func, record);
    if (args.isEmpty()) {
      return null;
    }
    return args.getFirst();
  }

  private List<Object> positionalArgs(Function func, GenericRecord record) {
    if (!(func.getParameters() instanceof ExpressionList<?> params) || params.isEmpty()) {
      return List.of();
    }
    List<Object> values = new ArrayList<>(params.size());
    for (Expression expr : params) {
      values.add(evalInternal(expr, record));
    }
    return Collections.unmodifiableList(values);
  }

  private NamedArgResult namedArgs(Function func, GenericRecord record) {
    if (!(func.getNamedParameters() instanceof NamedExpressionList<?> named)) {
      return null;
    }
    List<String> names = new ArrayList<>(named.getNames());
    List<Object> values = new ArrayList<>(names.size());
    for (Expression expr : named) {
      values.add(evalInternal(expr, record));
    }
    return new NamedArgResult(Collections.unmodifiableList(names), Collections.unmodifiableList(values));
  }

  private Integer toInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number num) {
      return num.intValue();
    }
    try {
      return new BigDecimal(value.toString()).intValue();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Expected numeric value but got " + value, e);
    }
  }

  private Long toLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number num) {
      return num.longValue();
    }
    try {
      return new BigDecimal(value.toString()).longValue();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Expected numeric value but got " + value, e);
    }
  }

  private Double toDouble(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number num) {
      return num.doubleValue();
    }
    try {
      return new BigDecimal(value.toString()).doubleValue();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Expected numeric value but got " + value, e);
    }
  }

  private String toStringValue(Object value) {
    return value == null ? null : value.toString();
  }

  private record NamedArgResult(List<String> names, List<Object> values) {
  }

  private Object arithmetic(Expression left, Expression right, GenericRecord record, Operation op) {
    Object leftVal = evalInternal(left, record);
    Object rightVal = evalInternal(right, record);
    if (leftVal == null || rightVal == null) {
      return null;
    }
    if (op == Operation.ADD) {
      Object temporal = DateTimeExpressions.plus(leftVal, rightVal);
      if (temporal != null) {
        return temporal;
      }
    }
    if (op == Operation.SUB) {
      Object temporal = DateTimeExpressions.minus(leftVal, rightVal);
      if (temporal != null) {
        return temporal;
      }
    }
    BigDecimal leftNum = toBigDecimal(leftVal);
    BigDecimal rightNum = toBigDecimal(rightVal);
    return switch (op) {
      case ADD -> leftNum.add(rightNum);
      case SUB -> leftNum.subtract(rightNum);
      case MUL -> leftNum.multiply(rightNum);
      case DIV -> rightNum.compareTo(BigDecimal.ZERO) == 0 ? null : leftNum.divide(rightNum, MathContext.DECIMAL64);
      case MOD -> rightNum.compareTo(BigDecimal.ZERO) == 0 ? null : leftNum.remainder(rightNum);
    };
  }

  private Object columnValue(Column column, GenericRecord record) {
    String qualifier = column.getTable() == null ? null : column.getTable().getName();
    String name = column.getColumnName();
    return resolveColumnValue(qualifier, name, record);
  }

  private Object resolveColumnValue(String qualifier, String columnName, GenericRecord record) {
    String canonical = canonicalFieldName(qualifier, columnName);
    return AvroCoercions.resolveColumnValue(canonical, record, fieldSchemas, caseInsensitiveIndex);
  }

  private Object resolveCorrelatedValue(String qualifier, String columnName, GenericRecord record) {
    String normalizedQualifier = JParqUtil.normalizeQualifier(qualifier);
    String normalizedColumn = columnName == null ? null : columnName.toLowerCase(Locale.ROOT);
    if (normalizedQualifier != null && normalizedColumn != null && !correlationContext.isEmpty()) {
      Map<String, String> mapping = correlationContext.get(normalizedQualifier);
      if (mapping != null) {
        String canonical = mapping.get(normalizedColumn);
        if (canonical != null) {
          assertCorrelatedFieldPresent(normalizedQualifier, normalizedColumn, canonical);
          Object val = AvroCoercions.resolveColumnValue(canonical, record, fieldSchemas, caseInsensitiveIndex);
          if (val != null) {
            return val;
          }
        }
      }
    }
    Object direct = resolveColumnValue(qualifier, columnName, record);
    if (direct != null) {
      return direct;
    }
    return resolveColumnValue(null, columnName, record);
  }

  /**
   * Guard correlated lookups to ensure the correlation context resolves to a
   * field that exists on the current record. When the correlation context
   * supplies a canonical column that is absent from the row schema, correlated
   * predicates silently evaluate against {@code null} values, masking alias
   * mismatches for derived tables.
   *
   * @param normalizedQualifier
   *          qualifier participating in the correlation lookup
   * @param normalizedColumn
   *          correlated column name normalized for lookup
   * @param canonical
   *          canonical column name derived from the correlation context
   */
  private void assertCorrelatedFieldPresent(String normalizedQualifier, String normalizedColumn, String canonical) {
    if (canonical == null || canonical.isBlank()) {
      return;
    }
    if (fieldSchemas.containsKey(canonical)) {
      return;
    }
    String fallback = caseInsensitiveIndex.get(canonical.toLowerCase(Locale.ROOT));
    if (fallback != null && fieldSchemas.containsKey(fallback)) {
      return;
    }
    throw new IllegalStateException("Correlation context mapped " + normalizedQualifier + "." + normalizedColumn
        + " to missing field '" + canonical + "' (available fields: " + fieldSchemas.keySet() + ")");
  }

  private List<String> correlationQualifiers() {
    if (correlationContext.isEmpty()) {
      return outerQualifiers;
    }
    Set<String> qualifiers = new LinkedHashSet<>(outerQualifiers);
    qualifiers.addAll(correlationContext.keySet());
    return List.copyOf(qualifiers);
  }

  private Set<String> correlationColumns() {
    if (correlationContext.isEmpty()) {
      return Set.of();
    }
    Set<String> columns = new LinkedHashSet<>();
    for (Map<String, String> mapping : correlationContext.values()) {
      if (mapping == null) {
        continue;
      }
      for (String key : mapping.keySet()) {
        if (!caseInsensitiveIndex.containsKey(key)) {
          columns.add(key);
        }
      }
    }
    return Set.copyOf(columns);
  }

  private String canonicalFieldName(String qualifier, String columnName) {
    return ColumnMappingUtil.canonicalFieldName(qualifier, columnName, qualifierColumnMapping, unqualifiedColumnMapping,
        caseInsensitiveIndex);
  }

  private BigDecimal toBigDecimal(Object value) {
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

  private enum Operation {
    ADD, SUB, MUL, DIV, MOD
  }
}
