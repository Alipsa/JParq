package se.alipsa.jparq.engine;

import static se.alipsa.jparq.engine.SqlExceptions.syntaxFailure;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
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
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TranscodingFunction;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.NumericType;
import se.alipsa.jparq.engine.function.AggregateFunctions;
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.engine.function.FunctionSupport;
import se.alipsa.jparq.engine.function.JsonFunctions;
import se.alipsa.jparq.engine.function.StringFunctions;
import se.alipsa.jparq.engine.window.WindowState;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.LiteralConverter;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Evaluates SELECT-list expressions (e.g. computed columns, {@code CASE}
 * expressions and supported SQL functions such as {@code COALESCE},
 * {@code CAST}, and SQL numeric functions) against a {@link GenericRecord}.
 */
public final class ValueExpressionEvaluator {

  private final Map<String, Schema> fieldSchemas;
  private final Map<Expression, NumericType> numericTypes = new IdentityHashMap<>();
  private final Map<Expression, Optional<Schema>> intervalTypes = new IdentityHashMap<>();
  private final Map<Expression, Optional<Schema>> jsonTypes = new IdentityHashMap<>();
  private final Map<CastExpression, Optional<DateTimeFunctions.BinaryCast>> binaryCasts = new IdentityHashMap<>();
  private final Map<TranscodingFunction, CastExpression> convertCasts = new IdentityHashMap<>();
  private final Map<String, String> caseInsensitiveIndex;
  private final Schema schema;
  private final SubqueryExecutor subqueryExecutor;
  private final List<String> outerQualifiers;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;
  private final Map<String, Map<String, String>> correlationContext;
  private final Map<String, Map<String, String>> effectiveQualifierMapping;
  private final WindowState windowState;
  private final FunctionSupport functionSupport;
  private ExpressionEvaluator conditionEvaluator;

  /**
   * Create an evaluator bound to the supplied Avro {@link Schema}.
   *
   * @param schema
   *          the Avro schema describing the available columns
   */
  public ValueExpressionEvaluator(Schema schema) {
    this(schema, null, CorrelationMappings.empty(), WindowState.empty());
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
    this(schema, subqueryExecutor, CorrelationMappings.empty(), WindowState.empty());
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
    this(schema, subqueryExecutor,
        CorrelationMappings.of(outerQualifiers, qualifierColumnMapping, unqualifiedColumnMapping), WindowState.empty());
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
   * @param mappings
   *          correlation-related mappings for resolving qualified and unqualified
   *          column references
   * @param windowState
   *          precomputed analytic function results available to projection
   *          expressions
   */
  public ValueExpressionEvaluator(Schema schema, SubqueryExecutor subqueryExecutor, CorrelationMappings mappings,
      WindowState windowState) {
    Map<String, Schema> fs = new HashMap<>();
    Map<String, String> ci = MaterializedNames.lookupIndex(schema);
    for (Schema.Field f : schema.getFields()) {
      fs.put(f.name(), f.schema());
    }
    this.fieldSchemas = Map.copyOf(fs);
    this.caseInsensitiveIndex = Map.copyOf(ci);
    this.schema = schema;
    this.subqueryExecutor = subqueryExecutor;
    this.outerQualifiers = mappings.outerQualifiers() == null ? List.of() : List.copyOf(mappings.outerQualifiers());
    this.qualifierColumnMapping = ColumnMappingUtil.normaliseQualifierMapping(mappings.qualifierColumnMapping());
    this.unqualifiedColumnMapping = ColumnMappingUtil.normaliseUnqualifiedMapping(mappings.unqualifiedColumnMapping());
    Map<String, Map<String, String>> ctx = mappings.correlationContext() == null
        ? Map.of()
        : mappings.correlationContext();
    this.correlationContext = ColumnMappingUtil.normaliseQualifierMapping(ctx);
    this.effectiveQualifierMapping = mergeQualifierMappings(this.qualifierColumnMapping, this.correlationContext);
    this.windowState = windowState == null ? WindowState.empty() : windowState;
    this.functionSupport = new FunctionSupport(subqueryExecutor, correlationQualifiers(), correlationColumns(),
        this.correlationContext, this::resolveCorrelatedValue, this::evalInternal);
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
   * @deprecated Use
   *             {@link #ValueExpressionEvaluator(Schema, SubqueryExecutor, CorrelationMappings, WindowState)}
   *             instead
   */
  @Deprecated(since = "0.6.0", forRemoval = true)
  public ValueExpressionEvaluator(Schema schema, SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping,
      Map<String, Map<String, String>> correlationContext, WindowState windowState) {
    this(schema, subqueryExecutor,
        new CorrelationMappings(outerQualifiers, qualifierColumnMapping, unqualifiedColumnMapping, correlationContext),
        windowState);
  }

  /**
   * Evaluate a projection expression and return the resulting value.
   *
   * @param expression
   *          the expression to evaluate
   * @param record
   *          the current {@link GenericRecord}
   * @return the computed value (may be {@code null})
   * @throws IllegalArgumentException
   *           if an expression is unsupported or a unary sign has an invalid
   *           operand; the cause carries SQLState 42000 for unsupported types or
   *           expressions, 22018 for numeric conversion failures, or 22003 for
   *           BIGINT overflow
   */
  public Object eval(Expression expression, GenericRecord record) {
    return evalInternal(ExpressionEvaluator.unwrapParenthesis(expression), record);
  }

  /**
   * Applies the expression's common interval declaration after lazy evaluation.
   *
   * @param expression
   *          expression whose result type is known independently of its branch
   * @param record
   *          current input row
   * @return computed value with the expression's common interval declaration
   */
  private Object evalInternal(Expression expression, GenericRecord record) {
    Expression unwrapped = ExpressionEvaluator.unwrapParenthesis(expression);
    Object value = evalUnconverted(unwrapped, record);
    if (value instanceof TemporalInterval interval) {
      Schema declaration = intervalSchema(unwrapped);
      return declaration == null ? interval : se.alipsa.jparq.helper.IntervalType.project(interval, declaration);
    }
    return value;
  }

  /**
   * Resolves a common interval declaration once for an expression.
   *
   * @param expression
   *          interval expression
   * @return declaration, or null when unknown
   */
  private Schema intervalSchema(Expression expression) {
    return intervalTypes
        .computeIfAbsent(expression, shape -> Optional.ofNullable(IntervalExpressionTypes.resolve(shape, column -> {
          try {
            String name = resolveColumnName(column);
            return name == null ? null : fieldSchemas.get(name);
          } catch (IllegalArgumentException unresolved) {
            return null;
          }
        }, select -> subqueryExecutor == null ? null : subqueryExecutor.projectionSchema(select)))).orElse(null);
  }

  private Object evalUnconverted(Expression expression, GenericRecord record) {
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
      Object value = evaluateCase(caseExpr, record);
      NumericType type = numericType(caseExpr);
      if (type == null || value == null) {
        return value;
      }
      try {
        return NumericExpressionTypes.convert(type, value);
      } catch (SQLException e) {
        throw new IllegalArgumentException("Cannot convert CASE result to its common numeric type", e);
      }
    }

    if (expression instanceof TimeKeyExpression tk) {
      return DateTimeFunctions.evaluateTimeKey(tk);
    }
    if (expression instanceof CastExpression cast) {
      Object inner = evalInternal(cast.getLeftExpression(), record);
      return evaluateCast(cast, inner);
    }
    if (expression instanceof SignedExpression se) {
      return LiteralConverter.applySign(evalInternal(se.getExpression(), record), se);
    }
    if (expression instanceof Concat concat) {
      return concatenate(concat.getLeftExpression(), concat.getRightExpression(), record);
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
      // Direct evaluator callers can supply raw JSqlParser expressions without
      // statement normalization. Apply the same keyword conversion for them.
      TimeKeyExpression tk = SqlExpressionNormalizer.timeKeyword(col);
      if (tk != null) {
        return DateTimeFunctions.evaluateTimeKey(tk);
      }
      return columnValue(col, record);
    }
    if (expression instanceof ExtractExpression extract) {
      Object value = evalInternal(extract.getExpression(), record);
      return DateTimeFunctions.extract(extract.getName(), value);
    }
    if (expression instanceof IntervalExpression interval) {
      return DateTimeFunctions.toInterval(interval);
    }
    if (expression instanceof TrimFunction trim) {
      return functionSupport.evaluate(trim, record);
    }
    if (expression instanceof CollateExpression collate) {
      return evalInternal(collate.getLeftExpression(), record);
    }
    if (expression instanceof ArrayConstructor array) {
      return functionSupport.evaluate(array, record);
    }
    if (BooleanExpressions.isPredicate(expression)) {
      return conditionEvaluator(record).evalTruth(expression, record);
    }
    if (expression instanceof JsonFunction json) {
      return evaluateJsonFunction(json, record);
    }
    if (expression instanceof TranscodingFunction convert) {
      return evaluateConvert(convert, record);
    }
    if (expression instanceof Function func) {
      return functionSupport.evaluate(func, record);
    }
    if (expression instanceof AnalyticExpression analytic) {
      return evaluateAnalytic(analytic, record);
    }
    if (expression instanceof net.sf.jsqlparser.statement.select.Select subSelect) {
      return evaluateScalarSubquery(subSelect, record);
    }
    if (LiteralConverter.isLiteral(expression)) {
      return LiteralConverter.toLiteral(expression);
    }
    throw syntaxFailure("Unsupported expression: " + SqlExceptions.describe(expression));
  }

  private Object evaluateConvert(TranscodingFunction convert, GenericRecord record) {
    if (convert.isTranscodeStyle()) {
      Object value = evalInternal(convert.getExpression(), record);
      return StringFunctions.convertCharset(value, convert.getTranscodingName());
    }
    CastExpression cast = convertCasts.computeIfAbsent(convert, this::convertCast);
    Object value = evalInternal(cast.getLeftExpression(), record);
    return cast.getColDataType() == null ? value : evaluateCast(cast, value);
  }

  /**
   * Converts a current row value using cached binary rules or the ordinary CAST
   * path.
   *
   * @param cast
   *          stable CAST expression
   * @param value
   *          current row value
   * @return converted value, possibly null
   * @throws IllegalArgumentException
   *           if the conversion is invalid
   */
  private Object evaluateCast(CastExpression cast, Object value) {
    DateTimeFunctions.BinaryCast binary = binaryCasts
        .computeIfAbsent(cast, shape -> Optional.ofNullable(DateTimeFunctions.compileBinaryCast(shape))).orElse(null);
    return binary == null ? DateTimeFunctions.castLiteral(cast, value) : binary.convert(value);
  }

  /**
   * Resolves one stable CAST shape for a cast-style CONVERT expression.
   *
   * @param convert
   *          parsed CONVERT expression
   * @return equivalent CAST shape retaining the operand and declaration
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if the arguments are ambiguous
   */
  private CastExpression convertCast(TranscodingFunction convert) {
    return CastExpressions.requireCast(convert, this::looksLikeSchemaColumn);
  }

  private boolean looksLikeSchemaColumn(String name) {
    if (name == null) {
      return false;
    }
    String normalized = Identifier.lookupKey(name);
    return normalized != null && caseInsensitiveIndex.containsKey(normalized);
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

  private Object evaluateScalarSubquery(net.sf.jsqlparser.statement.select.Select subSelect, GenericRecord record) {
    if (subqueryExecutor == null) {
      throw new IllegalStateException("Scalar subqueries require a subquery executor");
    }
    SubqueryExecutor.SubqueryResult result = subqueryExecutor.executeCorrelated(subSelect, correlationQualifiers(),
        correlationColumns(), correlationContext,
        (qualifier, column) -> resolveCorrelatedValue(qualifier, column, record));
    return result.scalarValue();
  }

  /**
   * Evaluates the selected CASE result using ordinary SQL value decoding.
   *
   * @param caseExpr
   *          CASE expression
   * @param record
   *          current input row
   * @return selected value, or null without a matching branch
   */
  private Object evaluateCase(CaseExpression caseExpr, GenericRecord record) {
    return evaluateCase(caseExpr, record, this::evalInternal);
  }

  /**
   * Selects a CASE branch once and evaluates it with the requested value decoder.
   * Conditions and switch operands keep their ordinary SQL evaluation.
   *
   * @param caseExpr
   *          CASE expression
   * @param record
   *          current input row
   * @param resultEvaluator
   *          selected result evaluator
   * @return selected value, or null without a matching branch
   */
  private Object evaluateCase(CaseExpression caseExpr, GenericRecord record,
      BiFunction<Expression, GenericRecord, Object> resultEvaluator) {
    boolean isSimpleCase = caseExpr.getSwitchExpression() != null;
    Object switchValue = isSimpleCase ? evalInternal(caseExpr.getSwitchExpression(), record) : null;

    List<WhenClause> whenClauses = caseExpr.getWhenClauses();
    if (whenClauses != null) {
      for (WhenClause clause : whenClauses) {
        if (isSimpleCase) {
          Object whenValue = evalInternal(clause.getWhenExpression(), record);
          if (valuesEqual(switchValue, whenValue)) {
            return resultEvaluator.apply(clause.getThenExpression(), record);
          }
        } else if (evaluateCondition(clause.getWhenExpression(), record)) {
          return resultEvaluator.apply(clause.getThenExpression(), record);
        }
      }
    }

    Expression elseExpr = caseExpr.getElseExpression();
    if (elseExpr == null) {
      return null;
    }
    return resultEvaluator.apply(elseExpr, record);
  }

  /**
   * Evaluates a CASE WHEN condition using the SQL TRUE selection boundary.
   *
   * @param condition
   *          the WHEN condition (may be {@code null})
   * @param record
   *          the current record
   * @return {@code true} only when the condition is SQL TRUE; UNKNOWN and
   *         {@code null} conditions do not match
   */
  private boolean evaluateCondition(Expression condition, GenericRecord record) {
    return condition != null && conditionEvaluator(record).eval(condition, record);
  }

  /**
   * Lazily creates the predicate evaluator shared by CASE WHEN and Boolean
   * values.
   *
   * @param record
   *          the current record, used to resolve the schema when the evaluator
   *          was created without one
   * @return the shared evaluator, carrying this evaluator's window state so
   *         analytic expressions keep working inside predicates
   */
  private ExpressionEvaluator conditionEvaluator(GenericRecord record) {
    if (conditionEvaluator == null) {
      Schema schemaToUse = schema != null ? schema : record.getSchema();
      conditionEvaluator = new ExpressionEvaluator(schemaToUse, subqueryExecutor, outerQualifiers,
          qualifierColumnMapping, unqualifiedColumnMapping, windowState);
    }
    return conditionEvaluator;
  }

  private static boolean valuesEqual(Object left, Object right) {
    if (left == null || right == null) {
      return false;
    }
    try {
      return ExpressionEvaluator.typedCompare(left, right) == 0;
    } catch (RuntimeException e) {
      if (SqlExceptions.findSqlStateFailure(e) != null) {
        throw e;
      }
      return left.equals(right);
    }
  }

  private Object evaluateJsonFunction(JsonFunction json, GenericRecord record) {
    JsonFunctionType type = json.getType();
    if (type == JsonFunctionType.VALUE || type == JsonFunctionType.QUERY) {
      if (json.getReturningType() != null || json.getOnEmptyBehavior() != null || json.getOnErrorBehavior() != null
          || !json.getPassingExpressions().isEmpty() || !json.getAdditionalQueryPathArguments().isEmpty()
          || json.getWrapperType() != null || json.getQuotesType() != null) {
        throw new IllegalArgumentException("Unsupported JSON_" + type
            + " clause: RETURNING, ON EMPTY, ON ERROR, PASSING, extra paths, wrappers, and quotes are not supported");
      }
      Object document = evalInternal(json.getInputExpression().getExpression(), record);
      Object path = evalInternal(json.getJsonPathExpression(), record);
      return type == JsonFunctionType.VALUE
          ? JsonFunctions.jsonValue(document, path)
          : JsonFunctions.jsonQuery(document, path);
    }
    if (type == JsonFunctionType.OBJECT || type == JsonFunctionType.POSTGRES_OBJECT
        || type == JsonFunctionType.MYSQL_OBJECT) {
      List<Object> args = new ArrayList<>();
      for (JsonKeyValuePair pair : json.getKeyValuePairs()) {
        Object keyObj = pair.getKey();
        String key = null;
        if (keyObj instanceof Expression keyExpr) {
          key = toStringValue(evalInternal(keyExpr, record));
        } else if (keyObj instanceof String keyText) {
          key = LiteralConverter.decodeStringLiteral(new StringValue(keyText));
        } else if (keyObj != null) {
          key = SqlText.valueOf(keyObj);
        }
        if (key == null) {
          continue;
        }
        Object valueObj = pair.getValue();
        Object value;
        if (valueObj instanceof Expression valueExpr) {
          value = evaluateJsonValue(valueExpr, record);
        } else {
          value = valueObj;
        }
        args.add(key);
        args.add(value);
      }
      return JsonFunctions.jsonObject(args);
    }
    if (type == JsonFunctionType.ARRAY) {
      List<Object> values = new ArrayList<>();
      for (JsonFunctionExpression expr : json.getExpressions()) {
        values.add(evaluateJsonValue(expr.getExpression(), record));
      }
      return JsonFunctions.jsonArray(values);
    }
    throw syntaxFailure("Unsupported JSON function: " + SqlExceptions.describe(json));
  }

  /**
   * Retains source schemas for temporal constructor values and nested containers.
   *
   * @param expression
   *          constructor value expression
   * @param record
   *          current input row
   * @return JSON value with its declared temporal semantics
   */
  private Object evaluateJsonValue(Expression expression, GenericRecord record) {
    return jsonOperand(expression, record).node();
  }

  /**
   * Retains an evaluated value's declaration until JSON promotion is applied.
   *
   * @param value
   *          decoded value or already serialized array
   * @param declaration
   *          source declaration, or null when unknown
   */
  private record JsonOperand(Object value, Schema declaration) {
    /**
     * Serializes the source value without reinterpreting an existing node.
     *
     * @return corresponding JSON node
     */
    private JsonNode node() {
      return value instanceof JsonNode node ? node : JsonFunctions.constructorValue(value, declaration);
    }
  }

  /**
   * Evaluates arguments once, keeping the selected conditional branch's source
   * declaration before any required offset promotion.
   *
   * @param expression
   *          value expression
   * @param record
   *          current input row
   * @return evaluated value and source declaration
   */
  private JsonOperand jsonOperand(Expression expression, GenericRecord record) {
    Expression unwrapped = ExpressionEvaluator.unwrapParenthesis(expression);
    if (unwrapped instanceof CaseExpression choice && numericType(choice) == null) {
      return jsonBranch(unwrapped, (JsonOperand) evaluateCase(choice, record, this::jsonOperand));
    }
    if (unwrapped instanceof Function function && "COALESCE".equalsIgnoreCase(function.getName())
        && function.getParameters() != null) {
      for (Expression argument : function.getParameters()) {
        JsonOperand operand = jsonOperand(argument, record);
        if (operand.value() != null) {
          return jsonBranch(unwrapped, operand);
        }
      }
      return new JsonOperand(null, null);
    }
    if (unwrapped instanceof ArrayConstructor array) {
      Schema declaration = jsonSchema(unwrapped);
      if (declaration != null && temporalElements(declaration)) {
        List<JsonNode> values = new ArrayList<>();
        for (Expression element : array.getExpressions()) {
          values.add((JsonNode) evaluateJsonValue(element, record));
        }
        return new JsonOperand(JsonFunctions.constructorArray(values, declaration.getElementType()), declaration);
      }
    }
    Object value;
    if (unwrapped instanceof Column column && SqlExpressionNormalizer.timeKeyword(column) == null) {
      String name = resolveColumnName(column);
      value = AvroCoercions.resolveColumnValue(name, record, fieldSchemas, caseInsensitiveIndex,
          AvroCoercions::unwrapLocalFields);
    } else {
      value = evalInternal(unwrapped, record);
    }
    Schema declaration = null;
    if (DateTimeFunctions.isTemporal(value) || value instanceof Object[] || value instanceof List<?>) {
      declaration = jsonSchema(unwrapped);
      if (declaration != null && !temporalElements(declaration)) {
        declaration = null;
      }
    }
    return new JsonOperand(value, declaration);
  }

  /**
   * Applies a conditional expression's required promotion without losing UTC
   * provenance, local fields, or the common interval qualifier.
   *
   * @param expression
   *          CASE or COALESCE expression
   * @param selected
   *          selected branch, or null when CASE had no matching branch
   * @return promoted operand
   */
  private JsonOperand jsonBranch(Expression expression, JsonOperand selected) {
    if (selected == null || selected.value() == null) {
      return new JsonOperand(null, null);
    }
    if (selected.value() instanceof TemporalInterval interval) {
      Schema declaration = intervalSchema(expression);
      return new JsonOperand(
          declaration == null ? interval : se.alipsa.jparq.helper.IntervalType.project(interval, declaration), null);
    }
    if (DateTimeFunctions.isTemporal(selected.value())
        || selected.declaration() != null && temporalElements(selected.declaration())) {
      Schema declaration = jsonSchema(expression);
      return new JsonOperand(JsonFunctions.constructorBranch(selected.node(), declaration), declaration);
    }
    return selected;
  }

  /**
   * Resolves a constructor declaration once for this evaluator's input schema.
   *
   * @param expression
   *          argument expression
   * @return known declaration, or null
   */
  private Schema jsonSchema(Expression expression) {
    return jsonTypes.computeIfAbsent(expression, shape -> Optional.ofNullable(ArrayExpressionSchemas.resolve(shape,
        this::jsonColumnSchema, select -> subqueryExecutor == null ? null : subqueryExecutor.projectionSchema(select))))
        .orElse(null);
  }

  /**
   * Identifies arrays whose datetime leaves need their source declarations.
   *
   * @param schema
   *          array or leaf declaration
   * @return whether the innermost array elements are datetime values
   */
  private static boolean temporalElements(Schema schema) {
    Schema element = JdbcTypeMapper.nonNullSchema(schema);
    while (element.getType() == Schema.Type.ARRAY) {
      element = JdbcTypeMapper.nonNullSchema(element.getElementType());
    }
    int type = JdbcTypeMapper.mapSchemaToJdbcType(element);
    return type == java.sql.Types.DATE || type == java.sql.Types.TIME || type == java.sql.Types.TIMESTAMP
        || type == java.sql.Types.TIME_WITH_TIMEZONE || type == java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
  }

  /**
   * Resolves an actual field using the same case-insensitive index as evaluation.
   * Datetime keywords parsed as columns do not name input fields.
   *
   * @param column
   *          possible field reference
   * @return field schema, or null for a keyword or unresolved name
   */
  private Schema jsonColumnSchema(Column column) {
    if (SqlExpressionNormalizer.timeKeyword(column) != null) {
      return null;
    }
    String name = resolveColumnName(column);
    if (name == null) {
      return null;
    }
    Schema declaration = fieldSchemas.get(name);
    String canonical = declaration == null ? caseInsensitiveIndex.get(Identifier.lookupKey(name)) : null;
    return canonical == null ? declaration : fieldSchemas.get(canonical);
  }

  private String toStringValue(Object value) {
    return value == null ? null : SqlText.valueOf(value);
  }

  /**
   * Concatenate two expressions following SQL-92 {@code ||} semantics.
   *
   * @param left
   *          left-hand expression
   * @param right
   *          right-hand expression
   * @param record
   *          current record
   * @return concatenated result, or {@code null} when any operand is {@code null}
   */
  private Object concatenate(Expression left, Expression right, GenericRecord record) {
    Object leftVal = evalInternal(left, record);
    Object rightVal = evalInternal(right, record);
    if (leftVal == null || rightVal == null) {
      return null;
    }
    boolean leftBinary = isBinaryValue(leftVal);
    boolean rightBinary = isBinaryValue(rightVal);
    if (leftBinary || rightBinary) {
      byte[] leftBytes = leftBinary ? toByteArray(leftVal) : toStringValue(leftVal).getBytes(StandardCharsets.UTF_8);
      byte[] rightBytes = rightBinary
          ? toByteArray(rightVal)
          : toStringValue(rightVal).getBytes(StandardCharsets.UTF_8);
      byte[] result = new byte[leftBytes.length + rightBytes.length];
      System.arraycopy(leftBytes, 0, result, 0, leftBytes.length);
      System.arraycopy(rightBytes, 0, result, leftBytes.length, rightBytes.length);
      return result;
    }
    return toStringValue(leftVal) + toStringValue(rightVal);
  }

  private boolean isBinaryValue(Object value) {
    return value instanceof byte[] || value instanceof ByteBuffer;
  }

  private byte[] toByteArray(Object value) {
    if (value instanceof byte[] bytes) {
      return bytes.clone();
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer duplicate = buffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return bytes;
    }
    throw new IllegalArgumentException("Unsupported binary value: " + value);
  }

  private Object arithmetic(Expression left, Expression right, GenericRecord record, Operation op) {
    Object leftVal = evalInternal(left, record);
    Object rightVal = evalInternal(right, record);
    if (leftVal == null || rightVal == null) {
      return null;
    }
    if (op == Operation.ADD) {
      Object temporal = DateTimeFunctions.plus(leftVal, rightVal);
      if (temporal != null) {
        return temporal;
      }
    }
    if (op == Operation.SUB) {
      Object temporal = DateTimeFunctions.minus(leftVal, rightVal);
      if (temporal != null) {
        return temporal;
      }
    }
    if (DateTimeFunctions.isTemporal(leftVal) || DateTimeFunctions.isTemporal(rightVal)
        || leftVal instanceof TemporalInterval || rightVal instanceof TemporalInterval) {
      String operator = switch (op) {
        case ADD -> "+";
        case SUB -> "-";
        case MUL -> "*";
        case DIV -> "/";
        case MOD -> "%";
      };
      String hint = leftVal instanceof CharSequence || rightVal instanceof CharSequence
          ? "; use CAST(... AS DATE) for date text or an explicit INTERVAL for offsets"
          : "; date/time offsets require an explicit INTERVAL; numeric day counts require numeric operands";
      throw syntaxFailure("Unsupported temporal arithmetic " + operator + " between " + sqlOperandType(leftVal)
          + " and " + sqlOperandType(rightVal) + hint);
    }
    NumericType leftType = numericType(left);
    NumericType rightType = numericType(right);
    NumericType resultType = leftType == null || rightType == null ? null : leftType.arithmetic(rightType, op);
    return AggregateFunctions.calculate(leftVal, rightVal, op, resultType);
  }

  /**
   * Resolves and caches operand declarations independently of current row values.
   *
   * @param expression
   *          numeric expression or arithmetic operand
   * @return its numeric declaration, or null when unknown
   */
  private NumericType numericType(Expression expression) {
    if (!numericTypes.containsKey(expression)) {
      NumericType numeric = NumericExpressionTypes.resolve(expression, column -> {
        String canonical = resolveColumnName(column);
        Schema field = canonical == null ? null : fieldSchemas.get(canonical);
        return field == null
            ? null
            : NumericType.of(JdbcTypeMapper.mapSchemaToJdbcType(field), JdbcTypeMapper.nonNullSchema(field));
      });
      numericTypes.put(expression, numeric);
    }
    return numericTypes.get(expression);
  }

  /**
   * Describes an arithmetic operand using SQL type names.
   *
   * @param value
   *          non-null operand
   * @return SQL type name for diagnostics
   */
  private static String sqlOperandType(Object value) {
    if (value instanceof TemporalInterval) {
      return "INTERVAL";
    }
    if (value instanceof java.time.LocalDate) {
      return "DATE";
    }
    if (value instanceof java.time.LocalDateTime || value instanceof java.time.OffsetDateTime) {
      return "TIMESTAMP";
    }
    if (value instanceof java.time.LocalTime || value instanceof java.time.OffsetTime) {
      return "TIME";
    }
    return java.sql.JDBCType.valueOf(JdbcTypeMapper.mapJavaClassNameToJdbcType(value.getClass().getName())).getName();
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

  /**
   * Resolves an outer reference against the current row, retaining NULL values
   * from qualified columns and projection aliases.
   *
   * @param qualifier
   *          outer table qualifier, or null for an unqualified reference
   * @param columnName
   *          outer column name
   * @param record
   *          current outer row
   * @return the bound value, possibly null
   * @throws IllegalStateException
   *           if a correlation mapping names a missing field
   * @throws IllegalArgumentException
   *           if the column reference is unknown or ambiguous
   */
  private Object resolveCorrelatedValue(String qualifier, String columnName, GenericRecord record) {
    String normalizedQualifier = Identifier.lookupKey(qualifier);
    String normalizedColumn = Identifier.lookupKey(columnName);
    if (normalizedQualifier != null && normalizedColumn != null && !correlationContext.isEmpty()) {
      Map<String, String> mapping = correlationContext.get(normalizedQualifier);
      if (mapping != null) {
        String canonical = mapping.get(normalizedColumn);
        if (canonical != null) {
          assertCorrelatedFieldPresent(normalizedQualifier, normalizedColumn, canonical);
          return AvroCoercions.resolveColumnValue(canonical, record, fieldSchemas, caseInsensitiveIndex);
        }
      }
    }
    // NULL is a resolved SQL value, not a signal to retry without the qualifier.
    return resolveColumnValue(qualifier, columnName, record);
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
    String fallback = caseInsensitiveIndex.get(Identifier.lookupKey(canonical));
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
    return ColumnMappingUtil.correlationColumns(caseInsensitiveIndex, unqualifiedColumnMapping,
        effectiveQualifierMapping);
  }

  /**
   * Resolves a column once for callers that can read a stable schema directly.
   *
   * @param column
   *          SQL column reference
   * @return internal field name, or null when the reference is hidden or unknown
   * @throws IllegalArgumentException
   *           if the reference is ambiguous
   */
  public String resolveColumnName(Column column) {
    String qualifier = column.getTable() == null ? null : column.getTable().getName();
    return canonicalFieldName(qualifier, column.getColumnName());
  }

  private String canonicalFieldName(String qualifier, String columnName) {
    String canonical = ColumnMappingUtil.canonicalFieldName(qualifier, columnName, effectiveQualifierMapping,
        unqualifiedColumnMapping, caseInsensitiveIndex);
    return MaterializedNames.sqlReference(schema, columnName, canonical);
  }

  private static Map<String, Map<String, String>> mergeQualifierMappings(Map<String, Map<String, String>> primary,
      Map<String, Map<String, String>> secondary) {
    if (secondary == null || secondary.isEmpty()) {
      return primary == null ? Map.of() : primary;
    }
    Map<String, Map<String, String>> merged = new LinkedHashMap<>();
    if (secondary != null) {
      merged.putAll(secondary);
    }
    if (primary != null) {
      merged.putAll(primary);
    }
    return merged.isEmpty() ? Map.of() : Map.copyOf(merged);
  }
}
