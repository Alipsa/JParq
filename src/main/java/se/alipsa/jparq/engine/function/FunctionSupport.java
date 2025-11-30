package se.alipsa.jparq.engine.function;

import static se.alipsa.jparq.engine.function.NumericFunctions.toInteger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.CorrelatedSubqueryRewriter;
import se.alipsa.jparq.engine.SubqueryCorrelatedFiltersIsolator;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.helper.LiteralConverter;

/**
 * Central dispatch for evaluating SQL {@link Function} expressions.
 */
public final class FunctionSupport {

  private final SubqueryExecutor subqueryExecutor;
  private final List<String> correlationQualifiers;
  private final Set<String> correlationColumns;
  private final Map<String, Map<String, String>> correlationContext;
  private final CorrelatedValueResolver correlatedValueResolver;
  private final BiFunction<Expression, GenericRecord, Object> evaluator;

  /**
   * Create a new function support instance.
   *
   * @param subqueryExecutor
   *          executor used for ARRAY subqueries (may be {@code null})
   * @param correlationQualifiers
   *          qualifiers available to correlated subqueries
   * @param correlationColumns
   *          columns available to correlated subqueries
   * @param correlationContext
   *          qualifier-aware correlation context
   * @param correlatedValueResolver
   *          resolver for correlated column lookups
   * @param evaluator
   *          evaluator used to resolve argument expressions
   */
  public FunctionSupport(SubqueryExecutor subqueryExecutor, List<String> correlationQualifiers,
      Set<String> correlationColumns, Map<String, Map<String, String>> correlationContext,
      CorrelatedValueResolver correlatedValueResolver, BiFunction<Expression, GenericRecord, Object> evaluator) {
    this.subqueryExecutor = subqueryExecutor;
    this.correlationQualifiers = correlationQualifiers == null ? List.of() : List.copyOf(correlationQualifiers);
    this.correlationColumns = correlationColumns == null ? Set.of() : Set.copyOf(correlationColumns);
    this.correlationContext = correlationContext == null ? Map.of() : Map.copyOf(correlationContext);
    this.correlatedValueResolver = correlatedValueResolver;
    this.evaluator = evaluator;
  }

  /**
   * Evaluate a {@link Function} expression.
   *
   * @param func
   *          the function to evaluate
   * @param record
   *          the current record
   * @return the computed value or {@code null}
   */
  public Object evaluate(Function func, GenericRecord record) {
    String name = func.getName();
    if (name == null) {
      return null;
    }
    String upper = name.toUpperCase(Locale.ROOT);
    FunctionArguments args = new FunctionArguments(func, record, evaluator);
    return switch (upper) {
      case "COALESCE" -> StringFunctions.coalesce(args);
      case "IFNULL" -> SystemFunctions.ifNull(args.firstValue(),
          args.positionalValues().size() > 1 ? args.positionalValues().get(1) : null);
      case "DATABASE" -> SystemFunctions.database();
      case "USER" -> SystemFunctions.user();
      case "SYSTEM_USER" -> SystemFunctions.user();
      case "ASCII" -> StringFunctions.ascii(args.firstValue());
      case "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH" -> StringFunctions.charLength(args.firstValue());
      case "OCTET_LENGTH" -> StringFunctions.octetLength(args.firstValue());
      case "POSITION" -> evaluatePosition(args);
      case "LOCATE" -> evaluateLocate(args);
      case "SUBSTRING" -> evaluateSubstring(args);
      case "LEFT" -> evaluateLeftOrRight(args, true);
      case "RIGHT" -> evaluateLeftOrRight(args, false);
      case "CONCAT" -> StringFunctions.concat(args.positionalValues());
      case "UPPER", "UCASE" -> StringFunctions.upper(args.firstValue());
      case "LOWER", "LCASE" -> StringFunctions.lower(args.firstValue());
      case "LTRIM" -> evaluateTrimFunction(args, StringFunctions.TrimMode.LEADING);
      case "RTRIM" -> evaluateTrimFunction(args, StringFunctions.TrimMode.TRAILING);
      case "LPAD" -> evaluatePad(args, true);
      case "RPAD" -> evaluatePad(args, false);
      case "OVERLAY" -> evaluateOverlay(args);
      case "REPLACE" -> evaluateReplace(args);
      case "INSERT" -> evaluateInsert(args);
      case "REPEAT" -> evaluateRepeat(args);
      case "SPACE" -> StringFunctions.space(toInteger(args.firstValue()));
      case "CHAR" -> evaluateChar(args);
      case "UNICODE" -> StringFunctions.unicode(args.firstValue());
      case "NORMALIZE" -> evaluateNormalize(args);
      case "REGEXP_LIKE" -> evaluateRegexpLike(args);
      case "SOUNDEX" -> StringFunctions.soundex(args.firstValue());
      case "DIFFERENCE" -> evaluateDifference(args);
      case "JSON_VALUE" -> JsonFunctions.jsonValue(args.positionalValues());
      case "JSON_QUERY" -> JsonFunctions.jsonQuery(args.positionalValues());
      case "JSON_OBJECT" -> JsonFunctions.jsonObject(args.positionalValues());
      case "JSON_ARRAY" -> JsonFunctions.jsonArray(args.positionalValues());
      case "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP" ->
        evaluateTimeKeyword(upper);
      case "DAYOFWEEK" -> DateTimeFunctions.dayOfWeek(args.firstValue());
      case "DAYOFMONTH" -> DateTimeFunctions.dayOfMonth(args.firstValue());
      case "DAYOFYEAR" -> DateTimeFunctions.dayOfYear(args.firstValue());
      case "HOUR" -> DateTimeFunctions.hour(args.firstValue());
      case "MINUTE" -> DateTimeFunctions.minute(args.firstValue());
      case "SECOND" -> DateTimeFunctions.second(args.firstValue());
      case "MONTH" -> DateTimeFunctions.month(args.firstValue());
      case "YEAR" -> DateTimeFunctions.year(args.firstValue());
      case "QUARTER" -> DateTimeFunctions.quarter(args.firstValue());
      case "WEEK" -> DateTimeFunctions.week(args.firstValue());
      case "TIMESTAMPADD" -> DateTimeFunctions.timestampAdd(args.positionalValues());
      case "TIMESTAMPDIFF" -> DateTimeFunctions.timestampDiff(args.positionalValues());
      case "ARRAY" -> evaluateArrayFunction(func, record, args);
      case "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "SQRT", "TRUNC", "TRUNCATE", "MOD", "POWER", "POW", "EXP", "LOG",
          "LOG10", "RAND", "RANDOM", "SIGN", "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2", "DEGREES",
          "RADIANS" ->
        NumericFunctions.evaluate(upper, args.positionalValues());
      default -> LiteralConverter.toLiteral(func);
    };
  }

  /**
   * Evaluate a {@link TrimFunction}.
   *
   * @param trim
   *          trim expression
   * @param record
   *          current record
   * @return trimmed value or {@code null}
   */
  public Object evaluate(TrimFunction trim, GenericRecord record) {
    TrimFunction.TrimSpecification specValue = trim.getTrimSpecification();
    StringFunctions.TrimMode mode;
    if (specValue == TrimFunction.TrimSpecification.LEADING) {
      mode = StringFunctions.TrimMode.LEADING;
    } else if (specValue == TrimFunction.TrimSpecification.TRAILING) {
      mode = StringFunctions.TrimMode.TRAILING;
    } else {
      mode = StringFunctions.TrimMode.BOTH;
    }
    String characters = null;
    String target;
    if (trim.isUsingFromKeyword()) {
      characters = toStringValue(trim.getExpression() == null ? null : evaluator.apply(trim.getExpression(), record));
      target = toStringValue(
          trim.getFromExpression() == null ? null : evaluator.apply(trim.getFromExpression(), record));
    } else {
      target = toStringValue(trim.getExpression() == null ? null : evaluator.apply(trim.getExpression(), record));
    }
    return StringFunctions.trim(target, characters, mode);
  }

  /**
   * Evaluate an {@link ArrayConstructor} expression.
   *
   * @param array
   *          ARRAY constructor
   * @param record
   *          current record
   * @return homogenized array values
   */
  public Object evaluate(ArrayConstructor array, GenericRecord record) {
    ExpressionList<? extends Expression> expressionItems = array.getExpressions();
    if (expressionItems == null || expressionItems.isEmpty()) {
      return List.of();
    }
    List<Object> values = new ArrayList<>(expressionItems.size());
    for (Expression element : expressionItems) {
      values.add(evaluator.apply(element, record));
    }
    return ArrayFunctions.homogenizeArrayValues(values);
  }

  private Object evaluateArrayFunction(Function func, GenericRecord record, FunctionArguments args) {
    List<Expression> parameterExpressions = args.positionalExpressions();
    if (parameterExpressions.isEmpty()) {
      return List.of();
    }
    Expression first = parameterExpressions.getFirst();
    if (first instanceof Select select) {
      List<Object> values = executeArraySubquery(select, record);
      return ArrayFunctions.homogenizeArrayValues(values);
    }
    List<Object> values = new ArrayList<>(parameterExpressions.size());
    for (Expression element : parameterExpressions) {
      values.add(args.evaluate(element));
    }
    return ArrayFunctions.homogenizeArrayValues(values);
  }

  private List<Object> executeArraySubquery(Select subSelect, GenericRecord record) {
    if (subqueryExecutor == null) {
      throw new IllegalStateException("ARRAY subqueries require a subquery executor");
    }
    CorrelatedSubqueryRewriter.Result rewritten = SubqueryCorrelatedFiltersIsolator.isolate(subSelect,
        correlationQualifiers, correlationColumns, correlationContext,
        (qualifier, column) -> correlatedValueResolver.resolve(qualifier, column, record));
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

  private Object evaluatePosition(FunctionArguments args) {
    FunctionArguments.NamedArgs named = args.named();
    Object substring;
    Object source;
    if (named != null) {
      substring = named.values().isEmpty() ? null : named.values().getFirst();
      source = named.values().size() > 1 ? named.values().get(1) : null;
    } else {
      List<Object> positional = args.positionalValues();
      if (positional.size() < 2) {
        return null;
      }
      substring = positional.get(0);
      source = positional.get(1);
    }
    return StringFunctions.position(substring, source);
  }

  private Object evaluateLocate(FunctionArguments args) {
    List<Object> positional = args.positionalValues();
    if (positional.size() < 2) {
      return null;
    }
    Object substring = positional.get(0);
    Object source = positional.get(1);
    Number start = positional.size() > 2 ? toInteger(positional.get(2)) : null;
    return StringFunctions.locate(substring, source, start);
  }

  private Object evaluateSubstring(FunctionArguments args) {
    FunctionArguments.NamedArgs named = args.named();
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
      List<Object> positional = args.positionalValues();
      if (positional.isEmpty()) {
        return null;
      }
      input = toStringValue(positional.get(0));
      if (positional.size() > 1) {
        start = toInteger(positional.get(1));
      }
      if (positional.size() > 2) {
        length = toInteger(positional.get(2));
      }
    }
    if (input == null || start == null) {
      return null;
    }
    return StringFunctions.substring(input, start, length);
  }

  private Object evaluateLeftOrRight(FunctionArguments args, boolean left) {
    List<Object> positional = args.positionalValues();
    if (positional.size() < 2) {
      return null;
    }
    String input = toStringValue(positional.get(0));
    Integer count = toInteger(positional.get(1));
    if (input == null || count == null) {
      return null;
    }
    return left ? StringFunctions.left(input, count) : StringFunctions.right(input, count);
  }

  private Object evaluateTrimFunction(FunctionArguments args, StringFunctions.TrimMode mode) {
    List<Object> positional = args.positionalValues();
    if (positional.isEmpty()) {
      return null;
    }
    String input = toStringValue(positional.get(0));
    String chars = positional.size() > 1 ? toStringValue(positional.get(1)) : null;
    return StringFunctions.trim(input, chars, mode);
  }

  private Object evaluatePad(FunctionArguments args, boolean left) {
    List<Object> positional = args.positionalValues();
    if (positional.size() < 2) {
      return null;
    }
    String input = toStringValue(positional.get(0));
    Integer length = toInteger(positional.get(1));
    String fill = positional.size() > 2 ? toStringValue(positional.get(2)) : null;
    if (input == null || length == null) {
      return null;
    }
    return left ? StringFunctions.lpad(input, length, fill) : StringFunctions.rpad(input, length, fill);
  }

  private Object evaluateOverlay(FunctionArguments args) {
    FunctionArguments.NamedArgs named = args.named();
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
      List<Object> positional = args.positionalValues();
      if (positional.size() < 3) {
        return null;
      }
      input = toStringValue(positional.get(0));
      replacement = toStringValue(positional.get(1));
      start = toInteger(positional.get(2));
      if (positional.size() > 3) {
        length = toInteger(positional.get(3));
      }
    }
    if (input == null || replacement == null || start == null) {
      return null;
    }
    return StringFunctions.overlay(input, replacement, start, length);
  }

  private Object evaluateReplace(FunctionArguments args) {
    List<Object> positional = args.positionalValues();
    if (positional.size() < 3) {
      return null;
    }
    String input = toStringValue(positional.get(0));
    String search = toStringValue(positional.get(1));
    String replacement = toStringValue(positional.get(2));
    if (input == null || search == null || replacement == null) {
      return null;
    }
    return StringFunctions.replace(input, search, replacement);
  }

  private Object evaluateInsert(FunctionArguments args) {
    List<Object> positional = args.positionalValues();
    if (positional.size() < 4) {
      return null;
    }
    String input = toStringValue(positional.get(0));
    Integer start = toInteger(positional.get(1));
    Integer length = toInteger(positional.get(2));
    String replacement = toStringValue(positional.get(3));
    return StringFunctions.insert(input, start, length, replacement);
  }

  private Object evaluateRepeat(FunctionArguments args) {
    List<Object> positional = args.positionalValues();
    if (positional.size() < 2) {
      return null;
    }
    String input = toStringValue(positional.get(0));
    Integer count = toInteger(positional.get(1));
    return StringFunctions.repeat(input, count);
  }

  private Object evaluateChar(FunctionArguments args) {
    List<Object> positional = args.positionalValues();
    if (positional.isEmpty()) {
      return null;
    }
    List<Object> codes = new ArrayList<>(positional.size());
    for (Object arg : positional) {
      Integer value = toInteger(arg);
      if (value != null) {
        codes.add(value);
      }
    }
    return StringFunctions.charFromCodes(codes);
  }

  private Object evaluateNormalize(FunctionArguments args) {
    List<Object> positional = args.positionalValues();
    if (positional.isEmpty()) {
      return null;
    }
    Object value = positional.get(0);
    Object form = positional.size() > 1 ? positional.get(1) : null;
    return StringFunctions.normalize(value, form);
  }

  private Object evaluateRegexpLike(FunctionArguments args) {
    List<Object> positional = args.positionalValues();
    if (positional.size() < 2) {
      return null;
    }
    String input = toStringValue(positional.get(0));
    String pattern = toStringValue(positional.get(1));
    String options = positional.size() > 2 && positional.get(2) != null ? positional.get(2).toString() : null;
    return StringFunctions.regexpLike(input, pattern, options);
  }

  private Object evaluateDifference(FunctionArguments args) {
    List<Object> positional = args.positionalValues();
    if (positional.size() < 2) {
      return null;
    }
    return StringFunctions.difference(positional.get(0), positional.get(1));
  }

  private String toStringValue(Object value) {
    return value == null ? null : value.toString();
  }

  private Object evaluateTimeKeyword(String keyword) {
    TimeKeyExpression tk = new TimeKeyExpression();
    tk.setStringValue(keyword);
    return DateTimeFunctions.evaluateTimeKey(tk);
  }

  /** Resolve a correlated column value. */
  @FunctionalInterface
  public interface CorrelatedValueResolver {

    /**
     * Resolve a correlated column value for the supplied qualifier and column name.
     *
     * @param qualifier
     *          table qualifier or alias
     * @param column
     *          column name to resolve
     * @param record
     *          current record providing the values
     * @return resolved value (may be {@code null})
     */
    Object resolve(String qualifier, String column, GenericRecord record);
  }
}
