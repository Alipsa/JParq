package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JsonFunction;
import net.sf.jsqlparser.expression.JsonFunctionExpression;
import net.sf.jsqlparser.expression.JsonFunctionType;
import net.sf.jsqlparser.expression.JsonKeyValuePair;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.helper.DateTimeExpressions;
import se.alipsa.jparq.helper.JsonExpressions;
import se.alipsa.jparq.helper.LiteralConverter;
import se.alipsa.jparq.helper.StringExpressions;

/**
 * Evaluates SELECT-list expressions (e.g. computed columns and supported SQL
 * functions such as {@code COALESCE} and {@code CAST}) against a
 * {@link GenericRecord}.
 */
public final class ValueExpressionEvaluator {

  private final Map<String, Schema> fieldSchemas;
  private final Map<String, String> caseInsensitiveIndex;

  /**
   * Create an evaluator bound to the supplied Avro {@link Schema}.
   *
   * @param schema
   *          the Avro schema describing the available columns
   */
  public ValueExpressionEvaluator(Schema schema) {
    Map<String, Schema> fs = new HashMap<>();
    Map<String, String> ci = new HashMap<>();
    for (Schema.Field f : schema.getFields()) {
      fs.put(f.name(), f.schema());
      ci.put(f.name().toLowerCase(Locale.ROOT), f.name());
    }
    this.fieldSchemas = Map.copyOf(fs);
    this.caseInsensitiveIndex = Map.copyOf(ci);
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
    return LiteralConverter.toLiteral(expression);
  }

  /**
   * Evaluate a SQL function call. Supports COALESCE, character functions,
   * trimming/padding helpers, pattern matching utilities, JSON helpers and
   * Unicode conversions.
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
    return compiled.matcher(input).matches();
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
    String keyword = keyWord == null ? like.getStringExpression() : keyWord.name();
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
    if ("SIMILAR_TO".equalsIgnoreCase(keyword)) {
      boolean matches = StringExpressions.similarTo(input, pattern, escapeChar);
      return like.isNot() ? !matches : matches;
    }
    boolean caseInsensitive = keyWord == LikeExpression.KeyWord.ILIKE
        || (keyWord == null && "ILIKE".equalsIgnoreCase(keyword));
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
    String name = column.getColumnName();
    String lookup = name;
    Schema colSchema = fieldSchemas.get(name);
    if (colSchema == null) {
      String canonical = caseInsensitiveIndex.get(name.toLowerCase(Locale.ROOT));
      if (canonical != null) {
        colSchema = fieldSchemas.get(canonical);
        lookup = canonical;
      }
    }
    if (colSchema == null) {
      return null;
    }
    return AvroCoercions.unwrap(record.get(lookup), colSchema);
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
