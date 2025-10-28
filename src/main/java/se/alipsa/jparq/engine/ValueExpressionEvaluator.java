package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.helper.DateTimeExpressions;
import se.alipsa.jparq.helper.LiteralConverter;

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
    if (expression instanceof Function func) {
      return evaluateFunction(func, record);
    }
    return LiteralConverter.toLiteral(expression);
  }

  /**
   * Evaluate a SQL function call. Currently supports COALESCE.
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
    if ("COALESCE".equalsIgnoreCase(name)) {
      return evaluateCoalesce(func, record);
    }
    return LiteralConverter.toLiteral(func);
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
