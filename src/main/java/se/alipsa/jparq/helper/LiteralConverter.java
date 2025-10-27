package se.alipsa.jparq.helper;

import java.math.BigDecimal;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimestampValue;

/** Converts JSqlParser Expression literals to Java Objects. */
public class LiteralConverter {

  /**
   * Constructor for LiteralConverter.
   */
  public LiteralConverter() {
    // empty
  }

  /**
   * Converts a JSqlParser Expression literal to a Java Object.
   *
   * @param e
   *          the expression
   * @return the corresponding Java Object
   */
  @SuppressWarnings({
      "PMD.AvoidDecimalLiteralsInBigDecimalConstructor", "PMD.EmptyCatchBlock", "checkstyle:NeedBraces"
  })
  public static Object toLiteral(Expression e) {
    if (e instanceof NullValue) {
      return null;
    }
    if (e instanceof StringValue sv) {
      return sv.getValue();
    }
    if (e instanceof LongValue lv) {
      return lv.getBigIntegerValue().longValue();
    }
    if (e instanceof DoubleValue dv) {
      return new BigDecimal(dv.getValue());
    }
    if (e instanceof SignedExpression se) {
      return toSignedLiteral(se);
    }
    if (e instanceof CastExpression cast) {
      Object value = toLiteral(cast.getLeftExpression());
      return DateTimeExpressions.castLiteral(cast, value);
    }
    if (e instanceof BooleanValue bv) {
      return bv.getValue();
    }
    if (e instanceof TimeKeyExpression tk) {
      return DateTimeExpressions.evaluateTimeKey(tk);
    }
    if (e instanceof DateValue dv) {
      return dv.getValue(); // java.sql.Date
    }
    if (e instanceof TimestampValue tv) {
      return tv.getValue(); // java.sql.Timestamp
    }
    if (e instanceof IntervalExpression interval) {
      return DateTimeExpressions.toInterval(interval);
    }
    try {
      return new BigDecimal(e.toString());
    } catch (Exception ignore) {
    }
    return e.toString();
  }

  @SuppressWarnings("PMD.EmptyCatchBlock")
  private static Object toSignedLiteral(SignedExpression se) {
    Object inner = toLiteral(se.getExpression());
    if (inner instanceof Number n) {
      var bd = new BigDecimal(n.toString());
      return se.getSign() == '-' ? bd.negate() : bd;
    }
    if (inner instanceof String s) {
      try {
        var bd = new BigDecimal(s);
        return se.getSign() == '-' ? bd.negate() : bd;
      } catch (Exception ignore) {
      }
    }
    if (inner instanceof TemporalInterval interval) {
      return se.getSign() == '-' ? interval.negate() : interval;
    }
    return (se.getSign() == '-') ? ("-" + inner) : inner;
  }
}
