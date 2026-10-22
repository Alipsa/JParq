package se.alipsa.jparq.helper;

import static se.alipsa.jparq.engine.SqlExceptions.syntaxFailure;
import static se.alipsa.jparq.engine.function.NumericFunctions.toBigDecimal;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import se.alipsa.jparq.engine.CorrelatedBindings;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/** Converts JSqlParser Expression literals to Java Objects. */
public class LiteralConverter {

  /**
   * Constructor for LiteralConverter.
   */
  public LiteralConverter() {
    // empty
  }

  /**
   * Identifies literal nodes shared by scalar evaluation and predicate pushdown.
   * Casts and unary signs require separate operand validation.
   *
   * @param expression
   *          the parsed expression
   * @return whether the node represents a literal, including legacy hex literals
   */
  public static boolean isLiteral(Expression expression) {
    return expression instanceof StringValue || expression instanceof LongValue || expression instanceof DoubleValue
        || expression instanceof NullValue || expression instanceof BooleanValue || expression instanceof HexValue
        || expression instanceof DateTimeLiteralExpression || expression instanceof DateValue
        || expression instanceof TimeValue || expression instanceof TimestampValue;
  }

  /**
   * Applies a unary sign using the numeric conversion rules of arithmetic.
   *
   * @param value
   *          evaluated operand, including numeric text, an interval, or NULL
   * @param expression
   *          the signed expression, used for diagnostics
   * @return the signed value or NULL
   * @throws IllegalArgumentException
   *           if conversion fails (SQLState 22018) or the operand type is invalid
   *           (SQLState 42000)
   */
  public static Object applySign(Object value, SignedExpression expression) {
    char sign = expression.getSign();
    if (value == null) {
      return null;
    }
    if (value instanceof TemporalInterval interval) {
      return sign == '-' ? interval.negate() : interval;
    }
    if (value instanceof Number || value instanceof String) {
      try {
        BigDecimal number = toBigDecimal(value);
        return sign == '-' ? number.negate() : number;
      } catch (NumberFormatException failure) {
        String message = "Cannot convert unary sign operand to a numeric value: " + expression;
        throw new IllegalArgumentException(message, new SQLDataException(message, "22018", failure));
      }
    }
    throw syntaxFailure("Unary sign requires a numeric or interval operand: " + expression);
  }

  /**
   * Converts a JSqlParser Expression literal to a Java Object. String literals
   * decode SQL doubled apostrophes once, while dollar-quoted strings stay
   * literal.
   *
   * @param e
   *          the expression
   * @return the corresponding Java Object
   * @throws IllegalArgumentException
   *           if a signed literal has an invalid operand, with SQLState 22018 for
   *           numeric conversion failures or 42000 for invalid types
   */
  @SuppressWarnings({
      "PMD.AvoidDecimalLiteralsInBigDecimalConstructor", "PMD.EmptyCatchBlock", "checkstyle:NeedBraces"
  })
  public static Object toLiteral(Expression e) {
    if (e instanceof Function function && CorrelatedBindings.isReserved(function)) {
      return CorrelatedBindings.value(function);
    }
    if (e instanceof NullValue) {
      return null;
    }
    if (e instanceof StringValue sv) {
      return decodeStringLiteral(sv);
    }
    if (e instanceof LongValue lv) {
      var value = lv.getBigIntegerValue();
      if (value.bitLength() < 64) {
        return value.longValue();
      }
      return new BigDecimal(value);
    }
    if (e instanceof DoubleValue dv) {
      return new BigDecimal(dv.toString());
    }
    if (e instanceof SignedExpression se) {
      return toSignedLiteral(se);
    }
    if (e instanceof CastExpression cast) {
      Object value = toLiteral(cast.getLeftExpression());
      return DateTimeFunctions.castLiteral(cast, value);
    }
    if (e instanceof BooleanValue bv) {
      return bv.getValue();
    }
    if (e instanceof TimeKeyExpression tk) {
      return DateTimeFunctions.evaluateTimeKey(tk);
    }
    if (e instanceof DateTimeLiteralExpression temporal) {
      String value = new StringValue(temporal.getValue()).getValue();
      return switch (temporal.getType()) {
        case DATE -> DateTimeFunctions.toDate(value);
        case TIME -> DateTimeFunctions.toTime(value);
        case TIMESTAMP, DATETIME -> DateTimeFunctions.toTimestamp(value);
        default -> throw new IllegalArgumentException("Unsupported temporal literal type: " + temporal.getType());
      };
    }
    if (e instanceof TimeValue time) {
      return time.getValue();
    }
    if (e instanceof DateValue dv) {
      return dv.getValue(); // java.sql.Date
    }
    if (e instanceof TimestampValue tv) {
      return tv.getValue(); // java.sql.Timestamp
    }
    if (e instanceof IntervalExpression interval) {
      return DateTimeFunctions.toInterval(interval);
    }
    try {
      return new BigDecimal(e.toString());
    } catch (Exception ignore) {
    }
    return e.toString();
  }

  /**
   * Decodes SQL apostrophe escapes without copying literals that have no doubled
   * apostrophes. Dollar-quoted contents remain unchanged.
   *
   * @param literal
   *          the parsed string literal
   * @return the decoded string value
   */
  public static String decodeStringLiteral(StringValue literal) {
    String raw = literal.getValue();
    return raw.indexOf("''") < 0 ? raw : literal.getNotExcapedValue();
  }

  /**
   * Converts and signs a literal operand.
   *
   * @param expression
   *          the signed expression
   * @return the signed value
   */
  private static Object toSignedLiteral(SignedExpression expression) {
    return applySign(toLiteral(expression.getExpression()), expression);
  }
}
