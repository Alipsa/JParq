package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Date;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.DateTimeExpressions;
import se.alipsa.jparq.helper.LiteralConverter;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Tests for
 * {@link LiteralConverter#toLiteral(net.sf.jsqlparser.expression.Expression)}.
 */
class LiteralConverterTest {

  @Test
  void convertsBasicLiteralExpressions() {
    assertNull(LiteralConverter.toLiteral(new net.sf.jsqlparser.expression.NullValue()));
    assertEquals("hello", LiteralConverter.toLiteral(new StringValue("hello")));
    assertEquals(42L, LiteralConverter.toLiteral(new LongValue(42)));
    assertEquals(new BigDecimal("3.5"), LiteralConverter.toLiteral(new DoubleValue("3.5")));
    assertEquals(Boolean.TRUE, LiteralConverter.toLiteral(new BooleanValue("true")));

    TimeKeyExpression timeKeyExpression = new TimeKeyExpression();
    timeKeyExpression.setStringValue("CURRENT_DATE");
    assertInstanceOf(Date.class, LiteralConverter.toLiteral(timeKeyExpression));

    IntervalExpression intervalExpression = new IntervalExpression();
    intervalExpression.setParameter("2");
    intervalExpression.setIntervalType("DAY");
    assertEquals(TemporalInterval.parse("2", "DAY"), LiteralConverter.toLiteral(intervalExpression));
  }

  @Test
  void convertsSignedAndTemporalLiterals() {
    SignedExpression negativeNumber = new SignedExpression();
    negativeNumber.setSign('-');
    negativeNumber.setExpression(new LongValue(7));
    assertEquals(new BigDecimal("-7"), LiteralConverter.toLiteral(negativeNumber));

    SignedExpression decimalString = new SignedExpression();
    decimalString.setSign('+');
    decimalString.setExpression(new StringValue("11.25"));
    assertEquals(new BigDecimal("11.25"), LiteralConverter.toLiteral(decimalString));

    IntervalExpression intervalExpression = new IntervalExpression();
    intervalExpression.setIntervalType("DAY");
    intervalExpression.setParameter("3");
    SignedExpression negativeInterval = new SignedExpression();
    negativeInterval.setSign('-');
    negativeInterval.setExpression(intervalExpression);
    assertEquals(TemporalInterval.parse("3", "DAY").negate(), LiteralConverter.toLiteral(negativeInterval));
  }

  @Test
  void castsLiteralExpressions() throws Exception {
    CastExpression castExpression = (CastExpression) CCJSqlParserUtil.parseExpression("CAST('2024-01-15' AS DATE)");
    Object literal = LiteralConverter.toLiteral(castExpression);
    assertEquals(DateTimeExpressions.castLiteral(castExpression, "2024-01-15"), literal);
  }

  @Test
  void fallsBackToBigDecimalOrStringValue() {
    SignedExpression stringLiteral = new SignedExpression();
    stringLiteral.setSign('-');
    stringLiteral.setExpression(new StringValue("abc"));
    assertEquals("-abc", LiteralConverter.toLiteral(stringLiteral));

    SignedExpression numericFromString = new SignedExpression();
    numericFromString.setSign('+');
    numericFromString.setExpression(new StringValue("5"));
    assertEquals(new BigDecimal("5"), LiteralConverter.toLiteral(numericFromString));

    SignedExpression fallback = new SignedExpression();
    fallback.setSign('+');
    fallback.setExpression(new StringValue("not-a-number"));
    assertEquals("not-a-number", LiteralConverter.toLiteral(fallback));

    AnalyticExpression unknownExpression = new AnalyticExpression();
    assertEquals(unknownExpression.toString(), LiteralConverter.toLiteral(unknownExpression));
  }
}
