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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.function.AggregateFunctions;
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.helper.LiteralConverter;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Tests for
 * {@link LiteralConverter#toLiteral(net.sf.jsqlparser.expression.Expression)}.
 */
class LiteralConverterTest {

  /**
   * Decodes escaped apostrophes without changing the AST or dollar-quoted text.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void decodesStringEscapesWithoutMutatingLiteral() throws Exception {
    var literal = CCJSqlParserUtil.parseExpression("'a''''b'");
    assertEquals("a''b", LiteralConverter.toLiteral(literal));
    assertEquals("a''b", LiteralConverter.toLiteral(literal));
    assertEquals("'a''''b'", literal.toString());
    assertEquals("a''''b", LiteralConverter.toLiteral(new StringValue("$tag$a''''b$tag$")));
    assertTrue(AggregateFunctions.expressionsEquivalent(literal, new StringValue("$$a''b$$")));
    assertFalse(AggregateFunctions.expressionsEquivalent(literal, new StringValue("$$a''''b$$")));
    var unescaped = new StringValue("plain text");
    assertSame(unescaped.getValue(), LiteralConverter.toLiteral(unescaped));
    var dollarQuoted = new StringValue("$$a''b$$");
    assertSame(dollarQuoted.getValue(), LiteralConverter.toLiteral(dollarQuoted));
  }

  /**
   * Converts the typed temporal nodes emitted by JSqlParser 5.4.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void convertsTypedTemporalLiterals() throws Exception {
    assertEquals(Date.valueOf("2024-06-30"),
        LiteralConverter.toLiteral(CCJSqlParserUtil.parseExpression("DATE '2024-06-30'")));
    assertEquals(java.sql.Time.valueOf("22:34:45"),
        LiteralConverter.toLiteral(CCJSqlParserUtil.parseExpression("TIME '22:34:45'")));
    assertEquals(java.sql.Timestamp.valueOf("2024-06-30 22:34:45.123456789"),
        LiteralConverter.toLiteral(CCJSqlParserUtil.parseExpression("TIMESTAMP '2024-06-30 22:34:45.123456789'")));
  }

  /**
   * Retains the lenient temporal forms accepted before the parser upgrade.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void convertsLenientTemporalLiterals() throws Exception {
    assertEquals(java.sql.Timestamp.valueOf("2024-06-30 00:00:00"),
        LiteralConverter.toLiteral(CCJSqlParserUtil.parseExpression("TIMESTAMP '2024-06-30'")));
    assertEquals(java.sql.Timestamp.valueOf("2024-06-30 10:00:00"),
        LiteralConverter.toLiteral(CCJSqlParserUtil.parseExpression("TIMESTAMP '2024-06-30T10:00:00'")));
    assertEquals(java.sql.Time.valueOf("10:00:00"),
        LiteralConverter.toLiteral(CCJSqlParserUtil.parseExpression("TIME '10:00:00.5'")));
    var zoned = CCJSqlParserUtil.parseExpression("TIMESTAMPTZ '2024-06-30 10:00:00+01'");
    var failure = assertThrows(IllegalArgumentException.class, () -> LiteralConverter.toLiteral(zoned));
    assertTrue(failure.getMessage().contains("Unsupported temporal literal type"));
  }

  /**
   * Prevents invalid temporal literals from rolling into valid values.
   *
   * @param sql
   *          a literal with invalid calendar or clock fields
   * @throws Exception
   *           if parsing fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "DATE '2024-02-30'", "TIME '25:00:00'", "CAST('2024-02-30' AS DATE)", "CAST('25:00:00' AS TIME)",
      "TIMESTAMP '2024-02-30 00:00:00'", "TIMESTAMP '2024-06-30 25:00:00'", "CAST('2024-02-30 00:00:00' AS TIMESTAMP)",
      "CAST('2024-06-30 25:00:00' AS TIMESTAMP)"
  })
  void invalidTemporalLiteralsAreRejected(String sql) throws Exception {
    var expression = CCJSqlParserUtil.parseExpression(sql);
    assertThrows(IllegalArgumentException.class, () -> LiteralConverter.toLiteral(expression));
  }

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
    assertEquals(DateTimeFunctions.castLiteral(castExpression, "2024-01-15"), literal);
  }

  @Test
  void fallsBackToBigDecimalOrStringValue() {
    SignedExpression stringLiteral = new SignedExpression();
    stringLiteral.setSign('-');
    stringLiteral.setExpression(new StringValue("abc"));
    assertThrows(IllegalArgumentException.class, () -> LiteralConverter.toLiteral(stringLiteral));

    SignedExpression numericFromString = new SignedExpression();
    numericFromString.setSign('+');
    numericFromString.setExpression(new StringValue("5"));
    assertEquals(new BigDecimal("5"), LiteralConverter.toLiteral(numericFromString));

    SignedExpression fallback = new SignedExpression();
    fallback.setSign('+');
    fallback.setExpression(new StringValue("not-a-number"));
    assertThrows(IllegalArgumentException.class, () -> LiteralConverter.toLiteral(fallback));

    AnalyticExpression unknownExpression = new AnalyticExpression();
    assertEquals(unknownExpression.toString(), LiteralConverter.toLiteral(unknownExpression));
  }
}
