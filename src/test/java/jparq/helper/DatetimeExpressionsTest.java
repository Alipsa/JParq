package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.DateTimeExpressions;
import se.alipsa.jparq.helper.TemporalInterval;

public class DatetimeExpressionsTest {

  @BeforeEach
  public void setUp() {
    // Set a fixed zone for consistent test results
    System.setProperty("user.timezone", "UTC");
  }

  @Test
  void testEvaluateTimeKeyCurrentDate() {
    TimeKeyExpression expr = new TimeKeyExpression();
    expr.setStringValue("CURRENT_DATE");
    Object result = DateTimeExpressions.evaluateTimeKey(expr);
    assertInstanceOf(Date.class, result);
    assertEquals(Date.valueOf(LocalDate.now(ZoneId.of("UTC"))), result);
  }

  @Test
  void testEvaluateTimeKeyCurrentTime() {
    TimeKeyExpression expr = new TimeKeyExpression();
    expr.setStringValue("CURRENT_TIME");
    Object result = DateTimeExpressions.evaluateTimeKey(expr);
    assertInstanceOf(Time.class, result);
  }

  @Test
  void testEvaluateTimeKeyCurrentTimestamp() {
    TimeKeyExpression expr = new TimeKeyExpression();
    expr.setStringValue("CURRENT_TIMESTAMP");
    Object result = DateTimeExpressions.evaluateTimeKey(expr);
    assertInstanceOf(Timestamp.class, result);
    // Allow for some leeway as system clock can tick between getting current
    // Instant and creating Timestamp.
    // Check that the difference is within a reasonable range (e.g., 2 seconds).
    long currentEpochSecond = Instant.now().getEpochSecond();
    long resultEpochSecond = ((Timestamp) result).toInstant().getEpochSecond();
    assertTrue(Math.abs(currentEpochSecond - resultEpochSecond) <= 2);
  }

  @Test
  void testEvaluateTimeKeyUnsupported() {
    TimeKeyExpression expr = new TimeKeyExpression();
    expr.setStringValue("UNKNOWN_KEY");
    Object result = DateTimeExpressions.evaluateTimeKey(expr);
    assertEquals("UNKNOWN_KEY", result);
  }

  @Test
  void testEvaluateTimeKeyNullExpr() {
    assertNull(DateTimeExpressions.evaluateTimeKey(null));
  }

  @Test
  void testToIntervalYearMonth() {
    IntervalExpression expr = new IntervalExpression();
    expr.setParameter("10");
    expr.setIntervalType("YEAR");
    TemporalInterval interval = DateTimeExpressions.toInterval(expr);
    assertEquals(Period.ofYears(10), interval.period());
    assertEquals(Duration.ZERO, interval.duration());
  }

  @Test
  void testToIntervalDayTime() {
    IntervalExpression expr = new IntervalExpression();
    expr.setParameter("1 02:03:04");
    expr.setIntervalType("DAY TO SECOND");
    TemporalInterval interval = DateTimeExpressions.toInterval(expr);
    assertEquals(Period.ZERO, interval.period());
    assertEquals(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4), interval.duration());
  }

  @Test
  void testToIntervalNullExpr() {
    TemporalInterval interval = DateTimeExpressions.toInterval(null);
    assertEquals(Period.ZERO, interval.period());
    assertEquals(Duration.ZERO, interval.duration());
  }

  @Test
  void testExtractTimestampYear() {
    Timestamp ts = Timestamp.valueOf("2023-01-15 10:30:45");
    assertEquals(2023, DateTimeExpressions.extract("YEAR", ts));
  }

  @Test
  void testExtractDateMonth() {
    Date d = Date.valueOf("2023-03-20");
    assertEquals(3, DateTimeExpressions.extract("MONTH", d));
  }

  @Test
  void testExtractTimeHour() {
    Time t = Time.valueOf("14:20:00");
    assertEquals(14, DateTimeExpressions.extract("HOUR", t));
  }

  @Test
  void testExtractLocalDateTimeDay() {
    LocalDateTime ldt = LocalDateTime.of(2022, 12, 25, 0, 0);
    assertEquals(25, DateTimeExpressions.extract("DAY", ldt));
  }

  @Test
  void testExtractLocalDateMinute() {
    LocalDate ld = LocalDate.of(2021, 11, 1);
    assertEquals(0, DateTimeExpressions.extract("MINUTE", ld)); // Local date has no minute, defaults to 0
  }

  @Test
  void testExtractTemporalIntervalSecond() {
    TemporalInterval interval = TemporalInterval.parse("10 01:02:03", "DAY TO SECOND");
    assertEquals(Duration.ofDays(10).plusHours(1).plusMinutes(2).plusSeconds(3).toSeconds(),
        DateTimeExpressions.extract("SECOND", interval));
  }

  @Test
  void testExtractNullField() {
    assertNull(DateTimeExpressions.extract(null, Timestamp.valueOf("2023-01-01 00:00:00")));
  }

  @Test
  void testExtractNullSource() {
    assertNull(DateTimeExpressions.extract("YEAR", null));
  }

  @Test
  void testPlusTemporalIntervals() {
    TemporalInterval left = TemporalInterval.of(Period.ofYears(1), Duration.ofHours(1));
    TemporalInterval right = TemporalInterval.of(Period.ofMonths(6), Duration.ofMinutes(30));
    TemporalInterval result = (TemporalInterval) DateTimeExpressions.plus(left, right);
    assertEquals(Period.ofYears(1).plusMonths(6), result.period());
    assertEquals(Duration.ofHours(1).plusMinutes(30), result.duration());
  }

  @Test
  void testPlusTimestampAndInterval() {
    Timestamp ts = Timestamp.valueOf("2023-01-01 10:00:00");
    TemporalInterval interval = TemporalInterval.of(Period.ofMonths(1), Duration.ofHours(2));
    Timestamp result = (Timestamp) DateTimeExpressions.plus(ts, interval);
    assertEquals(Timestamp.valueOf("2023-02-01 12:00:00"), result);
  }

  @Test
  void testPlusDateAndIntervalHasTimeComponent() {
    Date date = Date.valueOf("2023-01-01");
    TemporalInterval interval = TemporalInterval.parse("1 02:00:00", "DAY TO SECOND");
    Object result = DateTimeExpressions.plus(date, interval);
    assertInstanceOf(Timestamp.class, result);
    assertEquals(Timestamp.valueOf("2023-01-02 02:00:00"), result);
  }

  @Test
  void testPlusDateAndIntervalNoTimeComponent() {
    Date date = Date.valueOf("2023-01-01");
    TemporalInterval interval = TemporalInterval.parse("1", "DAY");
    Object result = DateTimeExpressions.plus(date, interval);
    assertInstanceOf(Date.class, result);
    assertEquals(Date.valueOf("2023-01-02"), result);
  }

  @Test
  void testPlusTimeAndIntervalNoPeriod() {
    Time time = Time.valueOf("10:00:00");
    TemporalInterval interval = TemporalInterval.parse("30", "MINUTE");
    Time result = (Time) DateTimeExpressions.plus(time, interval);
    assertEquals(Time.valueOf("10:30:00"), result);
  }

  @Test
  void testPlusTimeAndIntervalWithPeriodThrowsException() {
    Time time = Time.valueOf("10:00:00");
    TemporalInterval interval = TemporalInterval.of(Period.ofMonths(1), Duration.ZERO);
    assertThrows(IllegalArgumentException.class, () -> DateTimeExpressions.plus(time, interval));
  }

  @Test
  void testPlusUnsupportedTypes() {
    assertNull(DateTimeExpressions.plus("string", 123));
  }

  @Test
  void testMinusTemporalIntervals() {
    TemporalInterval left = TemporalInterval.of(Period.ofYears(2), Duration.ofHours(5));
    TemporalInterval right = TemporalInterval.of(Period.ofYears(1), Duration.ofHours(2));
    TemporalInterval result = (TemporalInterval) DateTimeExpressions.minus(left, right);
    assertNotNull(result);
    assertEquals(Period.ofYears(1), result.period());
    assertEquals(Duration.ofHours(3), result.duration());
  }

  @Test
  void testMinusTimestampAndInterval() {
    Timestamp ts = Timestamp.valueOf("2023-02-01 12:00:00");
    TemporalInterval interval = TemporalInterval.of(Period.ofMonths(1), Duration.ofHours(2));
    Timestamp result = (Timestamp) DateTimeExpressions.minus(ts, interval);
    assertEquals(Timestamp.valueOf("2023-01-01 10:00:00"), result);
  }

  @Test
  void testMinusTemporalMinusTemporalTimestamp() {
    Timestamp ts1 = Timestamp.valueOf("2023-01-02 10:00:00");
    Timestamp ts2 = Timestamp.valueOf("2023-01-01 09:00:00");
    TemporalInterval result = (TemporalInterval) DateTimeExpressions.minus(ts1, ts2);
    assertNotNull(result);
    assertEquals(Period.ZERO, result.period());
    assertEquals(Duration.ofDays(1).plusHours(1), result.duration());
  }

  @Test
  void testMinusTemporalMinusTemporalDate() {
    Date d1 = Date.valueOf("2023-01-05");
    Date d2 = Date.valueOf("2023-01-02");
    TemporalInterval result = (TemporalInterval) DateTimeExpressions.minus(d1, d2);
    assertNotNull(result);
    assertEquals(Period.ofDays(3), result.period());
    assertEquals(Duration.ZERO, result.duration());
  }

  @Test
  void testMinusUnsupportedTypes() {
    assertNull(DateTimeExpressions.minus("string", 123));
  }

  @Test
  void testCastLiteralToBooleanTrue() {
    assertTrue((Boolean) DateTimeExpressions.castLiteral(createCastExpression("BOOLEAN"), "TRUE"));
    assertTrue((Boolean) DateTimeExpressions.castLiteral(createCastExpression("BOOL"), "1"));
    assertTrue((Boolean) DateTimeExpressions.castLiteral(createCastExpression("BOOLEAN"), BigDecimal.ONE));
  }

  @Test
  void testCastLiteralToBooleanFalse() {
    assertFalse((Boolean) DateTimeExpressions.castLiteral(createCastExpression("BOOLEAN"), "FALSE"));
    assertFalse((Boolean) DateTimeExpressions.castLiteral(createCastExpression("BOOL"), "0"));
    assertFalse((Boolean) DateTimeExpressions.castLiteral(createCastExpression("BOOLEAN"), BigDecimal.ZERO));
    assertFalse((Boolean) DateTimeExpressions.castLiteral(createCastExpression("BOOLEAN"), ""));
  }

  @Test
  void testCastLiteralToBooleanInvalid() {
    assertThrows(IllegalArgumentException.class,
        () -> DateTimeExpressions.castLiteral(createCastExpression("BOOLEAN"), "invalid"));
  }

  @Test
  void testCastLiteralToString() {
    assertEquals("hello", DateTimeExpressions.castLiteral(createCastExpression("VARCHAR"), "hello"));
    assertEquals("123", DateTimeExpressions.castLiteral(createCastExpression("STRING"), 123));
  }

  @Test
  void testCastLiteralToStringWithLength() {
    CastExpression cast = createCastExpression("VARCHAR(3)");
    assertEquals("hel", DateTimeExpressions.castLiteral(cast, "hello"));
  }

  @Test
  void testCastLiteralToInteger() {
    assertEquals(123, DateTimeExpressions.castLiteral(createCastExpression("INT"), "123"));
    assertEquals(123, DateTimeExpressions.castLiteral(createCastExpression("INTEGER"), 123.45));
  }

  @Test
  void testCastLiteralToBigDecimal() {
    assertEquals(new BigDecimal("123.45"), DateTimeExpressions.castLiteral(createCastExpression("DECIMAL"), "123.45"));
  }

  @Test
  void testCastLiteralToBigDecimalWithScale() {
    CastExpression cast = createCastExpression("DECIMAL(5,2)");
    assertEquals(new BigDecimal("123.46"), DateTimeExpressions.castLiteral(cast, "123.456"));
  }

  @Test
  void testCastLiteralToTimestamp() {
    Timestamp expected = Timestamp.valueOf("2023-01-01 12:30:00");
    assertEquals(expected, DateTimeExpressions.castLiteral(createCastExpression("TIMESTAMP"), "2023-01-01 12:30:00"));
    assertEquals(expected,
        DateTimeExpressions.castLiteral(createCastExpression("TIMESTAMP"), LocalDateTime.of(2023, 1, 1, 12, 30, 0)));
  }

  @Test
  void testCastLiteralToDate() {
    Date expected = Date.valueOf("2023-01-01");
    assertEquals(expected.toLocalDate(),
        ((Date) DateTimeExpressions.castLiteral(createCastExpression("DATE"), "2023-01-01")).toLocalDate());
    assertEquals(expected.toLocalDate(),
        ((Date) DateTimeExpressions.castLiteral(createCastExpression("DATE"), Timestamp.valueOf("2023-01-01 10:00:00")))
            .toLocalDate());
  }

  @Test
  void testCastLiteralToTime() {
    Time expected = Time.valueOf("10:30:00");
    assertEquals(expected.toLocalTime(),
        ((Time) DateTimeExpressions.castLiteral(createCastExpression("TIME"), "10:30:00")).toLocalTime());
    assertEquals(expected.toLocalTime(),
        ((Time) DateTimeExpressions.castLiteral(createCastExpression("TIME"), Timestamp.valueOf("2023-01-01 10:30:00")))
            .toLocalTime());
  }

  private CastExpression createCastExpression(String dataType) {
    CastExpression cast = new CastExpression();
    ColDataType colDataType = new ColDataType();
    colDataType.setDataType(dataType);
    cast.setColDataType(colDataType);
    return cast;
  }
}
