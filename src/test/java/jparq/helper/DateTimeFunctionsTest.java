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
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.helper.TemporalInterval;

public class DateTimeFunctionsTest {

  @BeforeEach
  public void setUp() {
    // Set a fixed zone for consistent test results
    System.setProperty("user.timezone", "UTC");
  }

  @Test
  void testEvaluateTimeKeyCurrentDate() {
    TimeKeyExpression expr = new TimeKeyExpression();
    expr.setStringValue("CURRENT_DATE");
    Object result = DateTimeFunctions.evaluateTimeKey(expr);
    assertInstanceOf(Date.class, result);
    assertEquals(Date.valueOf(LocalDate.now(ZoneId.of("UTC"))), result);
  }

  @Test
  void testEvaluateTimeKeyCurrentTime() {
    TimeKeyExpression expr = new TimeKeyExpression();
    expr.setStringValue("CURRENT_TIME");
    Object result = DateTimeFunctions.evaluateTimeKey(expr);
    assertInstanceOf(java.time.OffsetTime.class, result);
  }

  @Test
  void testEvaluateTimeKeyCurrentTimestamp() {
    TimeKeyExpression expr = new TimeKeyExpression();
    expr.setStringValue("CURRENT_TIMESTAMP");
    Object result = DateTimeFunctions.evaluateTimeKey(expr);
    assertInstanceOf(java.time.OffsetDateTime.class, result);
    // Allow for some leeway as system clock can tick between getting current
    // Instant and creating the result.
    long currentEpochSecond = Instant.now().getEpochSecond();
    long resultEpochSecond = ((java.time.OffsetDateTime) result).toInstant().getEpochSecond();
    assertTrue(Math.abs(currentEpochSecond - resultEpochSecond) <= 2);
  }

  @Test
  void testEvaluateTimeKeyUnsupported() {
    TimeKeyExpression expr = new TimeKeyExpression();
    expr.setStringValue("UNKNOWN_KEY");
    Object result = DateTimeFunctions.evaluateTimeKey(expr);
    assertEquals("UNKNOWN_KEY", result);
  }

  @Test
  void testEvaluateLocalTimeAndTimestamp() {
    TimeKeyExpression lt = new TimeKeyExpression();
    lt.setStringValue("LOCALTIME");
    assertInstanceOf(LocalTime.class, DateTimeFunctions.evaluateTimeKey(lt));

    TimeKeyExpression lts = new TimeKeyExpression();
    lts.setStringValue("LOCALTIMESTAMP");
    assertInstanceOf(LocalDateTime.class, DateTimeFunctions.evaluateTimeKey(lts));
  }

  @Test
  void testEvaluateTimeKeyNullExpr() {
    assertNull(DateTimeFunctions.evaluateTimeKey(null));
  }

  @Test
  void testToIntervalYearMonth() {
    IntervalExpression expr = new IntervalExpression();
    expr.setParameter("10");
    expr.setIntervalType("YEAR");
    TemporalInterval interval = DateTimeFunctions.toInterval(expr);
    assertEquals(Period.ofYears(10), interval.period());
    assertEquals(Duration.ZERO, interval.duration());
  }

  @Test
  void testToIntervalDayTime() {
    IntervalExpression expr = new IntervalExpression();
    expr.setParameter("1 02:03:04");
    expr.setIntervalType("DAY TO SECOND");
    TemporalInterval interval = DateTimeFunctions.toInterval(expr);
    assertEquals(Period.ZERO, interval.period());
    assertEquals(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4), interval.duration());
  }

  @Test
  void testToIntervalNullExpr() {
    TemporalInterval interval = DateTimeFunctions.toInterval(null);
    assertEquals(Period.ZERO, interval.period());
    assertEquals(Duration.ZERO, interval.duration());
  }

  @Test
  void testExtractTimestampYear() {
    Timestamp ts = Timestamp.valueOf("2023-01-15 10:30:45");
    assertEquals(2023, DateTimeFunctions.extract("YEAR", ts));
  }

  @Test
  void testExtractDateMonth() {
    Date d = Date.valueOf("2023-03-20");
    assertEquals(3, DateTimeFunctions.extract("MONTH", d));
  }

  @Test
  void testExtractTimeHour() {
    Time t = Time.valueOf("14:20:00");
    assertEquals(14, DateTimeFunctions.extract("HOUR", t));
  }

  @Test
  void testExtractLocalDateTimeDay() {
    LocalDateTime ldt = LocalDateTime.of(2022, 12, 25, 0, 0);
    assertEquals(25, DateTimeFunctions.extract("DAY", ldt));
  }

  @Test
  void testExtractLocalDateMinute() {
    LocalDate ld = LocalDate.of(2021, 11, 1);
    assertEquals(0, DateTimeFunctions.extract("MINUTE", ld)); // Local date has no minute, defaults to 0
  }

  @Test
  void testExtractTemporalIntervalSecond() {
    TemporalInterval interval = TemporalInterval.parse("10 01:02:03", "DAY TO SECOND");
    assertEquals(Duration.ofDays(10).plusHours(1).plusMinutes(2).plusSeconds(3).toSeconds(),
        DateTimeFunctions.extract("SECOND", interval));
  }

  @Test
  void testExtractNullField() {
    assertNull(DateTimeFunctions.extract(null, Timestamp.valueOf("2023-01-01 00:00:00")));
  }

  @Test
  void testExtractNullSource() {
    assertNull(DateTimeFunctions.extract("YEAR", null));
  }

  @Test
  void testPlusTemporalIntervals() {
    TemporalInterval left = TemporalInterval.of(Period.ofYears(1), Duration.ofHours(1));
    TemporalInterval right = TemporalInterval.of(Period.ofMonths(6), Duration.ofMinutes(30));
    TemporalInterval result = (TemporalInterval) DateTimeFunctions.plus(left, right);
    assertEquals(Period.ofYears(1).plusMonths(6), result.period());
    assertEquals(Duration.ofHours(1).plusMinutes(30), result.duration());
  }

  @Test
  void testPlusTimestampAndInterval() {
    Timestamp ts = Timestamp.valueOf("2023-01-01 10:00:00");
    TemporalInterval interval = TemporalInterval.of(Period.ofMonths(1), Duration.ofHours(2));
    Timestamp result = (Timestamp) DateTimeFunctions.plus(ts, interval);
    assertEquals(Timestamp.valueOf("2023-02-01 12:00:00"), result);
  }

  @Test
  void testPlusDateAndIntervalHasTimeComponent() {
    Date date = Date.valueOf("2023-01-01");
    TemporalInterval interval = TemporalInterval.parse("1 02:00:00", "DAY TO SECOND");
    Object result = DateTimeFunctions.plus(date, interval);
    assertInstanceOf(Timestamp.class, result);
    assertEquals(Timestamp.valueOf("2023-01-02 02:00:00"), result);
  }

  @Test
  void testPlusDateAndIntervalNoTimeComponent() {
    Date date = Date.valueOf("2023-01-01");
    TemporalInterval interval = TemporalInterval.parse("1", "DAY");
    Object result = DateTimeFunctions.plus(date, interval);
    assertInstanceOf(Date.class, result);
    assertEquals(Date.valueOf("2023-01-02"), result);
  }

  @Test
  void testPlusTimeAndIntervalNoPeriod() {
    Time time = Time.valueOf("10:00:00");
    TemporalInterval interval = TemporalInterval.parse("30", "MINUTE");
    Time result = (Time) DateTimeFunctions.plus(time, interval);
    assertEquals(Time.valueOf("10:30:00"), result);
  }

  @Test
  void testPlusTimeAndIntervalWithPeriodThrowsException() {
    Time time = Time.valueOf("10:00:00");
    TemporalInterval interval = TemporalInterval.of(Period.ofMonths(1), Duration.ZERO);
    assertThrows(IllegalArgumentException.class, () -> DateTimeFunctions.plus(time, interval));
  }

  @Test
  void testPlusUnsupportedTypes() {
    assertNull(DateTimeFunctions.plus("string", 123));
  }

  @Test
  void testMinusTemporalIntervals() {
    TemporalInterval left = TemporalInterval.of(Period.ofYears(2), Duration.ofHours(5));
    TemporalInterval right = TemporalInterval.of(Period.ofYears(1), Duration.ofHours(2));
    TemporalInterval result = (TemporalInterval) DateTimeFunctions.minus(left, right);
    assertNotNull(result);
    assertEquals(Period.ofYears(1), result.period());
    assertEquals(Duration.ofHours(3), result.duration());
  }

  @Test
  void testMinusTimestampAndInterval() {
    Timestamp ts = Timestamp.valueOf("2023-02-01 12:00:00");
    TemporalInterval interval = TemporalInterval.of(Period.ofMonths(1), Duration.ofHours(2));
    Timestamp result = (Timestamp) DateTimeFunctions.minus(ts, interval);
    assertEquals(Timestamp.valueOf("2023-01-01 10:00:00"), result);
  }

  @Test
  void testMinusTemporalMinusTemporalTimestamp() {
    Timestamp ts1 = Timestamp.valueOf("2023-01-02 10:00:00");
    Timestamp ts2 = Timestamp.valueOf("2023-01-01 09:00:00");
    TemporalInterval result = (TemporalInterval) DateTimeFunctions.minus(ts1, ts2);
    assertNotNull(result);
    assertEquals(Period.ZERO, result.period());
    assertEquals(Duration.ofDays(1).plusHours(1), result.duration());
  }

  @Test
  void testMinusTemporalMinusTemporalDate() {
    Date d1 = Date.valueOf("2023-01-05");
    Date d2 = Date.valueOf("2023-01-02");
    TemporalInterval result = (TemporalInterval) DateTimeFunctions.minus(d1, d2);
    assertNotNull(result);
    assertEquals(Period.ofDays(3), result.period());
    assertEquals(Duration.ZERO, result.duration());
  }

  @Test
  void testMinusUnsupportedTypes() {
    assertNull(DateTimeFunctions.minus("string", 123));
  }

  @Test
  void testCastLiteralToBooleanTrue() {
    assertTrue((Boolean) DateTimeFunctions.castLiteral(createCastExpression("BOOLEAN"), "TRUE"));
    assertTrue((Boolean) DateTimeFunctions.castLiteral(createCastExpression("BOOL"), "1"));
    assertTrue((Boolean) DateTimeFunctions.castLiteral(createCastExpression("BOOLEAN"), BigDecimal.ONE));
  }

  @Test
  void testCastLiteralToBooleanFalse() {
    assertFalse((Boolean) DateTimeFunctions.castLiteral(createCastExpression("BOOLEAN"), "FALSE"));
    assertFalse((Boolean) DateTimeFunctions.castLiteral(createCastExpression("BOOL"), "0"));
    assertFalse((Boolean) DateTimeFunctions.castLiteral(createCastExpression("BOOLEAN"), BigDecimal.ZERO));
    assertFalse((Boolean) DateTimeFunctions.castLiteral(createCastExpression("BOOLEAN"), ""));
  }

  @Test
  void testCastLiteralToBooleanInvalid() {
    assertThrows(IllegalArgumentException.class,
        () -> DateTimeFunctions.castLiteral(createCastExpression("BOOLEAN"), "invalid"));
  }

  @Test
  void testCastLiteralToString() {
    assertEquals("hello", DateTimeFunctions.castLiteral(createCastExpression("VARCHAR"), "hello"));
    assertEquals("123", DateTimeFunctions.castLiteral(createCastExpression("STRING"), 123));
  }

  @Test
  void testCastLiteralToStringWithLength() {
    CastExpression cast = createCastExpression("VARCHAR(3)");
    assertEquals("hel", DateTimeFunctions.castLiteral(cast, "hello"));
  }

  @Test
  void testCastLiteralToInteger() {
    assertEquals(123, DateTimeFunctions.castLiteral(createCastExpression("INT"), "123"));
    assertEquals(123, DateTimeFunctions.castLiteral(createCastExpression("INTEGER"), 123.45));
  }

  @Test
  void testCastLiteralToBigDecimal() {
    assertEquals(new BigDecimal("123.45"), DateTimeFunctions.castLiteral(createCastExpression("DECIMAL"), "123.45"));
  }

  @Test
  void testCastLiteralToBigDecimalWithScale() {
    CastExpression cast = createCastExpression("DECIMAL(5,2)");
    assertEquals(new BigDecimal("123.46"), DateTimeFunctions.castLiteral(cast, "123.456"));
  }

  @Test
  void testCastLiteralToTimestamp() {
    Timestamp expected = Timestamp.valueOf("2023-01-01 12:30:00");
    assertEquals(expected, DateTimeFunctions.castLiteral(createCastExpression("TIMESTAMP"), "2023-01-01 12:30:00"));
    assertEquals(expected,
        DateTimeFunctions.castLiteral(createCastExpression("TIMESTAMP"), LocalDateTime.of(2023, 1, 1, 12, 30, 0)));
  }

  @Test
  void testCastLiteralToDate() {
    Date expected = Date.valueOf("2023-01-01");
    assertEquals(expected.toLocalDate(),
        ((Date) DateTimeFunctions.castLiteral(createCastExpression("DATE"), "2023-01-01")).toLocalDate());
    assertEquals(expected.toLocalDate(),
        ((Date) DateTimeFunctions.castLiteral(createCastExpression("DATE"), Timestamp.valueOf("2023-01-01 10:00:00")))
            .toLocalDate());
  }

  @Test
  void testCastLiteralToTime() {
    Time expected = Time.valueOf("10:30:00");
    assertEquals(expected.toLocalTime(),
        ((Time) DateTimeFunctions.castLiteral(createCastExpression("TIME"), "10:30:00")).toLocalTime());
    assertEquals(expected.toLocalTime(),
        ((Time) DateTimeFunctions.castLiteral(createCastExpression("TIME"), Timestamp.valueOf("2023-01-01 10:30:00")))
            .toLocalTime());
  }

  // Additional tests for applyInterval
  @Test
  void testApplyIntervalWithLocalDateAndPeriod() {
    LocalDate ld = LocalDate.of(2023, 1, 15);
    TemporalInterval interval = TemporalInterval.of(Period.ofMonths(1), Duration.ZERO);
    Object result = DateTimeFunctions.plus(ld, interval);
    assertEquals(LocalDate.of(2023, 2, 15), result);
  }

  @Test
  void testApplyIntervalWithLocalDateAndDuration() {
    LocalDate ld = LocalDate.of(2023, 1, 15);
    TemporalInterval interval = TemporalInterval.of(Period.ZERO, Duration.ofHours(5));
    Object result = DateTimeFunctions.plus(ld, interval);
    assertEquals(LocalDateTime.of(2023, 1, 15, 5, 0), result);
  }

  @Test
  void testApplyIntervalWithLocalTimeAndDuration() {
    LocalTime lt = LocalTime.of(10, 30);
    TemporalInterval interval = TemporalInterval.of(Period.ZERO, Duration.ofMinutes(15));
    Object result = DateTimeFunctions.plus(lt, interval);
    assertEquals(LocalTime.of(10, 45), result);
  }

  @Test
  void testApplyIntervalWithOffsetDateTime() {
    OffsetDateTime odt = OffsetDateTime.of(2023, 1, 15, 10, 30, 0, 0, ZoneOffset.ofHours(2));
    TemporalInterval interval = TemporalInterval.of(Period.ofDays(1), Duration.ofHours(2));
    Object result = DateTimeFunctions.plus(odt, interval);
    assertEquals(OffsetDateTime.of(2023, 1, 16, 12, 30, 0, 0, ZoneOffset.ofHours(2)), result);
  }

  // Additional tests for toTimestamp, toDate, toTime
  @Test
  void testToTimestampWithNumber() {
    long millis = 1672531200000L; // 2023-01-01 00:00:00 UTC
    Timestamp expected = new Timestamp(millis);
    assertEquals(expected, DateTimeFunctions.castLiteral(createCastExpression("TIMESTAMP"), millis));
  }

  @Test
  void testToDateWithNumber() {
    long millis = 1672531200000L; // 2023-01-01
    Date expected = new Date(millis);
    assertEquals(expected, DateTimeFunctions.castLiteral(createCastExpression("DATE"), millis));
  }

  @Test
  void testToTimeWithNumber() {
    long millis = 37800000L; // 10:30:00
    Time expected = new Time(millis);
    assertEquals(expected, DateTimeFunctions.castLiteral(createCastExpression("TIME"), millis));
  }

  @Test
  void testToTimestampWithOffsetDateTime() {
    OffsetDateTime odt = OffsetDateTime.of(2023, 1, 1, 12, 30, 0, 0, ZoneOffset.UTC);
    Timestamp expected = Timestamp.from(odt.toInstant());
    assertEquals(expected, DateTimeFunctions.castLiteral(createCastExpression("TIMESTAMP"), odt));
  }

  @Test
  void testToTimestampInvalidString() {
    assertThrows(IllegalArgumentException.class,
        () -> DateTimeFunctions.castLiteral(createCastExpression("TIMESTAMP"), "invalid-date"));
  }

  // Additional tests for extract
  @Test
  void testExtractFromOffsetDateTime() {
    OffsetDateTime odt = OffsetDateTime.of(2023, 5, 20, 15, 0, 0, 0, ZoneOffset.UTC);
    assertEquals(2023, DateTimeFunctions.extract("YEAR", odt));
    assertEquals(15, DateTimeFunctions.extract("HOUR", odt));
  }

  // Additional tests for castLiteral
  @Test
  void testCastToNumericTypes() {
    assertEquals((byte) 123, DateTimeFunctions.castLiteral(createCastExpression("TINYINT"), "123"));
    assertEquals((short) 123, DateTimeFunctions.castLiteral(createCastExpression("SMALLINT"), "123"));
    assertEquals(123L, DateTimeFunctions.castLiteral(createCastExpression("BIGINT"), "123"));
    assertEquals(123.45f, DateTimeFunctions.castLiteral(createCastExpression("FLOAT"), "123.45"));
    assertEquals(123.45, DateTimeFunctions.castLiteral(createCastExpression("DOUBLE"), "123.45"));
  }

  @Test
  void testCastToCharTypes() {
    assertEquals("test", DateTimeFunctions.castLiteral(createCastExpression("CHAR"), "test"));
    assertEquals("test", DateTimeFunctions.castLiteral(createCastExpression("NCHAR"), "test"));
    assertEquals("test", DateTimeFunctions.castLiteral(createCastExpression("VARCHAR"), "test"));
    assertEquals("test", DateTimeFunctions.castLiteral(createCastExpression("NVARCHAR"), "test"));
    assertEquals("test", DateTimeFunctions.castLiteral(createCastExpression("CHARACTER VARYING"), "test"));
  }

  @Test
  void testCastToTextClob() {
    assertEquals("long text", DateTimeFunctions.castLiteral(createCastExpression("TEXT"), "long text"));
    assertEquals("long text", DateTimeFunctions.castLiteral(createCastExpression("CLOB"), "long text"));
  }

  @Test
  void testMinusBetweenTemporalValues() {
    Timestamp later = Timestamp.valueOf("2023-01-02 00:00:00");
    Timestamp earlier = Timestamp.valueOf("2023-01-01 12:00:00");
    TemporalInterval interval = (TemporalInterval) DateTimeFunctions.minus(later, earlier);
    assertEquals(Duration.ofHours(12), interval.duration());
    assertEquals(Period.ZERO, interval.period());
  }

  @Test
  void testIntervalArithmeticBetweenIntervals() {
    TemporalInterval twoDays = TemporalInterval.parse("2", "DAY");
    TemporalInterval twelveHours = TemporalInterval.parse("12", "HOUR");
    TemporalInterval diff = (TemporalInterval) DateTimeFunctions.minus(twoDays, twelveHours);
    assertEquals(Period.ofDays(2), diff.period());
    assertEquals(Duration.ofHours(-12), diff.duration());
  }

  @Test
  void testDatePartsAcrossTemporalTypes() {
    Date sqlDate = Date.valueOf(LocalDate.of(2024, 2, 29));
    assertEquals(5, DateTimeFunctions.dayOfWeek(sqlDate));
    assertEquals(60, DateTimeFunctions.dayOfYear(sqlDate));
    assertEquals(2, DateTimeFunctions.month(sqlDate));
    assertEquals(1, DateTimeFunctions.quarter(sqlDate));
    assertEquals(2024, DateTimeFunctions.year(sqlDate));

    LocalDateTime ldt = LocalDateTime.of(2024, 12, 31, 23, 59, 58);
    assertEquals(3, DateTimeFunctions.dayOfWeek(ldt));
    assertEquals(366, DateTimeFunctions.dayOfYear(ldt));
    assertEquals(23, DateTimeFunctions.hour(ldt));
    assertEquals(59, DateTimeFunctions.minute(ldt));
    assertEquals(58, DateTimeFunctions.second(ldt));

    OffsetDateTime odt = OffsetDateTime.of(ldt, ZoneOffset.UTC);
    assertEquals(DateTimeFunctions.week(odt), DateTimeFunctions.week(ldt));
  }

  @Test
  void testTimestampAddAndDiffCalendarUnits() {
    Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2024, 1, 31, 10, 0));
    Object added = DateTimeFunctions.timestampAdd(java.util.Arrays.asList("SQL_TSI_MONTH", 1, ts));
    assertInstanceOf(Timestamp.class, added);
    assertEquals(LocalDateTime.of(2024, 2, 29, 10, 0), ((Timestamp) added).toLocalDateTime());

    Long diffDays = DateTimeFunctions.timestampDiff(java.util.Arrays.asList("SQL_TSI_DAY",
        Timestamp.valueOf("2024-01-01 00:00:00"), Timestamp.valueOf("2024-01-11 00:00:00")));
    assertEquals(10L, diffDays);

    Long diffHours = DateTimeFunctions.timestampDiff(java.util.Arrays.asList("SQL_TSI_HOUR",
        LocalDateTime.of(2024, 3, 10, 10, 0), LocalDateTime.of(2024, 3, 10, 15, 30)));
    assertEquals(5L, diffHours);
  }

  @Test
  void testNullInputsReturnNull() {
    assertNull(DateTimeFunctions.dayOfWeek(null));
    assertNull(DateTimeFunctions.hour(null));
    assertNull(DateTimeFunctions.timestampAdd(null));
    assertNull(DateTimeFunctions.timestampDiff(null));
  }

  private CastExpression createCastExpression(String dataType) {
    CastExpression cast = new CastExpression();
    ColDataType colDataType = new ColDataType();
    colDataType.setDataType(dataType);
    cast.setColDataType(colDataType);
    return cast;
  }
}
