package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.List;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.helper.TemporalInterval;

public class DateTimeFunctionsTest {

  private String previousTimeZone;

  @BeforeEach
  public void setUp() {
    previousTimeZone = System.getProperty("user.timezone");
    // Set a fixed zone for consistent test results
    System.setProperty("user.timezone", "UTC");
  }

  @AfterEach
  public void tearDown() {
    if (previousTimeZone == null) {
      System.clearProperty("user.timezone");
    } else {
      System.setProperty("user.timezone", previousTimeZone);
    }
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
    assertEquals(3L, DateTimeFunctions.minus(d1, d2));
    assertEquals(-3L, DateTimeFunctions.minus(d2, d1));
    assertEquals(0L, DateTimeFunctions.minus(d1, d1));
    assertEquals(3L, DateTimeFunctions.minus(d1.toLocalDate(), d2));
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

  /**
   * Rejects invalid calendar dates in public conversions and SQL casts.
   *
   * @param text
   *          invalid date text
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "2024-02-30", "2023-02-29", "2024-04-31", "2024-13-01", "2024-01-00", "2024-02-30T10:00:00", "2024-2-30",
      "2023-2-29"
  })
  void invalidDatesAreRejected(String text) {
    assertThrows(IllegalArgumentException.class, () -> DateTimeFunctions.toDate(text));
    assertThrows(IllegalArgumentException.class,
        () -> DateTimeFunctions.castLiteral(createCastExpression("DATE"), text));
  }

  /**
   * Rejects invalid time fields in public conversions and SQL casts.
   *
   * @param text
   *          invalid time text
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "25:00:00", "24:00:00", "12:60:00", "12:00:60", "-1:00:00", "25:00:00.5", "2024-06-30T25:00:00", "25:0:0",
      "10:60:0"
  })
  void invalidTimesAreRejected(String text) {
    assertThrows(IllegalArgumentException.class, () -> DateTimeFunctions.toTime(text));
    assertThrows(IllegalArgumentException.class,
        () -> DateTimeFunctions.castLiteral(createCastExpression("TIME"), text));
  }

  /**
   * Preserves unpadded SQL date fields without allowing calendar rollover.
   *
   * @param text
   *          SQL date text
   * @param expected
   *          normalized date
   */
  @ParameterizedTest
  @CsvSource({
      "2024-6-3,2024-06-03", "2024-06-3,2024-06-03", "2024-6-03,2024-06-03", "2024-02-29,2024-02-29"
  })
  void unpaddedDatesRemainAvailable(String text, String expected) {
    Date date = Date.valueOf(expected);
    assertEquals(date, DateTimeFunctions.toDate(text));
    assertEquals(date, DateTimeFunctions.castLiteral(createCastExpression("DATE"), text));
  }

  /**
   * Preserves flexible clock fields, optional seconds, and nanosecond input.
   *
   * @param text
   *          SQL time text
   * @param expected
   *          normalized time at JDBC TIME precision
   * @param millis
   *          expected fractional milliseconds
   */
  @ParameterizedTest
  @CsvSource({
      "9:05:00,09:05:00,0", "9:5:0,09:05:00,0", "10:00,10:00:00,0", "10:00:00.5,10:00:00,500",
      "23:59:59.123456789,23:59:59,123"
  })
  void unpaddedTimesRemainAvailable(String text, String expected, int millis) {
    Time time = Time.valueOf(expected);
    time.setTime(time.getTime() + millis);
    assertEquals(time, DateTimeFunctions.toTime(text));
    assertEquals(time, DateTimeFunctions.castLiteral(createCastExpression("TIME"), text));
  }

  /**
   * Validates timestamps using the same strict calendar and clock rules.
   *
   * @param text
   *          invalid timestamp text
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "2024-02-30 00:00:00", "2024-06-30 25:00:00", "2023-2-29 9:05:00", "2024-6-3 10:60:00", "2024-02-30",
      "2024-06-30T25:00:00Z", "2024-02-30T10:00:00+01:00"
  })
  void invalidTimestampsAreRejected(String text) {
    assertThrows(IllegalArgumentException.class, () -> DateTimeFunctions.toTimestamp(text));
    assertThrows(IllegalArgumentException.class,
        () -> DateTimeFunctions.castLiteral(createCastExpression("TIMESTAMP"), text));
  }

  /**
   * Retains valid timestamp forms and nanosecond precision after validation.
   *
   * @param text
   *          SQL timestamp text
   * @param expected
   *          normalized timestamp
   */
  @ParameterizedTest
  @CsvSource({
      "2024-6-3 9:5:0,2024-06-03 09:05:00", "2024-6-3,2024-06-03 00:00:00", "2024-02-29 10:00,2024-02-29 10:00:00",
      "2024-6-3T9:05:00.123456789,2024-06-03 09:05:00.123456789", "2024-06-03T09:05:00,2024-06-03 09:05:00"
  })
  void flexibleTimestampsRemainAvailable(String text, String expected) {
    Timestamp timestamp = Timestamp.valueOf(expected);
    assertEquals(timestamp, DateTimeFunctions.toTimestamp(text));
    assertEquals(timestamp, DateTimeFunctions.castLiteral(createCastExpression("TIMESTAMP"), text));
  }

  /** Keeps offset timestamp strings tied to their instant and full precision. */
  @Test
  void timestampOffsetsRemainAvailable() {
    Timestamp expected = Timestamp.from(Instant.parse("2024-06-03T08:05:00.123456789Z"));
    assertEquals(expected, DateTimeFunctions.toTimestamp("2024-06-03T08:05:00.123456789Z"));
    assertEquals(expected, DateTimeFunctions.toTimestamp("2024-06-03T09:05:00.123456789+01:00"));
  }

  /** Keeps leap days, fractional times, and ISO date-time inputs available. */
  @Test
  void validDateAndTimeFormsRemainAvailable() {
    Date leapDay = Date.valueOf(LocalDate.of(2024, 2, 29));
    final Time time = Time.valueOf(LocalTime.of(10, 30, 15));
    assertEquals(leapDay, DateTimeFunctions.toDate("2024-02-29"));
    assertEquals(leapDay, DateTimeFunctions.toDate("2024-02-29T10:30:15"));
    assertEquals(leapDay, DateTimeFunctions.toDate("2024-02-29 10:30:15"));
    time.setTime(time.getTime() + 123);
    assertEquals(time, DateTimeFunctions.toTime("10:30:15.123456789"));
    time.setTime(time.getTime() + 377);
    assertEquals(time, DateTimeFunctions.toTime("2024-02-29T10:30:15.5"));
    assertEquals(time, DateTimeFunctions.toTime("2024-02-29 10:30:15.5"));
  }

  /** Retains milliseconds from LocalTime, SQL text, and ISO date-time inputs. */
  @Test
  void retainsTimeMilliseconds() {
    for (int nanos : new int[]{
        0, 123_456_789, 500_000_000, 999_999_999
    }) {
      LocalTime local = LocalTime.of(23, 59, 59, nanos);
      Time expected = Time.valueOf("23:59:59");
      expected.setTime(expected.getTime() + nanos / 1_000_000);
      assertEquals(expected, DateTimeFunctions.toTime(local));
      assertEquals(expected, DateTimeFunctions.toTime(local.toString()));
      assertEquals(expected, DateTimeFunctions.castLiteral(createCastExpression("TIME"), local.toString()));
      assertEquals(local.withNano(nanos / 1_000_000 * 1_000_000), DateTimeFunctions.toLocalTime(expected));
    }
  }

  /**
   * Removes dates from JDBC time conversions while keeping local clock fields.
   *
   * @param zoneId
   *          JVM time zone, including differing historical offsets
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UTC", "America/New_York", "Asia/Kathmandu"
  })
  void normalizesTimeDates(String zoneId) {
    var previous = java.util.TimeZone.getDefault();
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(zoneId));
    try {
      Time expected = Time.valueOf("10:00:00");
      expected.setTime(expected.getTime() + 500);
      Timestamp timestamp = Timestamp.valueOf("2024-06-01 10:00:00.500999");
      Time dated = new Time(timestamp.getTime());
      for (Object source : List.of(timestamp, dated, timestamp.getTime(), LocalTime.of(10, 0, 0, 500_999_999))) {
        Time actual = DateTimeFunctions.toTime(source);
        assertEquals(expected, actual);
        assertEquals(expected.hashCode(), actual.hashCode());
      }
      assertSame(expected, DateTimeFunctions.toTime(expected));
      assertEquals(timestamp.getTime(), dated.getTime());
    } finally {
      java.util.TimeZone.setDefault(previous);
    }
  }

  /** Tests the TIMESTAMPADD implementation independently of SQL unit parsing. */
  @Test
  void timeIntervalHelpersRetainMilliseconds() {
    Time source = Time.valueOf("23:59:59");
    source.setTime(source.getTime() + 750);
    Time expected = Time.valueOf("00:00:00");
    expected.setTime(expected.getTime() + 250);
    assertEquals(expected, DateTimeFunctions.timestampAdd(List.of("SQL_TSI_FRAC_SECOND", 500_000_000L, source)));
    assertEquals(expected, DateTimeFunctions.plus(source, TemporalInterval.parse("0.5", "SECOND")));
    Time hourLater = Time.valueOf("00:59:59");
    hourLater.setTime(hourLater.getTime() + 750);
    assertEquals(hourLater, DateTimeFunctions.timestampAdd(List.of("SQL_TSI_HOUR", 1L, source)));
    assertEquals(hourLater, DateTimeFunctions.plus(source, TemporalInterval.parse("1", "HOUR")));
    assertEquals(source, DateTimeFunctions.minus(hourLater, TemporalInterval.parse("1", "HOUR")));
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

  /**
   * Rejects non-finite inputs and invalid numeric text with neutral diagnostics.
   */
  @Test
  void invalidNumericCastsCarrySqlState() {
    for (Object value : List.of("Infinity", "-Infinity", "NaN", Double.POSITIVE_INFINITY, Double.NaN,
        Float.NEGATIVE_INFINITY, Float.NaN, "not a number", Date.valueOf("2020-01-01"))) {
      for (String type : List.of("DOUBLE", "REAL", "INTEGER", "DECIMAL")) {
        var failure = assertThrows(IllegalArgumentException.class,
            () -> DateTimeFunctions.castLiteral(createCastExpression(type), value), value + " AS " + type);
        var classified = assertInstanceOf(SQLDataException.class, SqlExceptions.findSqlStateFailure(failure));
        assertEquals("22018", classified.getSQLState());
        assertEquals("Cannot cast value '" + value + "' to a numeric value", classified.getMessage());
        assertInstanceOf(NumberFormatException.class, classified.getCause());
      }
    }
  }

  /**
   * Checks integer bounds after truncation for every supported target alias.
   */
  @Test
  void integerCastsCheckRangeAfterTruncation() {
    for (String type : List.of("TINYINT", "SMALLINT", "INT2", "INT", "INTEGER", "SIGNED", "INT4", "BIGINT", "INT8")) {
      long minimum = switch (type) {
        case "TINYINT" -> Byte.MIN_VALUE;
        case "SMALLINT", "INT2" -> Short.MIN_VALUE;
        case "BIGINT", "INT8" -> Long.MIN_VALUE;
        default -> Integer.MIN_VALUE;
      };
      BigDecimal min = BigDecimal.valueOf(minimum);
      BigDecimal max = min.negate().subtract(BigDecimal.ONE);
      for (BigDecimal boundary : List.of(min, max)) {
        Number result = (Number) DateTimeFunctions.castLiteral(createCastExpression(type), boundary);
        assertEquals(boundary.longValueExact(), result.longValue(), type);
        BigDecimal fraction = new BigDecimal(boundary.signum() < 0 ? "-0.9" : "0.9");
        assertEquals(result, DateTimeFunctions.castLiteral(createCastExpression(type), boundary.add(fraction)), type);
      }
      for (Object value : List.of(min.subtract(BigDecimal.ONE), max.add(BigDecimal.ONE), "1e999999999",
          "-1e999999999")) {
        var failure = assertThrows(IllegalArgumentException.class,
            () -> DateTimeFunctions.castLiteral(createCastExpression(type), value), value + " AS " + type);
        assertEquals("22003",
            assertInstanceOf(SQLDataException.class, SqlExceptions.findSqlStateFailure(failure)).getSQLState());
      }
      for (String value : List.of("1e-999999999", "-1e-999999999", "0e999999999", "0e-999999999")) {
        Number result = (Number) DateTimeFunctions.castLiteral(createCastExpression(type), value);
        assertEquals(0L, result.longValue(), type);
      }
      assertNull(DateTimeFunctions.castLiteral(createCastExpression(type), null));
    }
  }

  /** Ensures long inputs do not overwhelm numeric conversion diagnostics. */
  @Test
  void numericCastDiagnosticsBoundValues() {
    for (String value : List.of("9".repeat(200000), "x".repeat(200000))) {
      var failure = assertThrows(IllegalArgumentException.class,
          () -> DateTimeFunctions.castLiteral(createCastExpression("DOUBLE PRECISION"), value));
      assertTrue(failure.getMessage().length() < 200, failure::getMessage);
      assertTrue(failure.getMessage().contains(value.substring(0, 64) + "…"));
      var classified = assertInstanceOf(SQLDataException.class, SqlExceptions.findSqlStateFailure(failure));
      assertTrue(classified.getMessage().length() < 200, classified::getMessage);
      assertEquals(value.startsWith("9") ? "22003" : "22018", classified.getSQLState());
    }
    String digits = "9".repeat(200000);
    var decimalFailure = assertThrows(IllegalArgumentException.class,
        () -> DateTimeFunctions.castLiteral(createCastExpression("DECIMAL(5,0)"), digits));
    assertTrue(decimalFailure.getMessage().length() < 200, decimalFailure::getMessage);
    assertTrue(decimalFailure.getMessage().contains(digits.substring(0, 64) + "…"));
    assertEquals("22003",
        assertInstanceOf(SQLDataException.class, SqlExceptions.findSqlStateFailure(decimalFailure)).getSQLState());
    String emojiValue = "x".repeat(63) + "😀tail";
    var emojiFailure = assertThrows(IllegalArgumentException.class,
        () -> DateTimeFunctions.castLiteral(createCastExpression("INTEGER"), emojiValue));
    assertEquals("Cannot cast value '" + "x".repeat(63) + "…' to a numeric value", emojiFailure.getMessage());
  }

  /** Keeps invalid BOOLEAN diagnostics bounded and Unicode-safe. */
  @Test
  void booleanCastDiagnosticsBoundValues() {
    for (String value : List.of("x".repeat(200000), "x".repeat(63) + "😀tail")) {
      var failure = assertThrows(IllegalArgumentException.class,
          () -> DateTimeFunctions.castLiteral(createCastExpression("BOOLEAN"), value));
      assertEquals("Cannot cast value '" + SqlExceptions.diagnosticValue(value) + "' to BOOLEAN", failure.getMessage());
      assertTrue(failure.getMessage().length() < 120);
    }
  }

  /** Checks finite floating-point boundaries and overflow for all aliases. */
  @Test
  void floatingCastsRejectOverflow() {
    for (String type : List.of("FLOAT", "REAL", "FLOAT4", "DOUBLE", "DOUBLE PRECISION", "FLOAT8")) {
      boolean single = List.of("FLOAT", "REAL", "FLOAT4").contains(type);
      Object maximum = single ? Float.toString(Float.MAX_VALUE) : Double.toString(Double.MAX_VALUE);
      for (String sign : List.of("", "-")) {
        Number result = (Number) DateTimeFunctions.castLiteral(createCastExpression(type), sign + maximum);
        assertTrue(Double.isFinite(result.doubleValue()), type);
        assertEquals(single ? Float.class : Double.class, result.getClass(), type);
        String overflow = sign + (single ? "1e39" : "1e400");
        var failure = assertThrows(IllegalArgumentException.class,
            () -> DateTimeFunctions.castLiteral(createCastExpression(type), overflow));
        assertEquals("22003",
            assertInstanceOf(SQLDataException.class, SqlExceptions.findSqlStateFailure(failure)).getSQLState());
      }
      assertNull(DateTimeFunctions.castLiteral(createCastExpression(type), null));
    }
  }

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
