package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;
import se.alipsa.jparq.helper.TemporalInterval;

/** Tests for date functions. */
public class DateFunctionsTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = DateFunctionsTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testYear() {
    executeSingleInt("SELECT EXTRACT(YEAR FROM timestamp '2024-06-30 12:34:56') AS year_val FROM mtcars LIMIT 1",
        "year_val", 2024);
  }

  @Test
  void testMonth() {
    executeSingleInt("SELECT EXTRACT(MONTH FROM timestamp '2024-06-30 12:34:56') AS month_val FROM mtcars LIMIT 1",
        "month_val", 6);
  }

  @Test
  void testDay() {
    executeSingleInt("SELECT EXTRACT(DAY FROM timestamp '2024-06-30 12:34:56') AS day_val FROM mtcars LIMIT 1",
        "day_val", 30);
  }

  @Test
  void testHour() {
    executeSingleInt("SELECT EXTRACT(HOUR FROM timestamp '2024-06-30 22:34:56') AS hour_val FROM mtcars LIMIT 1",
        "hour_val", 22);
  }

  @Test
  void testMinute() {
    executeSingleInt("SELECT EXTRACT(MINUTE FROM timestamp '2024-06-30 22:34:56') AS minute_val FROM mtcars LIMIT 1",
        "minute_val", 34);
  }

  @Test
  void testSecond() {
    executeSingleInt("SELECT EXTRACT(SECOND FROM timestamp '2024-06-30 22:34:45') AS second_val FROM mtcars LIMIT 1",
        "second_val", 45);
  }

  @Test
  void testCurrentDate() {
    AtomicReference<Date> value = new AtomicReference<>();

    jparqSql.query("SELECT CURRENT_DATE AS current_date FROM mtcars LIMIT 1", rs -> {
      try {
        assertTrue(rs.next(), "Expected a row");
        value.set(rs.getDate("current_date"));
      } catch (SQLException e) {
        fail(e);
      }
    });
    Date current = value.get();
    assertNotNull(current, "CURRENT_DATE should produce a value");
    assertEquals(LocalDate.now(), current.toLocalDate(), "CURRENT_DATE should be today");
  }

  @Test
  void testCurrentTime() {
    final LocalTime start = LocalTime.now();
    AtomicReference<Object> value = new AtomicReference<>();

    jparqSql.query("SELECT CURRENT_TIME AS current_time FROM mtcars LIMIT 1", rs -> {
      try {
        assertTrue(rs.next(), "Expected a row");
        value.set(rs.getObject("current_time"));
      } catch (SQLException e) {
        fail(e);
      }
    });

    final LocalTime end = LocalTime.now();
    Object current = value.get();
    assertNotNull(current, "CURRENT_TIME should produce a value");
    assertInstanceOf(java.time.OffsetTime.class, current);
    final LocalTime actual = ((java.time.OffsetTime) current).toLocalTime();
    assertFalse(actual.isBefore(start.minusSeconds(1)), "Time should be within the expected window");
    assertFalse(actual.isAfter(end.plusSeconds(1)), "Time should be within the expected window");
  }

  @Test
  void testCurrentTimestamp() {
    final Instant start = Instant.now();
    AtomicReference<Object> value = new AtomicReference<>();

    jparqSql.query("SELECT CURRENT_TIMESTAMP AS current_ts FROM mtcars LIMIT 1", rs -> {
      try {
        assertTrue(rs.next(), "Expected a row");
        value.set(rs.getObject("current_ts"));
      } catch (SQLException e) {
        fail(e);
      }
    });

    final Instant end = Instant.now();
    Object current = value.get();
    assertNotNull(current, "CURRENT_TIMESTAMP should produce a value");
    assertInstanceOf(java.time.OffsetDateTime.class, current);
    final Instant actual = ((java.time.OffsetDateTime) current).toInstant();
    assertFalse(actual.isBefore(start.minusSeconds(1)), "Timestamp should not precede the captured window");
    assertFalse(actual.isAfter(end.plusSeconds(1)), "Timestamp should not exceed the captured window");
  }

  @Test
  void testLocalTimeAndLocalTimestamp() {
    final LocalTime startTime = LocalTime.now();
    final LocalDateTime startTs = LocalDateTime.now();
    AtomicReference<Object> localTime = new AtomicReference<>();
    AtomicReference<Object> localTimestamp = new AtomicReference<>();

    jparqSql.query("SELECT LOCALTIME AS local_time, LOCALTIMESTAMP AS local_ts FROM mtcars LIMIT 1", rs -> {
      try {
        assertTrue(rs.next(), "Expected a row");
        localTime.set(rs.getObject("local_time"));
        localTimestamp.set(rs.getObject("local_ts"));
      } catch (SQLException e) {
        fail(e);
      }
    });

    final LocalTime endTime = LocalTime.now();
    final LocalDateTime endTs = LocalDateTime.now();

    Object lt = localTime.get();
    assertNotNull(lt, "LOCALTIME should produce a value");
    assertTrue(lt instanceof LocalTime || lt instanceof Time, "LOCALTIME should not contain timezone information");
    LocalTime ltVal = lt instanceof LocalTime ? (LocalTime) lt : ((Time) lt).toLocalTime();
    assertFalse(ltVal.isBefore(startTime.minusSeconds(1)), "LOCALTIME should be within the expected window");
    assertFalse(ltVal.isAfter(endTime.plusSeconds(1)), "LOCALTIME should be within the expected window");

    Object lts = localTimestamp.get();
    assertNotNull(lts, "LOCALTIMESTAMP should produce a value");
    assertTrue(lts instanceof LocalDateTime || lts instanceof Timestamp,
        "LOCALTIMESTAMP should not include timezone information");
    LocalDateTime ltsVal = lts instanceof LocalDateTime ? (LocalDateTime) lts : ((Timestamp) lts).toLocalDateTime();
    assertFalse(ltsVal.isBefore(startTs.minusSeconds(1)), "LOCALTIMESTAMP should be within the expected window");
    assertFalse(ltsVal.isAfter(endTs.plusSeconds(1)), "LOCALTIMESTAMP should be within the expected window");
  }

  @Test
  void testJdbcFunctionEscapesAreTranslated() {
    AtomicReference<Object> curDate = new AtomicReference<>();
    AtomicReference<Object> curTime = new AtomicReference<>();
    AtomicReference<Object> nowTs = new AtomicReference<>();

    jparqSql.query(
        "SELECT {fn CURDATE()} AS cur_date, {fn CURTIME()} AS cur_time, {fn NOW()} AS cur_ts " + "FROM mtcars LIMIT 1",
        rs -> {
          try {
            assertTrue(rs.next(), "Expected a row");
            curDate.set(rs.getObject("cur_date"));
            curTime.set(rs.getObject("cur_time"));
            nowTs.set(rs.getObject("cur_ts"));
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertTrue(curDate.get() instanceof Date || curDate.get() instanceof LocalDate);
    assertNotNull(curTime.get());
    assertTrue(curTime.get() instanceof java.time.OffsetTime || curTime.get() instanceof java.time.LocalTime
        || curTime.get() instanceof Time, "CURTIME should yield a temporal value");
    assertNotNull(nowTs.get());
    assertTrue(nowTs.get() instanceof java.time.OffsetDateTime || nowTs.get() instanceof Timestamp
        || nowTs.get() instanceof LocalDateTime);
  }

  @Test
  void testDateAdd() {
    jparqSql.query("SELECT DATE '2025-01-01' + INTERVAL '1 month' AS next_month, "
        + "timestamp '2025-01-01 03:04:05' + INTERVAL '5 days 2 hours' AS ts_plus, "
        + "timestamp '2025-01-10 12:00:00' - INTERVAL '2 hours' AS ts_minus " + "FROM mtcars LIMIT 1", rs -> {
          try {
            assertTrue(rs.next(), "Expected a row");
            Date nextMonth = rs.getDate("next_month");
            assertEquals(LocalDate.of(2025, 2, 1), nextMonth.toLocalDate(), "Month addition should roll correctly");

            Timestamp plus = rs.getTimestamp("ts_plus");
            assertEquals(LocalDateTime.of(2025, 1, 6, 5, 4, 5), plus.toLocalDateTime(),
                "Timestamp addition should include both days and hours");

            Timestamp minus = rs.getTimestamp("ts_minus");
            assertEquals(LocalDateTime.of(2025, 1, 10, 10, 0, 0), minus.toLocalDateTime(),
                "Timestamp subtraction should remove two hours");
          } catch (SQLException e) {
            fail(e);
          }
        });
  }

  @Test
  void testDateDiff() {
    jparqSql.query("SELECT timestamp '2025-02-01 00:00:00' - timestamp '2025-01-01 00:00:00' AS ts_diff, "
        + "date '2025-01-10' - date '2025-01-01' AS date_diff FROM mtcars LIMIT 1", rs -> {
          try {
            assertTrue(rs.next(), "Expected a row");
            Object tsObj = rs.getObject("ts_diff");
            assertInstanceOf(TemporalInterval.class, tsObj, "Timestamp subtraction should yield an interval");
            TemporalInterval tsInterval = (TemporalInterval) tsObj;
            assertEquals(Duration.ofDays(31), tsInterval.toDuration(), "Expected 31 day interval");

            Object dateObj = rs.getObject("date_diff");
            assertInstanceOf(TemporalInterval.class, dateObj, "Date subtraction should yield an interval");
            TemporalInterval dateInterval = (TemporalInterval) dateObj;
            assertEquals(Duration.ofDays(9), dateInterval.toDuration(), "Expected 9 day interval");
          } catch (SQLException e) {
            fail(e);
          }
        });
  }

  private void executeSingleInt(String sql, String column, int expected) {
    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next(), "Expected a row");
        assertEquals(expected, rs.getInt(column), "Unexpected value for " + column);
      } catch (SQLException e) {
        fail(e);
      }
    });
  }
}
