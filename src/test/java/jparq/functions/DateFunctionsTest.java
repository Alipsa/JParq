package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.DriverManager;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
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

  /** Exercises lenient temporal literals through JDBC projections and filters. */
  @Test
  void lenientTemporalLiteralsExecute() {
    jparqSql.query("""
        SELECT TIMESTAMP '2024-06-30' AS midnight,
               TIMESTAMP '2024-06-30T10:00:00' AS morning,
               TIME '10:00:00.5' AS clock
        FROM mtcars WHERE TIMESTAMP '2024-06-30' IS NOT NULL LIMIT 1
        """, rs -> {
      try {
        assertTrue(rs.next());
        assertEquals(Timestamp.valueOf("2024-06-30 00:00:00"), rs.getTimestamp("midnight"));
        assertEquals(Timestamp.valueOf("2024-06-30 10:00:00"), rs.getTimestamp("morning"));
        assertEquals(Time.valueOf("10:00:00"), rs.getTime("clock"));
        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  /**
   * Rejects invalid temporal fields when retrieving JDBC projection values.
   *
   * @param expression
   *          an invalid temporal literal or cast
   * @throws Exception
   *           if the test connection cannot be opened
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "DATE '2024-02-30'", "TIME '25:00:00'", "CAST('2024-02-30' AS DATE)", "CAST('25:00:00' AS TIME)",
      "TIMESTAMP '2024-02-30 00:00:00'", "TIMESTAMP '2024-06-30 25:00:00'", "CAST('2024-02-30 00:00:00' AS TIMESTAMP)",
      "CAST('2024-06-30 25:00:00' AS TIMESTAMP)"
  })
  void invalidTemporalValuesFailThroughJdbc(String expression) throws Exception {
    Path dir = Paths.get(DateFunctionsTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + dir);
        var statement = connection.createStatement()) {
      var failure = assertThrows(SQLException.class, () -> {
        try (var result = statement.executeQuery("SELECT " + expression + " FROM mtcars LIMIT 1")) {
          result.next();
          result.getObject(1);
        }
      });
      assertTrue(failure.getMessage().contains("Invalid "), failure::getMessage);
      assertInstanceOf(IllegalArgumentException.class, failure.getCause());
    }
  }

  /**
   * Preserves unpadded temporal literals and casts through JDBC.
   *
   * @param expression
   *          SQL literal or cast
   * @param expected
   *          normalized value
   */
  @ParameterizedTest
  @CsvSource(value = {
      "DATE '2024-6-3'|2024-06-03", "CAST('2024-6-3' AS DATE)|2024-06-03", "TIME '9:05:00'|09:05:00",
      "CAST('9:05:00' AS TIME)|09:05:00", "TIME '10:00'|10:00:00", "TIME '10:00:00.5'|10:00:00",
      "TIME '23:59:59.123456789'|23:59:59", "TIMESTAMP '2024-6-3 9:5:0'|2024-06-03 09:05:00.0",
      "CAST('2024-6-3 9:5:0' AS TIMESTAMP)|2024-06-03 09:05:00.0"
  }, delimiter = '|', quoteCharacter = '"')
  void unpaddedTemporalLiteralsExecute(String expression, String expected) {
    jparqSql.query("SELECT " + expression + " FROM mtcars LIMIT 1", rs -> {
      try {
        assertTrue(rs.next());
        assertEquals(expected, rs.getObject(1).toString());
        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  /**
   * Casts date and time strings assembled from row values during aggregation.
   *
   * @throws Exception
   *           if the connection or query fails
   */
  @Test
  void unpaddedTemporalRowValuesExecute() throws Exception {
    Path dir = Paths.get(DateFunctionsTest.class.getResource("/acme").toURI());
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + dir);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT COUNT(*) FROM employees
            WHERE CAST(CAST(id AS VARCHAR) || ':05:00' AS TIME) IS NOT NULL
              AND CAST('2024-6-' || CAST(id AS VARCHAR) AS DATE) IS NOT NULL
            """)) {
      assertTrue(result.next());
      assertEquals(5, result.getInt(1));
      assertFalse(result.next());
    }
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
