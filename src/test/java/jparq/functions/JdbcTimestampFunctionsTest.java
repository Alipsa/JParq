package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/** JDBC regression tests for timestamp interval constants. */
class JdbcTimestampFunctionsTest {

  /**
   * Executes every JDBC interval with native and escape syntax in both query
   * shapes.
   *
   * @param unit
   *          JDBC interval constant
   * @param amount
   *          interval count
   * @param expected
   *          timestamp after adding the interval count
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "SQL_TSI_FRAC_SECOND, 123456789, 2024-01-01 00:00:00.123456789", "SQL_TSI_SECOND, 2, 2024-01-01 00:00:02",
      "SQL_TSI_FRAC_SECOND, 1, 2024-01-01 00:00:00.000000001", "SQL_TSI_FRAC_SECOND, -1, 2023-12-31 23:59:59.999999999",
      "SQL_TSI_MINUTE, 2, 2024-01-01 00:02:00", "SQL_TSI_HOUR, 2, 2024-01-01 02:00:00",
      "SQL_TSI_DAY, 2, 2024-01-03 00:00:00", "SQL_TSI_WEEK, 2, 2024-01-15 00:00:00",
      "SQL_TSI_MONTH, 2, 2024-03-01 00:00:00", "SQL_TSI_QUARTER, 2, 2024-07-01 00:00:00",
      "SQL_TSI_YEAR, 2, 2026-01-01 00:00:00", "SQL_TSI_FRAC_SECOND, 3000000000, 2024-01-01 00:00:03",
      "SQL_TSI_FRAC_SECOND, -3000000000, 2023-12-31 23:59:57"
  })
  void timestampIntervalsExecute(String unit, long amount, String expected) throws Exception {
    var directory = Paths.get(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    for (int mode = 0; mode < 64; mode++) {
      boolean predicate = (mode & 1) != 0;
      boolean projection = (mode & 2) != 0;
      boolean from = (mode & 4) != 0;
      boolean escape = (mode & 8) != 0;
      String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
      boolean lowerCase = (mode & 16) != 0;
      boolean literalEscape = (mode & 32) != 0;
      String token = lowerCase ? unit.toLowerCase(Locale.ROOT) : unit;
      String start = literalEscape ? "{ts '2024-01-01 00:00:00'}" : "TIMESTAMP '2024-01-01 00:00:00'";
      String end = literalEscape ? "{ts '" + expected + "'}" : "TIMESTAMP '" + expected + "'";
      String add = "TIMESTAMPADD(" + token + ", " + amount + ", " + start + ")";
      String diff = "TIMESTAMPDIFF(" + token + ", " + start + ", " + end + ")";
      String query = "SELECT " + (escape ? "{fn " + add + "}, {fn " + diff + "}" : add + ", " + diff)
          + (from ? " FROM mtcars WHERE cyl = 6 LIMIT 1" : "");
      try (var connection = DriverManager.getConnection(url);
          var statement = connection.prepareStatement(query);
          var result = statement.executeQuery()) {
        assertTrue(result.next(), query);
        assertEquals(Timestamp.valueOf(expected), result.getTimestamp(1), query);
        assertEquals(amount, result.getLong(2), query);
        assertFalse(result.next(), query);
      }
    }
  }

  /** Uses long arithmetic when converting large quarter counts into months. */
  @Test
  void timestampAddLargeQuarterCount() {
    LocalDateTime start = LocalDateTime.of(2024, 1, 1, 0, 0);
    assertEquals(LocalDateTime.of(250002024, 1, 1, 0, 0),
        DateTimeFunctions.timestampAdd(List.of("SQL_TSI_QUARTER", 1000000000L, start)));
    assertEquals(LocalDateTime.of(-249997976, 1, 1, 0, 0),
        DateTimeFunctions.timestampAdd(List.of("SQL_TSI_QUARTER", -1000000000L, start)));
  }

  /**
   * Preserves large JDBC date and timestamp results within the storage range.
   *
   * @throws Exception
   *           if JDBC execution fails
   */
  @Test
  void largeJdbcTemporalResultsRemainExact() throws Exception {
    var directory = Paths.get(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String query = "SELECT TIMESTAMPADD(SQL_TSI_YEAR, 280000000, TIMESTAMP '2024-01-01 00:00:00.123456789'), "
        + "TIMESTAMPADD(SQL_TSI_YEAR, 280000000, CAST(TIMESTAMP '2024-01-01 00:00:00' AS DATE))";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement(query);
        var result = statement.executeQuery()) {
      assertTrue(result.next());
      assertEquals(LocalDateTime.of(280002024, 1, 1, 0, 0, 0, 123456789), result.getTimestamp(1).toLocalDateTime());
      assertEquals(LocalDate.of(280002024, 1, 1), result.getDate(2).toLocalDate());
      assertFalse(result.next());
    }
  }

  /**
   * Preserves JDBC calendar normalization and the valid year-1 boundary.
   *
   * @param zone
   *          JVM default time zone for the query
   * @param expression
   *          timestamp addition crossing a gap
   * @param expected
   *          normalized JDBC value
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @CsvSource(value = {
      "Europe/Stockholm | TIMESTAMPADD(SQL_TSI_HOUR, 2, TIMESTAMP '2024-03-31 00:30:00') | 2024-03-31 03:30:00.0",
      "Europe/Stockholm | TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '2024-03-30 02:30:00') | 2024-03-31 03:30:00.0",
      "America/New_York | TIMESTAMPADD(SQL_TSI_HOUR, 2, TIMESTAMP '2024-03-10 00:30:00') | 2024-03-10 03:30:00.0",
      "UTC | TIMESTAMPADD(SQL_TSI_HOUR, 2, TIMESTAMP '2024-03-31 00:30:00') | 2024-03-31 02:30:00.0",
      "Pacific/Kiritimati | TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1994-12-30 12:00:00') | 1995-01-01 12:00:00.0",
      "Pacific/Kiritimati | TIMESTAMPADD(SQL_TSI_DAY, 1, DATE '1994-12-30') | 1995-01-01",
      "UTC | TIMESTAMPADD(SQL_TSI_YEAR, -2023, TIMESTAMP '2024-01-01 00:00:00') | 0001-01-01 00:00:00.0",
      "UTC | TIMESTAMPADD(SQL_TSI_YEAR, -2023, DATE '2024-01-01') | 0001-01-01",
      "UTC | TIMESTAMPADD(SQL_TSI_DAY, 1, DATE '1582-10-04') | 1582-10-15",
      "Europe/Stockholm | TIMESTAMPADD(SQL_TSI_DAY, 1, DATE '1582-10-04') | 1582-10-15",
      "UTC | TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1582-10-04 12:00:00') | 1582-10-15 12:00:00.0",
      "Europe/Stockholm | TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1582-10-04 12:00:00') | 1582-10-15 12:00:00.0"
  }, delimiter = '|')
  void timestampAddPreservesCalendarNormalization(String zone, String expression, String expected) throws Exception {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      var directory = Paths.get(getClass().getResource("/mtcars.parquet").toURI()).getParent();
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
          var statement = connection.prepareStatement("SELECT " + expression);
          var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals(expected, result.getString(1));
        assertFalse(result.next());
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  /**
   * Keeps unsupported units distinct from datetime field overflow (issue #345).
   */
  @Test
  void unsupportedTimestampUnitIsNotRangeOverflow() {
    assertThrows(UnsupportedTemporalTypeException.class,
        () -> DateTimeFunctions.timestampAdd(List.of("SQL_TSI_HOUR", 1L, LocalDate.of(2024, 1, 1))));
  }

  /**
   * Classifies numeric and datetime field overflow through the JDBC boundary.
   *
   * @param expression
   *          overflowing timestamp expression
   * @param expectedState
   *          expected JDBC SQLState
   * @throws Exception
   *           if JDBC setup fails
   */
  @ParameterizedTest
  @CsvSource(value = {
      "TIMESTAMPADD(SQL_TSI_QUARTER, 9223372036854775807, TIMESTAMP '2024-01-01 00:00:00') | 22003",
      "TIMESTAMPADD(SQL_TSI_QUARTER, -9223372036854775807, TIMESTAMP '2024-01-01 00:00:00') | 22003",
      "TIMESTAMPADD(SQL_TSI_SECOND, 9223372036854775808, TIMESTAMP '2024-01-01 00:00:00') | 22003",
      "TIMESTAMPDIFF(SQL_TSI_FRAC_SECOND, TIMESTAMP '1700-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00') | 22003",
      "TIMESTAMPDIFF(SQL_TSI_FRAC_SECOND, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '1700-01-01 00:00:00') | 22003",
      "TIMESTAMPADD(SQL_TSI_DAY, 9000000000000000, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_MONTH, 9223372036854775807, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_SECOND, 9223372036854775807, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_YEAR, 2000000000, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_YEAR, -2024, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_YEAR, -2024, DATE '2024-01-01') | 22008",
      "TIMESTAMPADD(SQL_TSI_DAY, -9000000000000000, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_MONTH, -9223372036854775807, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_SECOND, -9223372036854775807, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_YEAR, -2000000000, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_YEAR, 293000000, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_YEAR, -293000000, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_DAY, 365000000000, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_DAY, -365000000000, TIMESTAMP '2024-01-01 00:00:00') | 22008",
      "TIMESTAMPADD(SQL_TSI_YEAR, 999997975, CAST(TIMESTAMP '2024-01-01 00:00:00' AS DATE)) | 22008",
      "TIMESTAMPADD(SQL_TSI_YEAR, -293000000, CAST(TIMESTAMP '2024-01-01 00:00:00' AS DATE)) | 22008",
      "TIMESTAMPDIFF(SQL_TSI_YEAR, TIMESTAMP '2024-01-01 00:00:00', "
          + "TIMESTAMPADD(SQL_TSI_YEAR, 999997975, TIMESTAMP '2024-01-01 00:00:00')) | 22008"
  }, delimiter = '|')
  void timestampOverflowIsClassified(String expression, String expectedState) throws Exception {
    var directory = Paths.get(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      SQLException failure = assertThrows(SQLException.class, () -> {
        try (var statement = connection.prepareStatement("SELECT " + expression);
            var result = statement.executeQuery()) {
          assertTrue(result.next());
          result.getObject(1);
        }
      });
      assertEquals(expectedState, failure.getSQLState());
    }
  }

  /**
   * Supports date literal escapes nested inside JDBC function escapes.
   *
   * @param unit
   *          calendar interval constant
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SQL_TSI_DAY", "SQL_TSI_WEEK", "SQL_TSI_MONTH", "SQL_TSI_QUARTER", "SQL_TSI_YEAR"
  })
  void timestampIntervalsWithDateEscapes(String unit) throws Exception {
    var directory = Paths.get(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String query = "SELECT {fn TIMESTAMPDIFF(" + unit + ", {d '2024-01-01'}, " + "{fn TIMESTAMPADD(" + unit
        + ", 2, {d '2024-01-01'})})}";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement(query);
        var result = statement.executeQuery()) {
      assertTrue(result.next());
      assertEquals(2, result.getLong(1));
    }
  }

  /**
   * Keeps similarly named columns bound outside the interval keyword position.
   *
   * @param expression
   *          projection using a column or an interval keyword
   * @param expected
   *          expected projection value
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @CsvSource(value = {
      "SQL_TSI_DAY | 2", "ABS(SQL_TSI_DAY) | 2",
      "TIMESTAMPADD(SQL_TSI_DAY, SQL_TSI_DAY, TIMESTAMP '2024-01-01 00:00:00') | 2024-01-03 00:00:00.0",
      "TIMESTAMPADD(t.SQL_TSI_HOUR, 2, TIMESTAMP '2024-01-01 00:00:00') | 2024-01-01 00:02:00.0",
      "TIMESTAMPADD(\"SQL_TSI_HOUR\", 2, TIMESTAMP '2024-01-01 00:00:00') | 2024-01-03 00:00:00.0",
      "TIMESTAMPADD('SQL_TSI_DAY', 2, TIMESTAMP '2024-01-01 00:00:00') | 2024-01-03 00:00:00.0"
  }, delimiter = '|', quoteCharacter = '`')
  void timestampUnitsPreserveColumnBindings(String expression, String expected) throws Exception {
    var directory = Paths.get(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    String query = "SELECT " + expression + " FROM (SELECT 2 AS sql_tsi_day, 'SQL_TSI_MINUTE' AS sql_tsi_hour, "
        + "'SQL_TSI_DAY' AS \"SQL_TSI_HOUR\") t";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement(query);
        var result = statement.executeQuery()) {
      assertTrue(result.next());
      assertEquals(expected, result.getString(1));
      assertFalse(result.next());
    }
  }

  /**
   * Normalizes interval keywords inside nested query scopes and predicates.
   *
   * @param query
   *          query returning a two-day difference
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "WITH c AS (SELECT TIMESTAMPADD(SQL_TSI_DAY, 2, TIMESTAMP '2024-01-01 00:00:00') AS ts) "
          + "SELECT TIMESTAMPDIFF(SQL_TSI_DAY, TIMESTAMP '2024-01-01 00:00:00', ts) FROM c",
      "SELECT (SELECT TIMESTAMPDIFF(SQL_TSI_DAY, TIMESTAMP '2024-01-01 00:00:00', "
          + "TIMESTAMP '2024-01-03 00:00:00')) FROM mtcars LIMIT 1",
      "SELECT 2 FROM mtcars WHERE cyl = 6 AND TIMESTAMPDIFF(SQL_TSI_DAY, "
          + "TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-03 00:00:00') = 2 LIMIT 1"
  })
  void timestampUnitsNormalizeNestedScopes(String query) throws Exception {
    var directory = Paths.get(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement(query);
        var result = statement.executeQuery()) {
      assertTrue(result.next());
      assertEquals(2, result.getLong(1));
      assertFalse(result.next());
    }
  }
}
