package jparq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/** Regression tests for compatible temporal ARRAY elements in issue 273. */
class TemporalArrayPromotionTest {

  /**
   * Preserves explicitly cast dates and timestamp fractions through JDBC
   * projections and materializations.
   *
   * @param sql
   *          query producing the promoted array
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT ARRAY[NULL, CAST(DATE '2020-01-01' AS TIMESTAMP), TIMESTAMP '2020-02-03 12:34:56.123', NULL]",
      "SELECT a FROM (SELECT ARRAY[NULL, "
          + "CAST(DATE '2020-01-01' AS TIMESTAMP), TIMESTAMP '2020-02-03 12:34:56.123', NULL] AS a) t",
      "WITH c AS (SELECT ARRAY[NULL, "
          + "CAST(DATE '2020-01-01' AS TIMESTAMP), TIMESTAMP '2020-02-03 12:34:56.123', NULL] AS a) SELECT a FROM c"
  })
  void acceptsExplicitTimestampCasts(String sql, @TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1));
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          null, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-02-03 12:34:56.123"), null
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  /**
   * Returns TIME array elements as {@link java.sql.Time} in direct projections
   * and materializations, with or without representation promotion.
   *
   * @param sql
   *          query projecting or materializing TIME array elements
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT ARRAY[LOCALTIME, LOCALTIME]", "SELECT a FROM (SELECT ARRAY[LOCALTIME, LOCALTIME] a) t",
      "SELECT ARRAY[TIME '10:00:00', TIME '12:00:00']", "SELECT ARRAY[TIME '10:00:00', LOCALTIME]",
      "SELECT a FROM (SELECT ARRAY[TIME '10:00:00', TIME '12:00:00'] AS a) t",
      "SELECT a FROM (SELECT ARRAY[TIME '10:00:00', LOCALTIME] AS a) t",
      "WITH c AS (SELECT ARRAY[TIME '10:00:00', LOCALTIME] AS a) SELECT a FROM c"
  })
  void materializesTimeArrays(String sql, @TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertTrue(result.next());
      Object[] values = (Object[]) result.getObject(1);
      assertInstanceOf(Time.class, values[0]);
      if (sql.contains("TIME '10:00:00'")) {
        assertEquals(Time.valueOf("10:00:00"), values[0]);
      }
      assertInstanceOf(Time.class, values[1]);
      assertFalse(result.next());
    }
  }

  /**
   * Accepts time keys alongside temporal literals, including zoned promotions.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if execution fails
   */
  @Test
  void promotesTimeKeysAndNestedArrays(@TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT ARRAY[CURRENT_TIMESTAMP, TIMESTAMP '2026-07-01 10:00:00'],
                   ARRAY[TIME '10:00:00', CURRENT_TIME],
                   ARRAY[LOCALTIMESTAMP, TIMESTAMP '2026-01-01 10:00:00'],
                   ARRAY[LOCALTIME, TIME '10:00:00'],
                   ARRAY[ARRAY[CAST(DATE '2020-01-01' AS TIMESTAMP)], ARRAY[TIMESTAMP '2020-02-03 12:34:56.123456789']],
                   ARRAY[TIME '12:00:00.5', LOCALTIME], ARRAY[LOCALTIMESTAMP, LOCALTIMESTAMP]
            """)) {
      assertTrue(result.next());
      ZoneOffset session = DateTimeFunctions.sessionOffset();
      Object[] zonedTimestamps = (Object[]) result.getObject(1);
      assertInstanceOf(OffsetDateTime.class, zonedTimestamps[0]);
      assertEquals(LocalDateTime.parse("2026-07-01T10:00:00").atOffset(session), zonedTimestamps[1]);
      Object[] zonedTimes = (Object[]) result.getObject(2);
      assertEquals(LocalTime.of(10, 0).atOffset(session), zonedTimes[0]);
      assertInstanceOf(OffsetTime.class, zonedTimes[1]);
      for (Object value : (Object[]) result.getObject(3)) {
        assertInstanceOf(Timestamp.class, value);
      }
      Object[] times = (Object[]) result.getObject(4);
      assertInstanceOf(Time.class, times[0]);
      assertEquals(Time.valueOf("10:00:00"), times[1]);
      assertArrayEquals(new Object[]{
          new Object[]{
              Timestamp.valueOf("2020-01-01 00:00:00")
          }, new Object[]{
              Timestamp.valueOf("2020-02-03 12:34:56.123456789")
          }
      }, (Object[]) result.getObject(5));
      Object[] fractional = (Object[]) result.getObject(6);
      assertEquals(new Time(Time.valueOf("12:00:00").getTime() + 500), fractional[0]);
      assertInstanceOf(Time.class, fractional[1]);
      for (Object value : (Object[]) result.getObject(7)) {
        assertInstanceOf(Timestamp.class, value);
      }
      assertFalse(result.next());
    }
  }
}
