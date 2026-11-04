package jparq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Regression tests for compatible temporal ARRAY elements in issue 273. */
class TemporalArrayPromotionTest {

  /**
   * Preserves promoted dates and timestamp fractions through JDBC projections and
   * materializations.
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
      "SELECT ARRAY[NULL, DATE '2020-01-01', TIMESTAMP '2020-02-03 12:34:56.123', NULL]",
      "SELECT a FROM (SELECT ARRAY[NULL, DATE '2020-01-01', TIMESTAMP '2020-02-03 12:34:56.123', NULL] AS a) t",
      "WITH c AS (SELECT ARRAY[NULL, DATE '2020-01-01', TIMESTAMP '2020-02-03 12:34:56.123', NULL] AS a) SELECT a FROM c"
  })
  void promotesDatesToTimestamps(String sql, @TempDir Path directory) throws Exception {
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
   * Keeps normalized local-time elements typed as TIME during materialization.
   *
   * @param sql
   *          query materializing TIME array elements
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
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
      assertEquals(java.sql.Time.valueOf("10:00:00"), values[0]);
      assertInstanceOf(java.sql.Time.class, values[1]);
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
            SELECT ARRAY[CURRENT_TIMESTAMP, TIMESTAMP '2026-01-01 10:00:00'],
                   ARRAY[TIME '10:00:00', CURRENT_TIME],
                   ARRAY[LOCALTIMESTAMP, TIMESTAMP '2026-01-01 10:00:00'],
                   ARRAY[LOCALTIME, TIME '10:00:00'],
                   ARRAY[ARRAY[DATE '2020-01-01'], ARRAY[TIMESTAMP '2020-02-03 12:34:56.123456789']]
            """)) {
      assertTrue(result.next());
      for (Object value : (Object[]) result.getObject(1)) {
        assertInstanceOf(OffsetDateTime.class, value);
      }
      for (Object value : (Object[]) result.getObject(2)) {
        assertInstanceOf(OffsetTime.class, value);
      }
      for (Object value : (Object[]) result.getObject(3)) {
        assertInstanceOf(Timestamp.class, value);
      }
      Object[] times = (Object[]) result.getObject(4);
      assertEquals(times[0].getClass(), times[1].getClass());
      assertArrayEquals(new Object[]{
          new Object[]{
              Timestamp.valueOf("2020-01-01 00:00:00")
          }, new Object[]{
              Timestamp.valueOf("2020-02-03 12:34:56.123456789")
          }
      }, (Object[]) result.getObject(5));
      assertFalse(result.next());
    }
  }
}
