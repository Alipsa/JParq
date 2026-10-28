package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.util.Locale;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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
      "SQL_TSI_YEAR, 2, 2026-01-01 00:00:00"
  })
  void timestampIntervalsExecute(String unit, int amount, String expected) throws Exception {
    var directory = Paths.get(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    for (int mode = 0; mode < 16; mode++) {
      boolean predicate = (mode & 1) != 0;
      boolean projection = (mode & 2) != 0;
      boolean from = (mode & 4) != 0;
      boolean escape = (mode & 8) != 0;
      String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
      String token = escape ? unit : unit.toLowerCase(Locale.ROOT);
      String add = "TIMESTAMPADD(" + token + ", " + amount + ", TIMESTAMP '2024-01-01 00:00:00')";
      String diff = "TIMESTAMPDIFF(" + token + ", TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '" + expected + "')";
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
