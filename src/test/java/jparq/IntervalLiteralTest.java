package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.helper.TemporalInterval;

/** JDBC regressions for qualified interval literals and their data errors. */
class IntervalLiteralTest {

  @TempDir
  Path directory;

  /**
   * Checks each supported single-field unit with embedded and unary signs.
   *
   * @param unit
   *          the interval unit
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"
  })
  void qualifiedUnitsAndSigns(String unit) throws Exception {
    for (String amount : new String[]{
        "2", "-2", "+2"
    }) {
      for (String sign : new String[]{
          "", "+", "-"
      }) {
        TemporalInterval expected = TemporalInterval.parse(amount, unit);
        if ("-".equals(sign)) {
          expected = expected.negate();
        }
        assertEquals(expected, query(sign + "INTERVAL '" + amount + "' " + unit));
        assertEquals(expected, query(sign + "INTERVAL '" + amount + " " + unit + "'"));
      }
    }
  }

  /**
   * Verifies fractional seconds and the existing compound qualifier.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void fractionalAndCompoundIntervals() throws Exception {
    assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofMillis(1250)), query("INTERVAL '1.25' SECOND"));
    assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)),
        query("INTERVAL '1 02:03:04' DAY TO SECOND"));
    assertEquals(TemporalInterval.parse("1 day 2 hours", null), query("INTERVAL '1 day 2 hours'"));
  }

  /**
   * Verifies calendar and time arithmetic with qualified intervals.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void dateAndTimestampArithmetic() throws Exception {
    assertEquals(Date.valueOf("2024-01-01"), query("DATE '2024-01-02' + -INTERVAL '1' DAY"));
    assertEquals(Date.valueOf("2024-01-01"), query("DATE '2024-01-02' - INTERVAL '1' DAY"));
    assertEquals(Date.valueOf("2024-02-29"), query("DATE '2024-01-31' + INTERVAL '1' MONTH"));
    assertEquals(Timestamp.valueOf("2024-01-02 01:00:00"),
        query("TIMESTAMP '2024-01-01 23:00:00' + INTERVAL '2' HOUR"));
    assertEquals(Timestamp.valueOf("2024-01-01 22:59:59.5"),
        query("TIMESTAMP '2024-01-01 23:00:00' - INTERVAL '0.5' SECOND"));
  }

  /**
   * Checks that malformed text is classified at the JDBC boundary.
   *
   * @param expression
   *          the invalid interval expression
   * @throws Exception
   *           if the assertion cannot execute
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "INTERVAL 'oops' DAY", "INTERVAL '' DAY", "INTERVAL '1'", "INTERVAL 'oops day'", "INTERVAL '1 lightyear'",
      "INTERVAL '1 broken' DAY TO SECOND", "DATE '2024-01-01' + INTERVAL 'oops' DAY", "(SELECT INTERVAL 'oops' DAY)",
      "CASE WHEN INTERVAL 'oops' DAY = INTERVAL '1' DAY THEN 1 ELSE 0 END"
  })
  void invalidIntervalText(String expression) throws Exception {
    SQLDataException failure = assertThrows(SQLDataException.class, () -> query(expression));
    assertEquals("22007", failure.getSQLState());
    assertNotNull(failure.getCause());
    assertTrue(failure.getMessage().contains("INTERVAL"));
  }

  /**
   * Checks that unrepresentable interval amounts retain a range SQLState.
   *
   * @param expression
   *          the out-of-range interval
   * @throws Exception
   *           if the assertion cannot execute
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "INTERVAL '2147483648' DAY", "INTERVAL '9223372036854775807' HOUR", "INTERVAL '9223372036854775807' SECOND",
      "INTERVAL '0.5' MONTH"
  })
  void unrepresentableIntervalAmount(String expression) throws Exception {
    SQLDataException failure = assertThrows(SQLDataException.class, () -> query(expression));
    assertEquals("22003", failure.getSQLState());
    assertNotNull(failure.getCause());
  }

  /**
   * Executes a scalar expression and reads its JDBC value.
   *
   * @param expression
   *          the SQL expression
   * @return the expression value
   * @throws Exception
   *           if execution or value access fails
   */
  private Object query(String expression) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression)) {
      assertTrue(result.next());
      Object value = result.getObject(1);
      if (expression.startsWith("INTERVAL")) {
        assertInstanceOf(TemporalInterval.class, value);
      }
      return value;
    }
  }
}
