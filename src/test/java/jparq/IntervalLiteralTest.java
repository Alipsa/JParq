package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.function.DateTimeFunctions;
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
        int value = amount.startsWith("-") ? -2 : 2;
        value = "-".equals(sign) ? -value : value;
        TemporalInterval expected = switch (unit) {
          case "YEAR" -> TemporalInterval.of(Period.ofYears(value), Duration.ZERO);
          case "MONTH" -> TemporalInterval.of(Period.ofMonths(value), Duration.ZERO);
          case "DAY" -> TemporalInterval.of(Period.ofDays(value), Duration.ZERO);
          case "HOUR" -> TemporalInterval.of(Period.ZERO, Duration.ofHours(value));
          case "MINUTE" -> TemporalInterval.of(Period.ZERO, Duration.ofMinutes(value));
          case "SECOND" -> TemporalInterval.of(Period.ZERO, Duration.ofSeconds(value));
          default -> throw new AssertionError("Unexpected unit: " + unit);
        };
        assertEquals(expected, query(sign + "INTERVAL '" + amount + "' " + unit));
        assertEquals(expected, query(sign + "INTERVAL '" + amount + " " + unit + "'"));
      }
    }
  }

  /**
   * Verifies fractional seconds and compound qualifier support.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void fractionalAndCompoundIntervals() throws Exception {
    assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofMillis(1250)), query("INTERVAL '1.25' SECOND"));
    assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)),
        query("INTERVAL '1 02:03:04' DAY TO SECOND"));
    assertEquals(TemporalInterval.of(Period.ofDays(1), Duration.ofHours(2)), query("INTERVAL '1 day 2 hours'"));
  }

  /**
   * Checks that qualifier precision does not become part of the unit name.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void qualifiedPrecision() throws Exception {
    assertEquals(TemporalInterval.of(Period.ofDays(1), Duration.ZERO), query("INTERVAL '1' DAY(3)"));
    assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofMillis(1500)), query("INTERVAL '1.5' SECOND(2,3)"));
    assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofSeconds(93784)),
        query("INTERVAL '1 02:03:04' DAY TO SECOND(3)"));
  }

  /**
   * Preserves the whole compound interval's sign and fractional seconds.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void signedFractionalCompoundIntervals() throws Exception {
    for (String qualifier : new String[]{
        "DAY TO SECOND", "DAY(3) TO SECOND(3)"
    }) {
      assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofSeconds(-93784)),
          query("INTERVAL '-1 02:03:04' " + qualifier));
      assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofMillis(93784500)),
          query("INTERVAL '1 02:03:04.5' " + qualifier));
      assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofMillis(-93784500)),
          query("INTERVAL '-1 02:03:04.5' " + qualifier));
      assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofMillis(93784500)),
          query("INTERVAL '+1 02:03:04.5' " + qualifier));
      assertEquals(TemporalInterval.of(Period.ZERO, Duration.ofMillis(-500)),
          query("INTERVAL '-0 00:00:00.5' " + qualifier));
    }
    assertEquals(Timestamp.valueOf("2023-12-31 20:56:55.5"),
        query("TIMESTAMP '2024-01-01 23:00:00' + INTERVAL '-1 02:03:04.5' DAY TO SECOND"));
  }

  /**
   * Distinguishes unsupported SQL interval features from invalid literal data.
   *
   * @param expression
   *          the unsupported interval expression
   * @throws Exception
   *           if the assertion cannot execute
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '1' WEEK", "INTERVAL (1+1) DAY", "(SELECT INTERVAL '1' WEEK)",
      "DATE '2024-01-01' + INTERVAL '1' WEEK", "INTERVAL '1 week'", "INTERVAL '1 weeks'", "(SELECT INTERVAL '1 week')"
  })
  void unsupportedIntervalFeatures(String expression) throws Exception {
    SQLFeatureNotSupportedException failure = assertThrows(SQLFeatureNotSupportedException.class,
        () -> query(expression));
    assertEquals("0A000", failure.getSQLState());
    assertTrue(failure.getMessage().contains("Unsupported interval"));
    assertFalse(failure.getMessage().contains("value: null"));
    assertNotNull(failure.getCause());
  }

  /**
   * Rejects malformed compound day fields as format errors rather than ranges.
   *
   * @param days
   *          the malformed day field
   * @throws Exception
   *           if the assertion cannot execute
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "-+1", "+-1", "--1", "++1", "1e1", "1E1", "1.0", "1.5", ".5", "-1.0"
  })
  void invalidCompoundDayField(String days) throws Exception {
    SQLDataException failure = assertThrows(SQLDataException.class,
        () -> query("INTERVAL '" + days + " 02:03:04' DAY TO SECOND"));
    assertEquals("22007", failure.getSQLState());
    assertTrue(failure.getMessage().contains("Invalid DAY TO SECOND interval literal"));
  }

  /**
   * Classifies digit-only compound day overflow as a range error.
   *
   * @param days
   *          the overflowing day field
   * @throws Exception
   *           if the assertion cannot execute
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "9223372036854775808", "-9223372036854775808", "9999999999999999999999999999999999", "9223372036854775807"
  })
  void compoundDayOverflow(String days) throws Exception {
    SQLDataException failure = assertThrows(SQLDataException.class,
        () -> query("INTERVAL '" + days + " 02:03:04' DAY TO SECOND"));
    assertEquals("22003", failure.getSQLState());
    assertNotNull(failure.getCause());
  }

  /**
   * Includes the complete parsed interval expression in operand diagnostics.
   *
   * @param expression
   *          the unsupported operand form
   * @throws Exception
   *           if the assertion cannot execute
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "INTERVAL (1+1) DAY", "INTERVAL c DAY"
  })
  void expressionOperandDiagnostics(String expression) throws Exception {
    var parsed = CCJSqlParserUtil.parseExpression(expression);
    var failure = assertThrows(IllegalArgumentException.class,
        () -> DateTimeFunctions.toInterval((IntervalExpression) parsed));
    assertEquals("Unsupported interval expression operand: " + parsed, failure.getMessage());
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
