package jparq.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.IntervalQualifier;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.TemporalInterval;

/** Verifies exact interval materialization and SQL text output. */
class IntervalTypeTest {

  /** Ensures encoding preserves both components without consuming its buffer. */
  @Test
  void intervalComponentsRoundTrip() {
    TemporalInterval interval = TemporalInterval.of(Period.of(2, 3, -4), Duration.ofSeconds(-2, 123456789));
    var encoded = IntervalType.encode(interval);
    assertEquals(interval, IntervalType.decode(encoded));
    assertEquals(interval, IntervalType.decode(encoded));
    assertEquals(0, encoded.position());
    assertEquals(interval, IntervalType.decode(interval));
    assertTrue(IntervalType.matches(IntervalType.schema()));
  }

  /** Formats a lower declared precision without an unchecked rounding failure. */
  @Test
  void formattingTruncatesExcessFractionDigits() {
    TemporalInterval difference = TemporalInterval.between(LocalDateTime.of(2024, 1, 1, 0, 0),
        LocalDateTime.of(2024, 1, 2, 0, 0, 0, 123456789));
    TemporalInterval projected = IntervalType.project(difference,
        IntervalType.schema(IntervalQualifier.DAY_TO_SECOND, 6));
    assertEquals("1 00:00:00.123456", projected.toString());
    assertEquals("-1 00:00:00.123456", projected.negate().toString());
  }

  /** Ensures subtraction provenance and nanoseconds survive materialization. */
  @Test
  void differencesRoundTrip() {
    TemporalInterval difference = TemporalInterval.between(LocalDateTime.of(2024, 1, 1, 0, 0),
        LocalDateTime.of(2024, 1, 2, 0, 0, 0, 123456789));
    TemporalInterval decoded = IntervalType.decode(IntervalType.encode(difference));
    assertEquals(difference, decoded);
    assertTrue(decoded.isTemporalDifference());
    assertEquals("1 00:00:00.123456789", decoded.toString());
    assertEquals("-1 00:00:00.123456789", decoded.negate().toString());
  }

  /** Retains declared fields and precision in the interval codec. */
  @Test
  void declaredFieldsRoundTrip() {
    for (String[] source : new String[][]{
        {
            "90", "MINUTE"
        }, {
            "1.2300", "SECOND"
        }, {
            "0", "MONTH"
        }
    }) {
      TemporalInterval interval = TemporalInterval.parse(source[0], source[1]);
      TemporalInterval decoded = IntervalType.decode(IntervalType.encode(interval),
          IntervalType.schema(interval.qualifier(), interval.fractionalPrecision()));
      assertEquals(interval.qualifier(), decoded.qualifier());
      assertEquals(interval.fractionalPrecision(), decoded.fractionalPrecision());
      assertEquals(interval.toString(), decoded.toString());
    }
  }

  /** Checks normalized signs, day carries, zero, and year/month text. */
  @Test
  void sqlIntervalText() {
    assertEquals("0 00:00:00", TemporalInterval.of(Period.ZERO, Duration.ZERO).toString());
    assertEquals("-0 00:00:00.000000001", TemporalInterval.of(Period.ZERO, Duration.ofNanos(-1)).toString());
    assertEquals("2 02:03:04.5", TemporalInterval
        .of(Period.ofDays(1), Duration.ofHours(26).plusMinutes(3).plusSeconds(4).plusMillis(500)).toString());
    assertEquals("2-03", TemporalInterval.of(Period.ofMonths(27), Duration.ZERO).toString());
    assertEquals("-2-03", TemporalInterval.of(Period.ofMonths(-27), Duration.ZERO).toString());
  }
}
