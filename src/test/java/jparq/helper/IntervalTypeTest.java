package jparq.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import org.junit.jupiter.api.Test;
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

  /** Ensures subtraction provenance and nanoseconds survive materialization. */
  @Test
  void differencesRoundTrip() {
    TemporalInterval difference = TemporalInterval.between(LocalDateTime.of(2024, 1, 1, 0, 0),
        LocalDateTime.of(2024, 1, 2, 0, 0, 0, 123456789));
    TemporalInterval decoded = IntervalType.decode(IntervalType.encode(difference));
    assertEquals(difference, decoded);
    assertTrue(decoded.isTemporalDifference());
    assertEquals("+1 00:00:00.123456789", decoded.toString());
    assertEquals("-1 00:00:00.123456789", decoded.negate().toString());
  }

  /** Checks normalized signs, day carries, zero, and year/month text. */
  @Test
  void sqlIntervalText() {
    assertEquals("+0 00:00:00", TemporalInterval.of(Period.ZERO, Duration.ZERO).toString());
    assertEquals("-0 00:00:00.000000001", TemporalInterval.of(Period.ZERO, Duration.ofNanos(-1)).toString());
    assertEquals("+2 02:03:04.5", TemporalInterval
        .of(Period.ofDays(1), Duration.ofHours(26).plusMinutes(3).plusSeconds(4).plusMillis(500)).toString());
    assertEquals("+2-03", TemporalInterval.of(Period.ofMonths(27), Duration.ZERO).toString());
    assertEquals("-2-03", TemporalInterval.of(Period.ofMonths(-27), Duration.ZERO).toString());
  }
}
