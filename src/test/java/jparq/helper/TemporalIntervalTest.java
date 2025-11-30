package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.TemporalInterval;

/** Tests for {@link TemporalInterval}. */
class TemporalIntervalTest {

  @Test
  void parsesIntervalLiterals() {
    TemporalInterval years = TemporalInterval.parse("'2'", "YEAR");
    assertEquals(Period.ofYears(2), years.period());
    assertEquals(Duration.ZERO, years.duration());

    TemporalInterval months = TemporalInterval.parse("5", "MONTH");
    assertEquals(Period.ofMonths(5), months.period());
    assertEquals(Duration.ZERO, months.duration());

    TemporalInterval dayToSecond = TemporalInterval.parse("2 01:30:00", "DAY TO SECOND");
    assertEquals(Period.ZERO, dayToSecond.period());
    assertEquals(Duration.ofDays(2).plusHours(1).plusMinutes(30), dayToSecond.duration());
  }

  @Test
  void validatesIntervalLiterals() {
    assertThrows(IllegalArgumentException.class, () -> TemporalInterval.parse("10 extra", null));
    assertThrows(IllegalArgumentException.class, () -> TemporalInterval.parse("1", "DAY TO SECOND"));
    assertThrows(IllegalArgumentException.class, () -> TemporalInterval.parse("5", "LIGHTYEAR"));
  }

  @Test
  void supportsArithmeticAndComparison() {
    TemporalInterval left = TemporalInterval.parse("1", "DAY");
    TemporalInterval right = TemporalInterval.parse("12", "HOUR");

    TemporalInterval sum = left.plus(right);
    assertEquals(Period.ofDays(1), sum.period());
    assertEquals(Duration.ofHours(12), sum.duration());

    TemporalInterval difference = left.minus(right);
    assertEquals(Period.ofDays(1), difference.period());
    assertEquals(Duration.ofHours(-12), difference.duration());

    assertEquals(difference.negate(), right.minus(left));
    assertTrue(sum.hasTimeComponent());
    assertFalse(left.hasTimeComponent());
    assertTrue(sum.compareTo(right) > 0);
  }

  @Test
  void convertsBetweenTemporalTypes() {
    TemporalInterval fromDates = TemporalInterval.between(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 1, 3));
    assertEquals(Period.ofDays(2), fromDates.period());
    assertEquals(Duration.ZERO, fromDates.duration());

    TemporalInterval fromTimes = TemporalInterval.between(LocalTime.of(10, 0), LocalTime.of(11, 15));
    assertEquals(Period.ZERO, fromTimes.period());
    assertEquals(Duration.ofMinutes(75), fromTimes.duration());

    Instant start = LocalDateTime.of(2020, 1, 1, 0, 0).toInstant(java.time.ZoneOffset.UTC);
    Instant end = start.plusSeconds(90);
    TemporalInterval fromInstants = TemporalInterval.between(start, end);
    assertEquals(Duration.ofSeconds(90), fromInstants.duration());
    assertEquals(Period.ZERO, fromInstants.period());
  }
}
