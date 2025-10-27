package se.alipsa.jparq.helper;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents a SQL style interval with separate {@link Period} and {@link Duration} components.
 *
 * <p>
 * The implementation keeps the calendar based portion (years/months/days) in the {@link Period}
 * and the time based portion (hours/minutes/seconds/nanos) in the {@link Duration}. Instances are immutable.
 * </p>
 */
public final class TemporalInterval implements Comparable<TemporalInterval> {

  private static final ZoneId UTC = ZoneId.of("UTC");

  private final Period period;
  private final Duration duration;

  private TemporalInterval(Period period, Duration duration) {
    this.period = Objects.requireNonNull(period, "period").normalized();
    this.duration = Objects.requireNonNull(duration, "duration");
  }

  /**
   * Create a new interval from a {@link Period} and {@link Duration}.
   *
   * @param period the calendar component, not {@code null}
   * @param duration the time component, not {@code null}
   * @return the new interval instance
   */
  public static TemporalInterval of(Period period, Duration duration) {
    return new TemporalInterval(period, duration);
  }

  /**
   * Parse an interval literal.
   *
   * @param literal      the literal text (possibly quoted), may be {@code null}
   * @param explicitUnit optional explicit unit (e.g. {@code DAY}) or {@code null}
   * @return the parsed interval (never {@code null})
   */
  public static TemporalInterval parse(String literal, String explicitUnit) {
    if ((literal == null || literal.isBlank()) && (explicitUnit == null || explicitUnit.isBlank())) {
      return TemporalInterval.of(Period.ZERO, Duration.ZERO);
    }

    String value = literal == null ? "" : literal.trim();
    if (value.startsWith("'") && value.endsWith("'") && value.length() >= 2) {
      value = value.substring(1, value.length() - 1);
    }

    if (explicitUnit != null && !explicitUnit.isBlank()) {
      return fromUnit(explicitUnit, value);
    }

    if (value.isBlank()) {
      return TemporalInterval.of(Period.ZERO, Duration.ZERO);
    }

    String[] tokens = value.split("\\s+");
    if (tokens.length % 2 != 0) {
      throw new IllegalArgumentException("Interval literal must contain value/unit pairs: " + literal);
    }

    TemporalInterval result = TemporalInterval.of(Period.ZERO, Duration.ZERO);
    for (int i = 0; i < tokens.length; i += 2) {
      String amount = tokens[i];
      String unit = tokens[i + 1];
      result = result.plus(fromUnit(unit, amount));
    }
    return result;
  }

  private static TemporalInterval fromUnit(String unitRaw, String amountLiteral) {
    String unit = unitRaw.toLowerCase(Locale.ROOT);
    String amt = amountLiteral.trim();
    if (amt.startsWith("'") && amt.endsWith("'")) {
      amt = amt.substring(1, amt.length() - 1);
    }
    if (amt.isBlank()) {
      throw new IllegalArgumentException("Empty interval value for unit " + unitRaw);
    }

    switch (unit) {
      case "year", "years": {
        long years = new java.math.BigDecimal(amt).longValueExact();
        return TemporalInterval.of(Period.ofYears(Math.toIntExact(years)), Duration.ZERO);
      }
      case "month", "months": {
        long months = new java.math.BigDecimal(amt).longValueExact();
        return TemporalInterval.of(Period.ofMonths(Math.toIntExact(months)), Duration.ZERO);
      }
      case "day", "days": {
        long days = new java.math.BigDecimal(amt).longValueExact();
        return TemporalInterval.of(Period.ofDays(Math.toIntExact(days)), Duration.ZERO);
      }
      case "hour", "hours": {
        long hours = new java.math.BigDecimal(amt).longValueExact();
        return TemporalInterval.of(Period.ZERO, Duration.ofHours(hours));
      }
      case "minute", "minutes": {
        long minutes = new java.math.BigDecimal(amt).longValueExact();
        return TemporalInterval.of(Period.ZERO, Duration.ofMinutes(minutes));
      }
      case "second", "seconds": {
        java.math.BigDecimal bd = new java.math.BigDecimal(amt);
        long nanosTotal = bd.movePointRight(9).setScale(0, java.math.RoundingMode.UNNECESSARY).longValueExact();
        long seconds = Math.floorDiv(nanosTotal, 1_000_000_000L);
        long nanos = Math.floorMod(nanosTotal, 1_000_000_000L);
        return TemporalInterval.of(Period.ZERO, Duration.ofSeconds(seconds, nanos));
      }
      default:
        throw new IllegalArgumentException("Unsupported interval unit: " + unitRaw);
    }
  }

  /**
   * Add another interval.
   *
   * @param other the interval to add
   * @return the combined interval
   */
  public TemporalInterval plus(TemporalInterval other) {
    return TemporalInterval.of(period.plus(other.period), duration.plus(other.duration));
  }

  /**
   * Subtract another interval.
   *
   * @param other the interval to subtract
   * @return the resulting interval
   */
  public TemporalInterval minus(TemporalInterval other) {
    return TemporalInterval.of(period.minus(other.period), duration.minus(other.duration));
  }

  /**
   * Negate this interval.
   *
   * @return the negated interval
   */
  public TemporalInterval negate() {
    return TemporalInterval.of(period.negated(), duration.negated());
  }

  /**
   * @return the calendar component of the interval.
   */
  public Period period() {
    return period;
  }

  /**
   * @return the time component of the interval.
   */
  public Duration duration() {
    return duration;
  }

  /**
   * Determine whether the interval has a time component that is not a whole number of days.
   *
   * @return {@code true} if a sub-day component exists
   */
  public boolean hasTimeComponent() {
    Duration remainder = duration.minus(Duration.ofDays(duration.toDays()));
    return !remainder.isZero();
  }

  /**
   * Convert the interval to a {@link Duration} using a fixed calendar anchor (UTC epoch).
   * This enables comparisons between intervals that include calendar and time portions.
   *
   * @return the derived duration
   */
  public Duration toDuration() {
    LocalDateTime base = LocalDate.ofEpochDay(0).atStartOfDay();
    LocalDateTime end = base.plus(period).plus(duration);
    return Duration.between(base.atZone(UTC), end.atZone(UTC));
  }

  @Override
  public int compareTo(TemporalInterval other) {
    return this.toDuration().compareTo(other.toDuration());
  }

  /**
   * Compute an interval representing {@code end - start}.
   *
   * @param start the starting temporal value
   * @param end   the ending temporal value
   * @return the resulting interval
   */
  public static TemporalInterval between(java.time.temporal.TemporalAccessor start,
      java.time.temporal.TemporalAccessor end) {
    Objects.requireNonNull(start, "start");
    Objects.requireNonNull(end, "end");

    LocalDateTime startDt = toDateTime(start);
    LocalDateTime endDt = toDateTime(end);
    if (startDt != null && endDt != null) {
      Duration diff = Duration.between(startDt, endDt);
      return TemporalInterval.of(Period.ZERO, diff);
    }

    if (start instanceof java.time.LocalDate sDate && end instanceof java.time.LocalDate eDate) {
      Period diff = Period.between(sDate, eDate);
      return TemporalInterval.of(diff, Duration.ZERO);
    }

    if (start instanceof java.time.LocalTime sTime && end instanceof java.time.LocalTime eTime) {
      Duration diff = Duration.between(sTime, eTime);
      return TemporalInterval.of(Period.ZERO, diff);
    }

    throw new IllegalArgumentException("Unsupported temporal types: " + start.getClass() + " and " + end.getClass());
  }

  private static LocalDateTime toDateTime(java.time.temporal.TemporalAccessor accessor) {
    if (accessor instanceof LocalDateTime ldt) {
      return ldt;
    }
    if (accessor instanceof java.time.LocalDate ld) {
      return ld.atStartOfDay();
    }
    if (accessor instanceof java.time.LocalTime) {
      return null;
    }
    if (accessor instanceof java.time.Instant instant) {
      return LocalDateTime.ofInstant(instant, UTC);
    }
    if (accessor instanceof java.sql.Timestamp ts) {
      return ts.toLocalDateTime();
    }
    if (accessor instanceof java.sql.Date date) {
      return date.toLocalDate().atStartOfDay();
    }
    if (accessor instanceof java.sql.Time time) {
      return null;
    }
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TemporalInterval other)) {
      return false;
    }
    return period.equals(other.period) && duration.equals(other.duration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(period, duration);
  }

  @Override
  public String toString() {
    return "TemporalInterval[period=" + period + ", duration=" + duration + "]";
  }
}
