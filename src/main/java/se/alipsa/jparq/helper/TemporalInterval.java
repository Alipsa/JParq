package se.alipsa.jparq.helper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Represents a SQL style interval with separate {@link Period} and
 * {@link Duration} components.
 *
 * <p>
 * The implementation keeps the calendar based portion (years/months/days) in
 * the {@link Period} and the time based portion (hours/minutes/seconds/nanos)
 * in the {@link Duration}. Instances are immutable.
 * </p>
 */
public final class TemporalInterval implements Comparable<TemporalInterval> {

  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final Map<String, Function<String, TemporalInterval>> UNIT_PARSERS = createUnitParsers();
  private static final Set<String> UNSUPPORTED_UNITS = Set.of("week", "weeks", "quarter", "quarters", "millisecond",
      "milliseconds", "microsecond", "microseconds");
  private static final Pattern DAY_FIELD = Pattern.compile("[0-9]+");

  private final Period period;
  private final Duration duration;
  private final boolean temporalDifference;

  /**
   * Creates an interval, retaining whether temporal subtraction contributed to
   * it.
   *
   * @param period
   *          the calendar component
   * @param duration
   *          the time component
   * @param temporalDifference
   *          whether this value contains a temporal difference
   */
  TemporalInterval(Period period, Duration duration, boolean temporalDifference) {
    this.period = Objects.requireNonNull(period, "period").normalized();
    this.duration = Objects.requireNonNull(duration, "duration");
    this.temporalDifference = temporalDifference;
  }

  /**
   * Create a new interval from a {@link Period} and {@link Duration}.
   *
   * @param period
   *          the calendar component, not {@code null}
   * @param duration
   *          the time component, not {@code null}
   * @return the new interval instance
   */
  public static TemporalInterval of(Period period, Duration duration) {
    return new TemporalInterval(period, duration, false);
  }

  /**
   * Reports whether temporal subtraction contributed to this interval. This
   * provenance does not affect interval equality or ordering.
   *
   * @return true for differences and interval arithmetic involving differences
   */
  public boolean isTemporalDifference() {
    return temporalDifference;
  }

  /**
   * Reports whether a unit or compound qualifier is supported by the parser.
   *
   * @param unit
   *          the unit name without precision decorations, or null
   * @return true if the unit is supported, ignoring case
   */
  public static boolean supportsUnit(String unit) {
    return unit != null && UNIT_PARSERS.containsKey(unit.toLowerCase(Locale.ROOT));
  }

  /**
   * Parses an interval literal. A leading sign on a DAY TO SECOND value applies
   * to the entire duration, including fractional seconds.
   *
   * @param literal
   *          the literal text (possibly quoted), may be {@code null}
   * @param explicitUnit
   *          optional explicit unit (e.g. {@code DAY}) or {@code null}
   * @return the parsed interval (never {@code null})
   * @throws IllegalArgumentException
   *           if the text or unit is invalid
   * @throws ArithmeticException
   *           if an amount cannot be represented
   * @throws java.time.DateTimeException
   *           if a compound time component is invalid
   * @throws UnsupportedOperationException
   *           if a known unit such as WEEK is not implemented
   */
  public static TemporalInterval parse(String literal, String explicitUnit) {
    if ((literal == null || literal.isBlank()) && (explicitUnit == null || explicitUnit.isBlank())) {
      return of(Period.ZERO, Duration.ZERO);
    }

    String value = literal == null ? "" : literal.trim();
    if (value.startsWith("'") && value.endsWith("'") && value.length() >= 2) {
      value = value.substring(1, value.length() - 1);
    }

    if (explicitUnit != null && !explicitUnit.isBlank()) {
      return fromUnit(explicitUnit, value);
    }

    if (value.isBlank()) {
      return of(Period.ZERO, Duration.ZERO);
    }

    String[] tokens = value.split("\\s+");
    if (tokens.length % 2 != 0) {
      throw new IllegalArgumentException("Interval literal must contain value/unit pairs: " + literal);
    }

    TemporalInterval result = of(Period.ZERO, Duration.ZERO);
    for (int i = 0; i < tokens.length; i += 2) {
      String amount = tokens[i];
      String unit = tokens[i + 1];
      result = result.plus(fromUnit(unit, amount));
    }
    return result;
  }

  /**
   * Dispatches a value to the registered parser for its unit or plural alias.
   *
   * @param unitRaw
   *          the unit name
   * @param amountLiteral
   *          the possibly quoted amount
   * @return the parsed interval
   * @throws IllegalArgumentException
   *           if the amount is malformed or the unit is unknown
   * @throws ArithmeticException
   *           if the amount cannot be represented
   * @throws java.time.DateTimeException
   *           if a clock component is invalid
   * @throws UnsupportedOperationException
   *           if the unit is known but not implemented
   */
  private static TemporalInterval fromUnit(String unitRaw, String amountLiteral) {
    String unit = unitRaw.toLowerCase(Locale.ROOT);
    String amt = amountLiteral.trim();
    if (amt.startsWith("'") && amt.endsWith("'")) {
      amt = amt.substring(1, amt.length() - 1);
    }
    if (amt.isBlank()) {
      throw new IllegalArgumentException("Empty interval value for unit " + unitRaw);
    }

    Function<String, TemporalInterval> parser = UNIT_PARSERS.get(unit);
    if (parser != null) {
      return parser.apply(amt);
    }
    if (UNSUPPORTED_UNITS.contains(unit)) {
      throw new UnsupportedOperationException("Unsupported interval unit: " + unitRaw);
    }
    throw new IllegalArgumentException("Unknown interval unit: " + unitRaw);
  }

  /**
   * Builds the shared unit registry, including plural aliases for single fields.
   *
   * @return immutable unit-to-parser mappings
   */
  private static Map<String, Function<String, TemporalInterval>> createUnitParsers() {
    Map<String, Function<String, TemporalInterval>> parsers = new HashMap<>();
    parsers.put("year", amount -> of(Period.ofYears(new BigDecimal(amount).intValueExact()), Duration.ZERO));
    parsers.put("month", amount -> of(Period.ofMonths(new BigDecimal(amount).intValueExact()), Duration.ZERO));
    parsers.put("day", amount -> of(Period.ofDays(new BigDecimal(amount).intValueExact()), Duration.ZERO));
    parsers.put("hour", amount -> of(Period.ZERO, Duration.ofHours(new BigDecimal(amount).longValueExact())));
    parsers.put("minute", amount -> of(Period.ZERO, Duration.ofMinutes(new BigDecimal(amount).longValueExact())));
    parsers.put("second", TemporalInterval::parseSeconds);
    Map.copyOf(parsers).forEach((unit, parser) -> parsers.put(unit + "s", parser));
    parsers.put("day to second", TemporalInterval::parseDayToSecond);
    return Map.copyOf(parsers);
  }

  /**
   * Parses a seconds amount with nanosecond precision.
   *
   * @param amount
   *          the numeric amount
   * @return the time interval
   * @throws IllegalArgumentException
   *           if the amount is malformed
   * @throws ArithmeticException
   *           if the amount cannot be represented
   */
  private static TemporalInterval parseSeconds(String amount) {
    long nanosTotal = new BigDecimal(amount).movePointRight(9).setScale(0, RoundingMode.UNNECESSARY).longValueExact();
    long seconds = Math.floorDiv(nanosTotal, 1_000_000_000L);
    long nanos = Math.floorMod(nanosTotal, 1_000_000_000L);
    return of(Period.ZERO, Duration.ofSeconds(seconds, nanos));
  }

  /**
   * Parses an optional sign, digit-only day field, and clock component.
   *
   * @param amount
   *          the compound amount
   * @return the signed duration interval
   * @throws IllegalArgumentException
   *           if the day field or format is invalid
   * @throws ArithmeticException
   *           if the duration cannot be represented
   * @throws java.time.DateTimeException
   *           if the clock component is invalid
   */
  private static TemporalInterval parseDayToSecond(String amount) {
    boolean negative = amount.startsWith("-");
    String magnitude = negative || amount.startsWith("+") ? amount.substring(1) : amount;
    String[] parts = magnitude.split("\\s+");
    if (parts.length != 2 || !DAY_FIELD.matcher(parts[0]).matches()) {
      throw new IllegalArgumentException("Invalid DAY TO SECOND interval literal: " + amount);
    }
    long days;
    try {
      days = Long.parseLong(parts[0]);
    } catch (NumberFormatException failure) {
      ArithmeticException overflow = new ArithmeticException("DAY TO SECOND day value out of range");
      overflow.initCause(failure);
      throw overflow;
    }
    LocalTime timePart = LocalTime.parse(parts[1]);
    Duration duration = Duration.ofDays(days).plusHours(timePart.getHour()).plusMinutes(timePart.getMinute())
        .plusSeconds(timePart.getSecond()).plusNanos(timePart.getNano());
    return of(Period.ZERO, negative ? duration.negated() : duration);
  }

  /**
   * Add another interval.
   *
   * @param other
   *          the interval to add
   * @return the combined interval
   */
  public TemporalInterval plus(TemporalInterval other) {
    return new TemporalInterval(period.plus(other.period), duration.plus(other.duration),
        temporalDifference || other.temporalDifference);
  }

  /**
   * Subtract another interval.
   *
   * @param other
   *          the interval to subtract
   * @return the resulting interval
   */
  public TemporalInterval minus(TemporalInterval other) {
    return new TemporalInterval(period.minus(other.period), duration.minus(other.duration),
        temporalDifference || other.temporalDifference);
  }

  /**
   * Negate this interval.
   *
   * @return the negated interval
   */
  public TemporalInterval negate() {
    return new TemporalInterval(period.negated(), duration.negated(), temporalDifference);
  }

  /**
   * The calendar component of the interval.
   *
   * @return the calendar component of the interval.
   */
  public Period period() {
    return period;
  }

  /**
   * The time component of the interval.
   *
   * @return the time component of the interval.
   */
  public Duration duration() {
    return duration;
  }

  /**
   * Determine whether the interval has a time component that is not a whole
   * number of days.
   *
   * @return {@code true} if a sub-day component exists
   */
  public boolean hasTimeComponent() {
    Duration remainder = duration.minus(Duration.ofDays(duration.toDays()));
    return !remainder.isZero();
  }

  /**
   * Convert the interval to a {@link Duration} using a fixed calendar anchor (UTC
   * epoch). This enables comparisons between intervals that include calendar and
   * time portions.
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
   * @param start
   *          the starting temporal value
   * @param end
   *          the ending temporal value
   * @return the resulting interval, marked as an internal temporal difference
   */
  public static TemporalInterval between(java.time.temporal.TemporalAccessor start,
      java.time.temporal.TemporalAccessor end) {
    Objects.requireNonNull(start, "start");
    Objects.requireNonNull(end, "end");

    if (start instanceof LocalDate sDate && end instanceof LocalDate eDate) {
      Period diff = Period.between(sDate, eDate);
      return new TemporalInterval(diff, Duration.ZERO, true);
    }

    if (start instanceof LocalTime sTime && end instanceof LocalTime eTime) {
      Duration diff = Duration.between(sTime, eTime);
      return new TemporalInterval(Period.ZERO, diff, true);
    }

    LocalDateTime startDt = toDateTime(start);
    LocalDateTime endDt = toDateTime(end);
    if (startDt != null && endDt != null) {
      Duration diff = Duration.between(startDt, endDt);
      return new TemporalInterval(Period.ZERO, diff, true);
    }

    throw new IllegalArgumentException("Unsupported temporal types: " + start.getClass() + " and " + end.getClass());
  }

  private static LocalDateTime toDateTime(java.time.temporal.TemporalAccessor accessor) {
    if (accessor instanceof LocalDateTime ldt) {
      return ldt;
    }
    if (accessor instanceof LocalDate ld) {
      return ld.atStartOfDay();
    }
    if (accessor instanceof Instant instant) {
      return LocalDateTime.ofInstant(instant, UTC);
    }
    if (accessor instanceof java.sql.Timestamp ts) {
      return ts.toLocalDateTime();
    }
    if (accessor instanceof java.sql.Date date) {
      return date.toLocalDate().atStartOfDay();
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

  /**
   * Returns signed SQL interval text: YEAR TO MONTH or DAY TO SECOND, retaining
   * nanoseconds without rounding. Legacy mixed-family values include both parts.
   *
   * @return the interval value without the INTERVAL keyword or qualifier
   */
  @Override
  public String toString() {
    long months = period.toTotalMonths();
    if (months != 0 && period.getDays() == 0 && duration.isZero()) {
      return yearMonthText(months);
    }
    BigDecimal seconds = BigDecimal.valueOf(period.getDays()).multiply(BigDecimal.valueOf(86400))
        .add(BigDecimal.valueOf(duration.getSeconds())).add(BigDecimal.valueOf(duration.getNano(), 9));
    String sign = seconds.signum() < 0 ? "-" : "+";
    BigDecimal magnitude = seconds.abs();
    BigInteger[] parts = magnitude.toBigInteger().divideAndRemainder(BigInteger.valueOf(86400));
    long clock = parts[1].longValueExact();
    String fraction = magnitude.remainder(BigDecimal.ONE).stripTrailingZeros().toPlainString();
    fraction = "0".equals(fraction) ? "" : fraction.substring(1);
    String dayTime = String.format(Locale.ROOT, "%s%s %02d:%02d:%02d%s", sign, parts[0], clock / 3600, clock / 60 % 60,
        clock % 60, fraction);
    return months == 0 ? dayTime : yearMonthText(months) + " " + dayTime;
  }
  /**
   * Formats normalized year/month fields with a single leading sign.
   *
   * @param months
   *          signed total months
   * @return YEAR TO MONTH literal text
   */
  private static String yearMonthText(long months) {
    long magnitude = Math.abs(months);
    return String.format(Locale.ROOT, "%s%d-%02d", months < 0 ? "-" : "+", magnitude / 12, magnitude % 12);
  }

}
