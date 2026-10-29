package se.alipsa.jparq.engine.function;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import se.alipsa.jparq.engine.DecimalCasts;
import se.alipsa.jparq.engine.NumericCastTarget;
import se.alipsa.jparq.engine.NumericCasts;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Utility methods for evaluating SQL date/time functions and literal casts.
 */
public final class DateTimeFunctions {

  private static final DateTimeFormatter SQL_DATE = DateTimeFormatter.ofPattern("uuuu-M-d", Locale.ROOT)
      .withResolverStyle(ResolverStyle.STRICT);
  private static final DateTimeFormatter SQL_TIME = new DateTimeFormatterBuilder().appendPattern("H:m[:s]")
      .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd().toFormatter(Locale.ROOT)
      .withResolverStyle(ResolverStyle.STRICT);
  private static final DateTimeFormatter SQL_TIMESTAMP = new DateTimeFormatterBuilder().append(SQL_DATE)
      .appendLiteral(' ').append(SQL_TIME).toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);

  private DateTimeFunctions() {
  }

  /**
   * Evaluate a {@link TimeKeyExpression} such as {@code CURRENT_TIMESTAMP}.
   *
   * @param expr
   *          the time key expression
   * @return the evaluated value (Date/Time/Timestamp) or the literal string if
   *         unsupported
   */
  public static Object evaluateTimeKey(TimeKeyExpression expr) {
    if (expr == null) {
      return null;
    }
    String key = expr.getStringValue();
    if (key == null) {
      return null;
    }
    ZoneId zone = defaultZone();
    return switch (key.toUpperCase(Locale.ROOT)) {
      case "CURRENT_DATE", "CURRENT_DATE()" -> Date.valueOf(LocalDate.now(zone));
      case "CURRENT_TIME" -> OffsetTime.now(zone);
      case "LOCALTIME" -> LocalTime.now(zone);
      case "CURRENT_TIMESTAMP" -> OffsetDateTime.now(zone);
      case "LOCALTIMESTAMP" -> LocalDateTime.now(zone);
      default -> key;
    };
  }

  /**
   * Resolve the time zone to use for time-key evaluations. Prefers the
   * {@code user.timezone} system property when present to honor test isolation
   * and runtime overrides, otherwise falls back to the JVM default zone.
   *
   * @return the active {@link ZoneId} to use
   */
  private static ZoneId defaultZone() {
    String userZone = System.getProperty("user.timezone");
    if (userZone != null && !userZone.isBlank()) {
      try {
        return ZoneId.of(userZone);
      } catch (Exception ignored) {
        // fall through to system default if the provided zone is invalid
      }
    }
    return ZoneId.systemDefault();
  }

  /**
   * Return the JDBC-style day-of-week number for the supplied date.
   *
   * @param value
   *          a date-like value
   * @return 1 for Sunday through 7 for Saturday, or {@code null} when the input
   *         is not a date
   */
  public static Integer dayOfWeek(Object value) {
    LocalDate date = toLocalDate(value);
    if (date == null) {
      return null;
    }
    // JDBC canonical: 1=Sunday..7=Saturday
    int dow = date.getDayOfWeek().getValue(); // 1=Monday..7=Sunday
    int sundayBased = dow == 7 ? 1 : dow + 1;
    return sundayBased;
  }

  /**
   * Extract the day of month from the supplied date.
   *
   * @param value
   *          a date-like value
   * @return day of month or {@code null} when the input is not a date
   */
  public static Integer dayOfMonth(Object value) {
    LocalDate date = toLocalDate(value);
    return date == null ? null : date.getDayOfMonth();
  }

  /**
   * Extract the ordinal day of year from the supplied date.
   *
   * @param value
   *          a date-like value
   * @return day of year or {@code null} when the input is not a date
   */
  public static Integer dayOfYear(Object value) {
    LocalDate date = toLocalDate(value);
    return date == null ? null : date.getDayOfYear();
  }

  /**
   * Extract the hour component from a time value.
   *
   * @param value
   *          a time-like value
   * @return hour value or {@code null} when the input is not a time
   */
  public static Integer hour(Object value) {
    LocalTime time = toLocalTime(value);
    return time == null ? null : time.getHour();
  }

  /**
   * Extract the minute component from a time value.
   *
   * @param value
   *          a time-like value
   * @return minute value or {@code null} when the input is not a time
   */
  public static Integer minute(Object value) {
    LocalTime time = toLocalTime(value);
    return time == null ? null : time.getMinute();
  }

  /**
   * Extract the second component from a time value.
   *
   * @param value
   *          a time-like value
   * @return second value or {@code null} when the input is not a time
   */
  public static Integer second(Object value) {
    LocalTime time = toLocalTime(value);
    return time == null ? null : time.getSecond();
  }

  /**
   * Extract the numeric month from a date.
   *
   * @param value
   *          a date-like value
   * @return month value or {@code null} when the input is not a date
   */
  public static Integer month(Object value) {
    LocalDate date = toLocalDate(value);
    return date == null ? null : date.getMonthValue();
  }

  /**
   * Extract the four-digit year from a date.
   *
   * @param value
   *          a date-like value
   * @return year value or {@code null} when the input is not a date
   */
  public static Integer year(Object value) {
    LocalDate date = toLocalDate(value);
    return date == null ? null : date.getYear();
  }

  /**
   * Calculate the quarter of the year (1-4) for the supplied date.
   *
   * @param value
   *          a date-like value
   * @return quarter number or {@code null} when the input is not a date
   */
  public static Integer quarter(Object value) {
    Integer month = month(value);
    if (month == null) {
      return null;
    }
    return (month - 1) / 3 + 1;
  }

  /**
   * Calculate the ISO week number for the supplied date.
   *
   * @param value
   *          a date-like value
   * @return ISO week number or {@code null} when the input is not a date
   */
  public static Integer week(Object value) {
    LocalDate date = toLocalDate(value);
    if (date == null) {
      return null;
    }
    return date.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
  }

  /**
   * JDBC TIMESTAMPADD implementation supporting SQL_TSI units, with
   * SQL_TSI_FRAC_SECOND measured in nanoseconds.
   *
   * @param args
   *          arguments: interval unit, interval value, timestamp
   * @return adjusted temporal or {@code null} on invalid input
   * @throws IllegalArgumentException
   *           if the amount is invalid or numeric arithmetic overflows (22003)
   */
  public static Object timestampAdd(List<Object> args) {
    if (args == null || args.size() < 3) {
      return null;
    }
    String unitToken = args.get(0) == null ? null : args.get(0).toString();
    if (unitToken == null) {
      return null;
    }
    ChronoUnit unit = toChronoUnit(unitToken);
    boolean quarter = isQuarter(unitToken);
    Number amount = NumericCasts.convert(args.get(1), NumericCastTarget.BIGINT, "BIGINT");
    if (amount == null) {
      return null;
    }
    Object target = args.get(2);
    try {
      if (quarter) {
        return applyAdd(target, Math.multiplyExact(amount.longValue(), 3L), ChronoUnit.MONTHS);
      }
      if (unit == null) {
        return null;
      }
      return applyAdd(target, amount.longValue(), unit);
    } catch (ArithmeticException exception) {
      throw timestampOverflow("TIMESTAMPADD", exception);
    }
  }

  /**
   * JDBC TIMESTAMPDIFF implementation supporting SQL_TSI units, with
   * SQL_TSI_FRAC_SECOND measured in nanoseconds.
   *
   * @param args
   *          arguments: interval unit, start timestamp, end timestamp
   * @return difference in requested unit or {@code null} on invalid input
   * @throws IllegalArgumentException
   *           if the difference overflows a signed long (22003)
   */
  public static Long timestampDiff(List<Object> args) {
    if (args == null || args.size() < 3) {
      return null;
    }
    String unitToken = args.get(0) == null ? null : args.get(0).toString();
    if (unitToken == null) {
      return null;
    }
    ChronoUnit unit = toChronoUnit(unitToken);
    boolean quarter = isQuarter(unitToken);
    java.time.temporal.Temporal start = toTemporal(args.get(1));
    java.time.temporal.Temporal end = toTemporal(args.get(2));
    if (start == null || end == null) {
      return null;
    }
    try {
      if (quarter) {
        long months = ChronoUnit.MONTHS.between(start, end);
        return months / 3;
      }
      if (unit == null) {
        return null;
      }
      return unit.between(start, end);
    } catch (ArithmeticException exception) {
      throw timestampOverflow("TIMESTAMPDIFF", exception);
    }
  }

  /**
   * Parse a {@link IntervalExpression}.
   *
   * @param expr
   *          the interval expression
   * @return the parsed interval
   */
  public static TemporalInterval toInterval(IntervalExpression expr) {
    if (expr == null) {
      return TemporalInterval.of(Period.ZERO, Duration.ZERO);
    }
    return TemporalInterval.parse(expr.getParameter(), expr.getIntervalType());
  }

  /**
   * Perform EXTRACT(field FROM source).
   *
   * @param field
   *          the requested field
   * @param source
   *          the value to extract from
   * @return the extracted component or {@code null}
   */
  public static Object extract(String field, Object source) {
    if (field == null || source == null) {
      return null;
    }
    String upper = field.toUpperCase(Locale.ROOT);
    if (source instanceof Timestamp ts) {
      return extractFromLocalDateTime(upper, ts.toLocalDateTime());
    }
    if (source instanceof Date d) {
      return extractFromLocalDateTime(upper, d.toLocalDate().atStartOfDay());
    }
    if (source instanceof Time t) {
      return extractFromLocalTime(upper, t.toLocalTime());
    }
    if (source instanceof LocalDateTime ldt) {
      return extractFromLocalDateTime(upper, ldt);
    }
    if (source instanceof LocalDate ld) {
      return extractFromLocalDateTime(upper, ld.atStartOfDay());
    }
    if (source instanceof LocalTime lt) {
      return extractFromLocalTime(upper, lt);
    }
    if (source instanceof OffsetDateTime odt) {
      return extractFromLocalDateTime(upper, odt.toLocalDateTime());
    }
    if (source instanceof TemporalInterval interval) {
      return switch (upper) {
        case "DAY" -> interval.period().getDays() + interval.duration().toDays();
        case "HOUR" -> interval.duration().toHours();
        case "MINUTE" -> interval.duration().toMinutes();
        case "SECOND" -> interval.duration().toSeconds();
        default -> null;
      };
    }
    return null;
  }

  private static Object extractFromLocalDateTime(String field, LocalDateTime dateTime) {
    return switch (field) {
      case "YEAR" -> dateTime.getYear();
      case "MONTH" -> dateTime.getMonthValue();
      case "DAY" -> dateTime.getDayOfMonth();
      case "HOUR" -> dateTime.getHour();
      case "MINUTE" -> dateTime.getMinute();
      case "SECOND" -> dateTime.getSecond();
      default -> null;
    };
  }

  private static Object extractFromLocalTime(String field, LocalTime time) {
    return switch (field) {
      case "HOUR" -> time.getHour();
      case "MINUTE" -> time.getMinute();
      case "SECOND" -> time.getSecond();
      default -> null;
    };
  }

  /**
   * Handle addition between date/time values and intervals.
   *
   * @param left
   *          the left operand
   * @param right
   *          the right operand
   * @return the resulting value or {@code null} if no temporal addition applies
   */
  public static Object plus(Object left, Object right) {
    if (left instanceof TemporalInterval li && right instanceof TemporalInterval ri) {
      return li.plus(ri);
    }
    if (left instanceof TemporalInterval li) {
      return applyInterval(right, li);
    }
    if (right instanceof TemporalInterval ri) {
      return applyInterval(left, ri);
    }
    return null;
  }

  /**
   * Handle subtraction for temporal values.
   *
   * @param left
   *          the left operand
   * @param right
   *          the right operand
   * @return the signed day count as a {@link Long} for two DATE values, an
   *         interval for other temporal differences, or {@code null} if not a
   *         temporal subtraction
   */
  public static Object minus(Object left, Object right) {
    if (left instanceof TemporalInterval li && right instanceof TemporalInterval ri) {
      return li.minus(ri);
    }
    if (right instanceof TemporalInterval ri) {
      return applyInterval(left, ri.negate());
    }
    if (isTemporal(left) && isTemporal(right)) {
      TemporalAccessor start = toTemporalAccessor(right);
      TemporalAccessor end = toTemporalAccessor(left);
      if (start instanceof LocalDate startDate && end instanceof LocalDate endDate) {
        return ChronoUnit.DAYS.between(startDate, endDate);
      }
      if (start != null && end != null) {
        return TemporalInterval.between(start, end);
      }
    }
    return null;
  }

  /**
   * Determines whether a value is a supported date or time operand.
   *
   * @param value
   *          the operand to inspect
   * @return whether the value is a supported temporal type
   */
  public static boolean isTemporal(Object value) {
    return value instanceof Date || value instanceof Time || value instanceof Timestamp || value instanceof LocalDate
        || value instanceof LocalDateTime || value instanceof LocalTime || value instanceof OffsetDateTime
        || value instanceof OffsetTime;
  }

  private static TemporalAccessor toTemporalAccessor(Object value) {
    if (value instanceof Timestamp ts) {
      return ts.toLocalDateTime();
    }
    if (value instanceof Date d) {
      return d.toLocalDate();
    }
    if (value instanceof Time t) {
      return t.toLocalTime();
    }
    if (value instanceof LocalDateTime ldt) {
      return ldt;
    }
    if (value instanceof LocalDate ld) {
      return ld;
    }
    if (value instanceof LocalTime lt) {
      return lt;
    }
    if (value instanceof OffsetTime ot) {
      return ot.toLocalTime();
    }
    if (value instanceof OffsetDateTime odt) {
      return odt.toLocalDateTime();
    }
    return null;
  }

  private static LocalDate toLocalDate(Object value) {
    TemporalAccessor ta = toTemporalAccessor(value);
    if (ta instanceof LocalDate ld) {
      return ld;
    }
    if (ta instanceof LocalDateTime ldt) {
      return ldt.toLocalDate();
    }
    if (ta instanceof OffsetDateTime odt) {
      return odt.toLocalDate();
    }
    return null;
  }

  private static LocalTime toLocalTime(Object value) {
    TemporalAccessor ta = toTemporalAccessor(value);
    if (ta instanceof LocalTime lt) {
      return lt;
    }
    if (ta instanceof LocalDateTime ldt) {
      return ldt.toLocalTime();
    }
    if (ta instanceof OffsetTime ot) {
      return ot.toLocalTime();
    }
    if (ta instanceof OffsetDateTime odt) {
      return odt.toLocalTime();
    }
    return null;
  }

  /**
   * Creates a classified failure for timestamp interval arithmetic overflow.
   *
   * @param function
   *          timestamp function name
   * @param cause
   *          arithmetic overflow
   * @return exception carrying SQLState 22003
   */
  private static IllegalArgumentException timestampOverflow(String function, ArithmeticException cause) {
    String message = function + ": numeric value out of range";
    return new IllegalArgumentException(message, new SQLDataException(message, "22003", cause));
  }

  /**
   * Adds a signed interval without narrowing its amount.
   *
   * @param target
   *          temporal input
   * @param amount
   *          interval count
   * @param unit
   *          interval unit
   * @return adjusted temporal, or null for an unsupported input type
   * @throws ArithmeticException
   *           if interval arithmetic overflows
   * @throws DateTimeException
   *           if the result exceeds the temporal range or the unit is unsupported
   */
  private static Object applyAdd(Object target, long amount, ChronoUnit unit) {
    if (target instanceof OffsetDateTime odt) {
      return odt.plus(amount, unit);
    }
    if (target instanceof LocalDateTime ldt) {
      return ldt.plus(amount, unit);
    }
    if (target instanceof Timestamp ts) {
      LocalDateTime adjusted = ts.toLocalDateTime().plus(amount, unit);
      return Timestamp.valueOf(adjusted);
    }
    if (target instanceof Date d) {
      LocalDate adjusted = d.toLocalDate().plus(amount, unit);
      return Date.valueOf(adjusted);
    }
    if (target instanceof LocalDate ld) {
      return ld.plus(amount, unit);
    }
    if (target instanceof LocalTime lt) {
      return lt.plus(amount, unit);
    }
    if (target instanceof Time t) {
      LocalTime adjusted = t.toLocalTime().plus(amount, unit);
      return Time.valueOf(adjusted);
    }
    if (target instanceof OffsetTime ot) {
      return ot.plus(amount, unit);
    }
    return null;
  }

  private static ChronoUnit toChronoUnit(String token) {
    String normalized = token.replace("'", "").trim().toUpperCase(Locale.ROOT);
    return switch (normalized) {
      case "SQL_TSI_FRAC_SECOND" -> ChronoUnit.NANOS;
      case "SQL_TSI_SECOND", "SECOND" -> ChronoUnit.SECONDS;
      case "SQL_TSI_MINUTE", "MINUTE" -> ChronoUnit.MINUTES;
      case "SQL_TSI_HOUR", "HOUR" -> ChronoUnit.HOURS;
      case "SQL_TSI_DAY", "DAYOFMONTH", "DAYOFYEAR", "DAY" -> ChronoUnit.DAYS;
      case "SQL_TSI_WEEK", "WEEK" -> ChronoUnit.WEEKS;
      case "SQL_TSI_MONTH", "MONTH" -> ChronoUnit.MONTHS;
      case "SQL_TSI_YEAR", "YEAR" -> ChronoUnit.YEARS;
      default -> null;
    };
  }

  private static boolean isQuarter(String token) {
    String normalized = token.replace("'", "").trim().toUpperCase(Locale.ROOT);
    return "SQL_TSI_QUARTER".equals(normalized) || "QUARTER".equals(normalized);
  }

  private static java.time.temporal.Temporal toTemporal(Object value) {
    TemporalAccessor ta = toTemporalAccessor(value);
    if (ta instanceof java.time.temporal.Temporal temporal) {
      return temporal;
    }
    return null;
  }

  private static Object applyInterval(Object temporal, TemporalInterval interval) {
    Objects.requireNonNull(interval, "interval");
    if (temporal instanceof Timestamp ts) {
      LocalDateTime adjusted = ts.toLocalDateTime().plus(interval.period()).plus(interval.duration());
      return Timestamp.valueOf(adjusted);
    }
    if (temporal instanceof Date d) {
      LocalDateTime adjusted = d.toLocalDate().atStartOfDay().plus(interval.period()).plus(interval.duration());
      if (interval.hasTimeComponent()) {
        return Timestamp.valueOf(adjusted);
      }
      return Date.valueOf(adjusted.toLocalDate());
    }
    if (temporal instanceof Time t) {
      if (!interval.period().isZero()) {
        throw new IllegalArgumentException("Cannot add calendar interval to TIME");
      }
      Duration dur = interval.duration();
      long days = dur.toDays();
      Duration remainder = dur.minus(Duration.ofDays(days));
      LocalTime lt = t.toLocalTime().plus(remainder);
      lt = lt.plusHours(days * 24);
      return Time.valueOf(lt);
    }
    if (temporal instanceof LocalDateTime ldt) {
      return ldt.plus(interval.period()).plus(interval.duration());
    }
    if (temporal instanceof LocalDate ld) {
      LocalDateTime adjusted = ld.atStartOfDay().plus(interval.period()).plus(interval.duration());
      if (interval.hasTimeComponent()) {
        return adjusted;
      }
      return adjusted.toLocalDate();
    }
    if (temporal instanceof LocalTime lt) {
      if (!interval.period().isZero()) {
        throw new IllegalArgumentException("Cannot add calendar interval to time");
      }
      Duration dur = interval.duration();
      long days = dur.toDays();
      Duration remainder = dur.minus(Duration.ofDays(days));
      LocalTime adjusted = lt.plus(remainder).plusHours(days * 24);
      return adjusted;
    }
    if (temporal instanceof OffsetDateTime odt) {
      return odt.plus(interval.period()).plus(interval.duration());
    }
    return null;
  }

  /**
   * Apply a CAST expression for literals.
   *
   * @param cast
   *          the cast expression
   * @param value
   *          the evaluated inner value
   * @return the cast result converted to the requested type when possible
   * @throws IllegalArgumentException
   *           if decimal bounds are invalid (SQLState 42000) or the rounded value
   *           exceeds the target's range or precision (SQLState 22003), or
   *           numeric input is invalid or non-finite (SQLState 22018), or
   *           temporal input has invalid format (22007) or out-of-range fields
   *           (22008), carried by the cause
   */
  public static Object castLiteral(CastExpression cast, Object value) {
    if (cast != null && cast.getColDataType() != null
        && DecimalCasts.isDecimalType(cast.getColDataType().getDataType())) {
      return castToBigDecimal(cast, value);
    }
    if (cast == null || value == null) {
      return value;
    }
    if (cast.isTimeStamp()) {
      return toTimestamp(value);
    }
    if (cast.isDate()) {
      return toDate(value);
    }
    if (cast.isTime()) {
      return toTime(value);
    }
    ColDataType colType = cast.getColDataType();
    String dataType = colType == null ? null : colType.getDataType();
    if (dataType == null) {
      return value;
    }
    String normalized = normalizeTypeName(dataType);
    NumericCastTarget target = NumericCastTarget.fromNormalizedName(normalized);
    if (target != null) {
      return NumericCasts.convert(value, target, colType.toString());
    }
    return switch (normalized) {
      case "boolean", "bool" -> toBoolean(value);
      case "char", "character", "nchar", "varchar", "charactervarying", "nvarchar", "string" ->
        castToString(cast, value);
      case "text", "clob" -> value.toString();
      default -> value;
    };
  }

  private static String castToString(CastExpression cast, Object value) {
    String text = value == null ? null : value.toString();
    if (text == null) {
      return null;
    }
    Integer length = firstIntegerArgument(cast);
    if (length != null && length >= 0 && text.length() > length) {
      return text.substring(0, length);
    }
    return text;
  }

  /**
   * Rounds a decimal to declared scale and rejects invalid bounds or precision
   * overflow.
   *
   * @param cast
   *          the decimal CAST expression
   * @param value
   *          the numeric input, possibly null
   * @return the converted decimal, or null for a null input
   * @throws IllegalArgumentException
   *           if the declared bounds cannot represent the result
   */
  private static BigDecimal castToBigDecimal(CastExpression cast, Object value) {
    return DecimalCasts.convert(cast, NumericCasts.toBigDecimal(value));
  }

  private static Integer firstIntegerArgument(CastExpression cast) {
    return integerArgument(cast, 0);
  }

  private static Integer integerArgument(CastExpression cast, int index) {
    ColDataType colType = cast.getColDataType();
    if (colType == null) {
      return null;
    }
    List<Integer> args = parseIntegerArguments(colType);
    if (index >= args.size()) {
      return null;
    }
    return args.get(index);
  }

  private static Boolean toBoolean(Object value) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    if (value instanceof Number num) {
      if (value instanceof BigDecimal bd) {
        return bd.compareTo(BigDecimal.ZERO) != 0;
      }
      return num.doubleValue() != 0d;
    }
    String text = value.toString().trim();
    if (text.isEmpty()) {
      return Boolean.FALSE;
    }
    String normalized = text.toLowerCase(Locale.ROOT);
    if ("true".equals(normalized) || "t".equals(normalized) || "yes".equals(normalized) || "y".equals(normalized)
        || "1".equals(normalized)) {
      return Boolean.TRUE;
    }
    if ("false".equals(normalized) || "f".equals(normalized) || "no".equals(normalized) || "n".equals(normalized)
        || "0".equals(normalized)) {
      return Boolean.FALSE;
    }
    throw new IllegalArgumentException("Cannot cast value '" + SqlExceptions.diagnosticValue(value) + "' to BOOLEAN");
  }

  private static String normalizeTypeName(String dataType) {
    String normalized = dataType.toLowerCase(Locale.ROOT).trim().replace("_", "");
    int paren = normalized.indexOf('(');
    if (paren >= 0) {
      normalized = normalized.substring(0, paren);
    }
    return normalized.replace(" ", "");
  }

  private static List<Integer> parseIntegerArguments(ColDataType colType) {
    if (colType == null) {
      return Collections.emptyList();
    }
    List<String> args = colType.getArgumentsStringList();
    if (args != null && !args.isEmpty()) {
      List<Integer> ints = new ArrayList<>(args.size());
      for (String arg : args) {
        addIfInteger(ints, arg);
      }
      if (!ints.isEmpty()) {
        return ints;
      }
    }
    String type = colType.getDataType();
    if (type == null) {
      return Collections.emptyList();
    }
    int start = type.indexOf('(');
    int end = type.indexOf(')', start + 1);
    if (start < 0 || end < 0 || end <= start + 1) {
      return Collections.emptyList();
    }
    String slice = type.substring(start + 1, end).replace(" ", "");
    if (slice.isEmpty()) {
      return Collections.emptyList();
    }
    String[] parts = slice.split(",");
    List<Integer> ints = new ArrayList<>(parts.length);
    for (String part : parts) {
      addIfInteger(ints, part);
    }
    return ints;
  }

  private static void addIfInteger(List<Integer> target, String text) {
    if (text == null) {
      return;
    }
    String trimmed = text.trim();
    if (trimmed.isEmpty()) {
      return;
    }
    int startIdx = (trimmed.charAt(0) == '-' || trimmed.charAt(0) == '+') ? 1 : 0;
    if (startIdx == trimmed.length()) {
      return;
    }
    for (int i = startIdx; i < trimmed.length(); i++) {
      if (!Character.isDigit(trimmed.charAt(i))) {
        return;
      }
    }
    target.add(Integer.valueOf(trimmed));
  }

  /**
   * Converts a value to a timestamp, accepting unpadded SQL fields, date-only and
   * ISO date-time strings while rejecting invalid calendar and clock fields.
   *
   * @param value
   *          the temporal, numeric, or string value
   * @return the converted SQL temporal value
   * @throws IllegalArgumentException
   *           if the value cannot be converted, carrying SQLState 22007 for
   *           invalid format or 22008 for out-of-range fields
   */
  public static Timestamp toTimestamp(Object value) {
    if (value instanceof Timestamp ts) {
      return ts;
    }
    if (value instanceof Date d) {
      return new Timestamp(d.getTime());
    }
    if (value instanceof Time t) {
      return new Timestamp(t.getTime());
    }
    if (value instanceof Number n) {
      return new Timestamp(n.longValue());
    }
    if (value instanceof OffsetDateTime odt) {
      return Timestamp.from(odt.toInstant());
    }
    if (value instanceof LocalDateTime ldt) {
      return Timestamp.valueOf((LocalDateTime) value);
    }
    if (value instanceof LocalDate ld) {
      return Timestamp.valueOf(((LocalDate) value).atStartOfDay());
    }
    String text = value.toString().trim();
    String normalized = text.replace('T', ' ');
    try {
      return Timestamp.valueOf(LocalDateTime.parse(normalized, SQL_TIMESTAMP));
    } catch (DateTimeParseException ex) {
      try {
        return Timestamp.from(OffsetDateTime.parse(text, DateTimeFormatter.ISO_DATE_TIME).toInstant());
      } catch (DateTimeParseException ignore) {
        try {
          LocalDateTime ldt = LocalDateTime.parse(text, DateTimeFormatter.ISO_DATE_TIME);
          return Timestamp.valueOf(ldt);
        } catch (DateTimeParseException ignore2) {
          try {
            return Timestamp.valueOf(LocalDate.parse(text, SQL_DATE).atStartOfDay());
          } catch (DateTimeParseException invalid) {
            throw temporalFailure("TIMESTAMP", text, ex, ignore, ignore2, invalid);
          }
        }
      }
    }
  }

  /**
   * Converts a value to a date, accepting unpadded SQL dates and ISO date-time
   * strings while rejecting invalid calendar dates.
   *
   * @param value
   *          the temporal, numeric, or string value
   * @return the converted SQL temporal value
   * @throws IllegalArgumentException
   *           if the value cannot be converted, carrying SQLState 22007 for
   *           invalid format or 22008 for out-of-range fields
   */
  public static Date toDate(Object value) {
    if (value instanceof Date d) {
      return d;
    }
    if (value instanceof Timestamp ts) {
      return new Date(ts.getTime());
    }
    if (value instanceof LocalDate ld) {
      return Date.valueOf(ld);
    }
    if (value instanceof LocalDateTime ldt) {
      return Date.valueOf(((LocalDateTime) value).toLocalDate());
    }
    if (value instanceof Number n) {
      return new Date(n.longValue());
    }
    String text = value.toString().trim();
    try {
      return Date.valueOf(LocalDate.parse(text, SQL_DATE));
    } catch (DateTimeParseException ex) {
      try {
        LocalDate parsed = LocalDate.parse(text.replace(' ', 'T'), DateTimeFormatter.ISO_DATE_TIME);
        return Date.valueOf(parsed);
      } catch (DateTimeParseException invalid) {
        throw temporalFailure("DATE", text, ex, invalid);
      }
    }
  }

  /**
   * Converts a value to a time, accepting unpadded SQL fields, fractional
   * seconds, and ISO date-time strings while rejecting out-of-range clock fields.
   *
   * @param value
   *          the temporal, numeric, or string value
   * @return the converted SQL temporal value
   * @throws IllegalArgumentException
   *           if the value cannot be converted, carrying SQLState 22007 for
   *           invalid format or 22008 for out-of-range fields
   */
  public static Time toTime(Object value) {
    if (value instanceof Time t) {
      return t;
    }
    if (value instanceof Timestamp ts) {
      return new Time(ts.getTime());
    }
    if (value instanceof LocalTime lt) {
      return Time.valueOf(lt);
    }
    if (value instanceof Number n) {
      return new Time(n.longValue());
    }
    String text = value.toString().trim();
    try {
      return Time.valueOf(LocalTime.parse(text, SQL_TIME));
    } catch (DateTimeParseException ex) {
      try {
        LocalTime parsed = LocalTime.parse(text.replace(' ', 'T'), DateTimeFormatter.ISO_DATE_TIME);
        return Time.valueOf(parsed);
      } catch (DateTimeParseException invalid) {
        throw temporalFailure("TIME", text, ex, invalid);
      }
    }
  }

  /**
   * Classifies failed temporal parsing after all supported formats were tried. A
   * parser failure caused by DateTimeException means the format was read but its
   * fields could not be resolved; lexical failures have no such cause.
   *
   * @param target
   *          the SQL temporal type
   * @param text
   *          the input text
   * @param failures
   *          the failures from supported formats, in preference order
   * @return an engine exception carrying a data SQLState and the original parser
   *         failure
   */
  private static IllegalArgumentException temporalFailure(String target, String text,
      DateTimeParseException... failures) {
    DateTimeParseException original = failures[0];
    String state = "22007";
    for (DateTimeParseException failure : failures) {
      if (failure.getCause() instanceof DateTimeException) {
        original = failure;
        state = "22008";
        break;
      }
    }
    String message = "Invalid " + target + " value: " + SqlExceptions.diagnosticValue(text);
    return new IllegalArgumentException(message, new SQLDataException(message, state, original));
  }

}
