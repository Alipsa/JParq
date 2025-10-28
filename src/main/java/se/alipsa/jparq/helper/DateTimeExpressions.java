package se.alipsa.jparq.helper;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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

/**
 * Utility methods for evaluating SQL date/time expressions and literal casts.
 */
public final class DateTimeExpressions {

  private static final ZoneId DEFAULT_ZONE = ZoneId.systemDefault();

  private DateTimeExpressions() {
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
    return switch (key.toUpperCase(Locale.ROOT)) {
      case "CURRENT_DATE" -> Date.valueOf(LocalDate.now(DEFAULT_ZONE));
      case "CURRENT_TIME" -> Time.valueOf(LocalTime.now(DEFAULT_ZONE));
      case "CURRENT_TIMESTAMP" -> Timestamp.from(Instant.now());
      default -> key;
    };
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
   * @return the resulting value or {@code null} if not a temporal subtraction
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
      if (start != null && end != null) {
        return TemporalInterval.between(start, end);
      }
    }
    return null;
  }

  private static boolean isTemporal(Object value) {
    return value instanceof Date || value instanceof Time || value instanceof Timestamp || value instanceof LocalDate
        || value instanceof LocalDateTime || value instanceof LocalTime || value instanceof OffsetDateTime;
  }

  private static TemporalAccessor toTemporalAccessor(Object value) {
    if (value instanceof Timestamp ts) {
      return ts.toLocalDateTime();
    }
    if (value instanceof Date d) {
      return d.toLocalDate().atStartOfDay();
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
    if (value instanceof OffsetDateTime odt) {
      return odt.toLocalDateTime();
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
   */
  public static Object castLiteral(CastExpression cast, Object value) {
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
    return switch (normalized) {
      case "boolean", "bool" -> toBoolean(value);
      case "char", "character", "nchar", "varchar", "charactervarying", "nvarchar",
          "string" -> castToString(cast, value);
      case "text", "clob" -> value.toString();
      case "tinyint" -> toBigDecimalValue(value).byteValue();
      case "smallint", "int2" -> toBigDecimalValue(value).shortValue();
      case "int", "integer", "signed", "int4" -> toBigDecimalValue(value).intValue();
      case "bigint", "int8" -> toBigDecimalValue(value).longValue();
      case "float", "real", "float4" -> toBigDecimalValue(value).floatValue();
      case "double", "doubleprecision", "float8" -> toBigDecimalValue(value).doubleValue();
      case "numeric", "decimal", "number" -> castToBigDecimal(cast, value);
      default -> value;
    };
  }

  private static BigDecimal toBigDecimalValue(Object value) {
    if (value instanceof BigDecimal bd) {
      return bd;
    }
    if (value instanceof Number num) {
      return new BigDecimal(num.toString());
    }
    if (value instanceof Boolean bool) {
      return bool ? BigDecimal.ONE : BigDecimal.ZERO;
    }
    return new BigDecimal(value.toString().trim());
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

  private static BigDecimal castToBigDecimal(CastExpression cast, Object value) {
    BigDecimal decimal = toBigDecimalValue(value);
    Integer scale = secondIntegerArgument(cast);
    if (scale != null) {
      decimal = decimal.setScale(scale, RoundingMode.HALF_UP);
    }
    return decimal;
  }

  private static Integer firstIntegerArgument(CastExpression cast) {
    return integerArgument(cast, 0);
  }

  private static Integer secondIntegerArgument(CastExpression cast) {
    return integerArgument(cast, 1);
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
      return num.intValue() != 0;
    }
    String text = value.toString().trim();
    if (text.isEmpty()) {
      return Boolean.FALSE;
    }
    String normalized = text.toLowerCase(Locale.ROOT);
    if ("true".equals(normalized) || "t".equals(normalized) || "yes".equals(normalized)
        || "y".equals(normalized) || "1".equals(normalized)) {
      return Boolean.TRUE;
    }
    if ("false".equals(normalized) || "f".equals(normalized) || "no".equals(normalized)
        || "n".equals(normalized) || "0".equals(normalized)) {
      return Boolean.FALSE;
    }
    throw new IllegalArgumentException("Cannot cast value '" + value + "' to BOOLEAN");
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

  private static Timestamp toTimestamp(Object value) {
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
      return Timestamp.valueOf(normalized);
    } catch (IllegalArgumentException ex) {
      try {
        return Timestamp.from(Instant.parse(text));
      } catch (DateTimeParseException ignore) {
        try {
          LocalDateTime ldt = LocalDateTime.parse(text, DateTimeFormatter.ISO_DATE_TIME);
          return Timestamp.valueOf(ldt);
        } catch (DateTimeParseException ignore2) {
          try {
            return Timestamp.valueOf(LocalDate.parse(text).atStartOfDay());
          } catch (DateTimeParseException ignore3) {
            throw ex;
          }
        }
      }
    }
  }

  private static Date toDate(Object value) {
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
      return Date.valueOf(text);
    } catch (IllegalArgumentException ex) {
      try {
        LocalDate parsed = LocalDate.parse(text, DateTimeFormatter.ISO_DATE_TIME);
        return Date.valueOf(parsed);
      } catch (DateTimeParseException ignore) {
        try {
          return Date.valueOf(LocalDateTime.parse(text.replace('T', ' ')).toLocalDate());
        } catch (Exception ignore2) {
          throw ex;
        }
      }
    }
  }

  private static Time toTime(Object value) {
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
      return Time.valueOf(text);
    } catch (IllegalArgumentException ex) {
      try {
        LocalTime parsed = LocalTime.parse(text, DateTimeFormatter.ISO_DATE_TIME);
        return Time.valueOf(parsed);
      } catch (DateTimeParseException ignore) {
        try {
          return Time.valueOf(LocalTime.parse(text));
        } catch (DateTimeParseException ignore2) {
          throw ex;
        }
      }
    }
  }
}
