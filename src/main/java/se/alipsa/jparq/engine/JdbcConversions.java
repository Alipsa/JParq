package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.Set;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/**
 * Converts typed JDBC getters using SQL conversion rules and java.time
 * mappings.
 */
public final class JdbcConversions {

  private static final Map<Class<?>, NumericCastTarget> NUMBERS = Map.of(Byte.class, NumericCastTarget.TINYINT,
      Short.class, NumericCastTarget.SMALLINT, Integer.class, NumericCastTarget.INTEGER, Long.class,
      NumericCastTarget.BIGINT, Float.class, NumericCastTarget.REAL, Double.class, NumericCastTarget.DOUBLE);

  private static final Set<Class<?>> TEMPORAL_TARGETS = Set.of(Date.class, Time.class, Timestamp.class, LocalDate.class,
      LocalTime.class, LocalDateTime.class, OffsetTime.class, OffsetDateTime.class);

  /** Prevents utility class instantiation. */
  private JdbcConversions() {
  }

  /**
   * Converts a JDBC value without evaluating its projection again.
   *
   * @param value
   *          decoded JDBC value, possibly null
   * @param type
   *          requested target class
   * @param <T>
   *          target Java type
   * @return converted value or null for SQL NULL
   * @throws SQLException
   *           if conversion is unsupported (22005), invalid (22007 or 22018), or
   *           outside the target range (22003)
   */
  public static <T> T convert(Object value, Class<T> type) throws SQLException {
    if (type == null) {
      throw new SQLDataException("Requested Java type cannot be null", "22005");
    }
    if (value == null || type.isInstance(value)) {
      return type.cast(value);
    }
    try {
      Object converted = convertValue(value, type);
      return type.cast(converted);
    } catch (RuntimeException failure) {
      throw SqlExceptions.wrap("Cannot convert " + value.getClass().getName() + " to " + type.getName(), failure);
    }
  }

  /**
   * Converts a JDBC value to a boolean using the shared CAST rules plus the JDBC
   * '1' and '0' text forms.
   *
   * @param value
   *          the input, non-null; booleans and numbers convert by their boolean
   *          or decimal sign, and text uses the shared CAST text forms plus '1'
   *          and '0'
   * @return the converted boolean
   * @throws IllegalArgumentException
   *           if the input cannot be converted, carrying SQLState 22018
   */
  public static boolean jdbcBoolean(Object value) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    if (value instanceof Number || value instanceof String) {
      if (value instanceof String text) {
        String trimmed = text.trim();
        if ("1".equals(trimmed)) {
          return true;
        }
        if ("0".equals(trimmed)) {
          return false;
        }
      }
      return BooleanCasts.toBoolean(value);
    }
    String message = "Cannot cast value '" + SqlExceptions.diagnosticValue(value) + "' to BOOLEAN";
    throw new IllegalArgumentException(message, new SQLDataException(message, "22018"));
  }

  /**
   * Converts a native number for a JDBC numeric getter, applying the shared CAST
   * range checks unless the source type is already guaranteed to fit the target.
   * Floating-point sources keep NaN and infinity for the floating-point targets,
   * and finite values beyond the target range raise 22003.
   *
   * @param number
   *          the input, non-null
   * @param target
   *          the requested numeric type
   * @return the number, validated to represent the same value within the target
   *         range
   * @throws IllegalArgumentException
   *           if the value is not finite for an integer target (22018) or exceeds
   *           the target range (22003)
   */
  public static Number jdbcNumeric(Number number, NumericCastTarget target) {
    if (guaranteedToFit(number, target)) {
      return number;
    }
    if (number instanceof Float || number instanceof Double) {
      return rangeCheckFloatingPoint(number, target);
    }
    return NumericCasts.convert(number, target, target.name());
  }

  /**
   * Range-checks a floating-point source with primitive comparisons, falling back
   * to the shared checks by exact decimal value only for values beyond the target
   * range, because the shortest text form can misreport the digit count at large
   * magnitudes.
   *
   * @param number
   *          a floating-point value not covered by the guaranteed-fit fast path
   * @param target
   *          the requested numeric type
   * @return the source, whose narrowing to the target is exact
   * @throws IllegalArgumentException
   *           if the value is not finite for an integer target (22018) or exceeds
   *           the target range (22003)
   */
  private static Number rangeCheckFloatingPoint(Number number, NumericCastTarget target) {
    double value = number.doubleValue();
    if (!Double.isFinite(value)) {
      if (target == NumericCastTarget.REAL) {
        return (float) value;
      }
      String message = "Cannot cast value '" + SqlExceptions.diagnosticValue(number) + "' to a numeric value";
      throw new IllegalArgumentException(message, new SQLDataException(message, "22018"));
    }
    if (fitsInTarget(value, target)) {
      return number;
    }
    return convertWithOriginalDiagnostic(number, new BigDecimal(value), target);
  }

  /**
   * Compares a finite value with the target's bounds before truncation, so in
   * range values never build a decimal expansion.
   *
   * @param value
   *          the finite value
   * @param target
   *          the requested numeric type
   * @return whether truncation toward zero stays within the target range
   */
  private static boolean fitsInTarget(double value, NumericCastTarget target) {
    return switch (target) {
      case TINYINT -> value >= -128.0 && value < 128.0;
      case SMALLINT -> value >= -32768.0 && value < 32768.0;
      case INTEGER -> value >= -2147483648.0 && value < 2147483648.0;
      case BIGINT -> value >= -0x1p63 && value < 0x1p63;
      case REAL -> Float.isFinite((float) value);
      case DOUBLE -> true;
    };
  }

  /**
   * Applies the shared range checks with the original value in the diagnostic,
   * because the exact decimal expansion of a large magnitude is not useful to a
   * reader.
   *
   * @param number
   *          the original value for diagnostics
   * @param exact
   *          the value's exact decimal form
   * @param target
   *          the requested numeric type
   * @return the converted value
   * @throws IllegalArgumentException
   *           if the value exceeds the target range (22003)
   */
  private static Number convertWithOriginalDiagnostic(Number number, BigDecimal exact, NumericCastTarget target) {
    try {
      return NumericCasts.convert(exact, target, target.name());
    } catch (IllegalArgumentException overflow) {
      String message = "Cannot cast numeric value '" + SqlExceptions.diagnosticValue(number) + "' to " + target
          + ": value out of range";
      throw new IllegalArgumentException(message, new SQLDataException(message, "22003", overflow.getCause()));
    }
  }

  /**
   * Identifies sources whose range is already contained in the target, making the
   * shared range checks unnecessary.
   *
   * @param number
   *          the input, non-null
   * @param target
   *          the requested numeric type
   * @return whether the source type cannot overflow the target
   */
  private static boolean guaranteedToFit(Number number, NumericCastTarget target) {
    boolean integral = number instanceof Byte || number instanceof Short || number instanceof Integer
        || number instanceof Long;
    return switch (target) {
      case TINYINT -> number instanceof Byte;
      case SMALLINT -> number instanceof Byte || number instanceof Short;
      case INTEGER -> number instanceof Byte || number instanceof Short || number instanceof Integer;
      case BIGINT -> integral;
      case REAL -> integral || number instanceof Float;
      case DOUBLE -> integral || number instanceof Float || number instanceof Double;
    };
  }

  /**
   * Converts a JDBC value to its binary bytes.
   *
   * @param value
   *          the input, non-null; byte arrays pass through and text converts to
   *          UTF-8 bytes
   * @return the byte array
   * @throws IllegalArgumentException
   *           if the input cannot be converted, carrying SQLState 22018
   */
  public static byte[] toBytes(Object value) {
    if (value instanceof byte[] bytes) {
      return bytes;
    }
    if (value instanceof String text) {
      return text.getBytes(StandardCharsets.UTF_8);
    }
    String message = "Cannot cast value '" + SqlExceptions.diagnosticValue(value) + "' to a binary value";
    throw new IllegalArgumentException(message, new SQLDataException(message, "22018"));
  }

  /**
   * Applies supported numeric, boolean, binary, date, time, and timestamp
   * conversions.
   *
   * @param value
   *          non-null source
   * @param type
   *          target class
   * @return converted value
   * @throws SQLException
   *           if the requested conversion is not supported
   */
  private static Object convertValue(Object value, Class<?> type) throws SQLException {
    if (type == Double.class && value instanceof Float number) {
      return number.doubleValue();
    }
    NumericCastTarget numeric = NUMBERS.get(type);
    if (numeric != null) {
      if (value instanceof Number number) {
        Number checked = jdbcNumeric(number, numeric);
        if (type.isInstance(checked)) {
          return checked;
        }
        return switch (numeric) {
          case TINYINT -> checked.byteValue();
          case SMALLINT -> checked.shortValue();
          case INTEGER -> checked.intValue();
          case BIGINT -> checked.longValue();
          case REAL -> checked.floatValue();
          case DOUBLE -> checked.doubleValue();
        };
      }
      return NumericCasts.convert(value, numeric, type.getSimpleName());
    }
    if (type == BigDecimal.class) {
      return NumericCasts.toBigDecimal(value);
    }
    if (type == Boolean.class) {
      return jdbcBoolean(value);
    }
    if (type == byte[].class) {
      return toBytes(value);
    }
    if (TEMPORAL_TARGETS.contains(type)
        && !(value instanceof java.util.Date || value instanceof TemporalAccessor || value instanceof CharSequence)) {
      throw new SQLDataException("Cannot convert " + value.getClass().getName() + " to " + type.getName(), "22005");
    }
    if (type == Date.class || type == LocalDate.class) {
      Date date = DateTimeFunctions.toDate(value);
      return type == Date.class ? date : date.toLocalDate();
    }
    if (type == Time.class || type == LocalTime.class || type == OffsetTime.class) {
      Time time = DateTimeFunctions.toTime(value);
      LocalTime local = DateTimeFunctions.toLocalTime(time);
      return type == Time.class
          ? time
          : type == LocalTime.class
              ? local
              : local.atOffset(DateTimeFunctions.sessionOffset()).withOffsetSameInstant(ZoneOffset.UTC);
    }
    if (type == Timestamp.class || type == LocalDateTime.class || type == OffsetDateTime.class) {
      Timestamp timestamp = DateTimeFunctions.toTimestamp(value);
      return type == Timestamp.class
          ? timestamp
          : type == LocalDateTime.class ? timestamp.toLocalDateTime() : timestamp.toInstant().atOffset(ZoneOffset.UTC);
    }
    throw new SQLDataException("Cannot convert " + value.getClass().getName() + " to " + type.getName(), "22005");
  }
}
