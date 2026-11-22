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
    String message = "Cannot cast value '" + SqlExceptions.diagnosticValue(value) + "' to a boolean value";
    throw new IllegalArgumentException(message, new SQLDataException(message, "22018"));
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
