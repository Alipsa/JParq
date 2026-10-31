package se.alipsa.jparq.engine;

import java.math.BigDecimal;
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
import java.util.Map;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/**
 * Converts typed JDBC getters using SQL conversion rules and java.time
 * mappings.
 */
public final class JdbcConversions {

  private static final Map<Class<?>, NumericCastTarget> NUMBERS = Map.of(Byte.class, NumericCastTarget.TINYINT,
      Short.class, NumericCastTarget.SMALLINT, Integer.class, NumericCastTarget.INTEGER, Long.class,
      NumericCastTarget.BIGINT, Float.class, NumericCastTarget.REAL, Double.class, NumericCastTarget.DOUBLE);

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
   * Applies supported numeric, date, time, and timestamp conversions.
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
    if (type == Date.class || type == LocalDate.class) {
      Date date = DateTimeFunctions.toDate(value);
      return type == Date.class ? date : date.toLocalDate();
    }
    if (type == Time.class || type == LocalTime.class || type == OffsetTime.class) {
      Time time = DateTimeFunctions.toTime(value);
      LocalTime local = DateTimeFunctions.toLocalTime(time);
      return type == Time.class ? time : type == LocalTime.class ? local : local.atOffset(ZoneOffset.UTC);
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
