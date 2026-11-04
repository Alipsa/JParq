package se.alipsa.jparq.engine.function;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.SubqueryExecutor.SubqueryResult;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Helpers for ARRAY functions and constructors.
 */
public final class ArrayFunctions {

  private ArrayFunctions() {
  }

  /**
   * Validates an ARRAY subquery's shape and normalizes its element types.
   *
   * @param result
   *          the executed subquery
   * @param subSelect
   *          the subquery used in error diagnostics
   * @return homogenized first-column values, or an empty array for no columns
   * @throws IllegalArgumentException
   *           if the result has multiple columns or incompatible element types
   */
  public static List<Object> fromSubquery(SubqueryResult result, Select subSelect) {
    if (result.columnLabels().isEmpty()) {
      return List.of();
    }
    if (result.columnLabels().size() > 1) {
      throw new IllegalArgumentException("ARRAY subquery must return exactly one column: " + subSelect);
    }
    return homogenizeArrayValues(result.singleColumnValues());
  }

  /**
   * Normalize ARRAY element types so that a single SQL ARRAY has a consistent
   * element type and nesting depth. Numeric leaves are promoted across sibling
   * arrays; DATE and TIMESTAMP leaves promote to TIMESTAMP, and zoned temporal
   * leaves promote compatible local values using the active JVM time zone. SQL
   * and java.time representations share a family. TIME leaves use LocalTime to
   * retain nanoseconds. NULLs and empty arrays do not constrain the inferred leaf
   * type. Nested reference arrays, including stored Parquet array columns, are
   * normalized to unmodifiable lists. Primitive arrays remain scalar values. Map
   * implementations share the MAP family and retain their original values. MAP
   * key/value types and STRUCT field schemas are not inspected.
   *
   * @param values
   *          raw values
   * @return homogenized list or {@code List.of()} when {@code values} is null or
   *         empty
   * @throws IllegalArgumentException
   *           if non-NULL elements cannot share a common type; the cause carries
   *           SQLState 42000
   */
  public static List<Object> homogenizeArrayValues(List<Object> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    ElementType type = new ElementType();
    inferElementType(values, type);
    return coerceArrayValues(values, type);
  }

  /**
   * Infers a shared type at each depth across all sibling arrays.
   *
   * @param values
   *          raw array elements
   * @param type
   *          common type accumulated for this depth
   * @throws IllegalArgumentException
   *           if element families or depths differ; the cause carries SQLState
   *           42000
   */
  private static void inferElementType(List<?> values, ElementType type) {
    for (Object value : values) {
      if (value == null) {
        continue;
      }
      Class<?> candidate;
      if (value instanceof Number number) {
        candidate = Number.class;
        type.numeric = NumericType.promote(type.numeric, NumericType.of(number));
      } else if (value instanceof CharSequence) {
        candidate = String.class;
      } else if (value instanceof List<?> || value instanceof Object[]) {
        candidate = List.class;
      } else if (value instanceof Map<?, ?>) {
        candidate = Map.class;
      } else {
        candidate = temporalFamily(value);
      }
      type.family = determineObjectTarget(type.family, candidate);
      if (value instanceof TemporalInterval interval) {
        type.interval = IntervalType.common(type.interval,
            IntervalType.schema(interval.qualifier(), interval.fractionalPrecision()));
      }
      if (candidate == List.class) {
        if (type.nested == null) {
          type.nested = new ElementType();
        }
        inferElementType(arrayValues(value), type.nested);
      }
    }
  }

  /**
   * Copies an array, applying the inferred type to every non-NULL element.
   *
   * @param values
   *          validated array elements
   * @param type
   *          common type for this depth
   * @return recursively normalized, unmodifiable array
   */
  private static List<Object> coerceArrayValues(List<?> values, ElementType type) {
    if (type.numeric != NumericType.NONE) {
      return convertNumericValues(values, type.numeric);
    }
    List<Object> coerced = new ArrayList<>(values.size());
    for (Object value : values) {
      if (value == null) {
        coerced.add(null);
      } else if (type.family == String.class) {
        coerced.add(value.toString());
      } else if (type.interval != null) {
        coerced.add(IntervalType.project((TemporalInterval) value, type.interval));
      } else if (type.family == List.class) {
        coerced.add(coerceArrayValues(arrayValues(value), type.nested));
      } else {
        coerced.add(coerceTemporal(value, type.family));
      }
    }
    return Collections.unmodifiableList(coerced);
  }

  /**
   * Views a validated nested array as a list without copying its elements.
   *
   * @param value
   *          list or reference array
   * @return list view of the array elements
   */
  private static List<?> arrayValues(Object value) {
    return value instanceof Object[] array ? Arrays.asList(array) : (List<?>) value;
  }

  /**
   * Resolves a common element family, promoting compatible temporal types.
   * Diagnostics distinguish Java representations when SQL type labels coincide.
   *
   * @param current
   *          previously established family, or null for the first non-NULL value
   * @param candidate
   *          family of the next non-NULL element
   * @return the common element family
   * @throws IllegalArgumentException
   *           if the families differ; the cause carries SQLState 42000
   */
  private static Class<?> determineObjectTarget(Class<?> current, Class<?> candidate) {
    if (current == null || current == candidate) {
      return candidate;
    }
    if ((current == Date.class || current == Timestamp.class || current == OffsetDateTime.class)
        && (candidate == Date.class || candidate == Timestamp.class || candidate == OffsetDateTime.class)) {
      return current == OffsetDateTime.class || candidate == OffsetDateTime.class
          ? OffsetDateTime.class
          : Timestamp.class;
    }
    if ((current == LocalTime.class || current == OffsetTime.class)
        && (candidate == LocalTime.class || candidate == OffsetTime.class)) {
      return OffsetTime.class;
    }
    String currentName = sqlTypeName(current);
    String candidateName = sqlTypeName(candidate);
    if (currentName.equals(candidateName)) {
      currentName += " (" + current.getName() + ")";
      candidateName += " (" + candidate.getName() + ")";
    }
    throw SqlExceptions.incompatibleArrayElements(currentName, candidateName);
  }

  /**
   * Maps supported Java temporal representations to a common SQL family.
   *
   * @param value
   *          non-NULL scalar element
   * @return canonical family, or the original class for non-temporal values
   */
  private static Class<?> temporalFamily(Object value) {
    return switch (value) {
      case Date ignored -> Date.class;
      case LocalDate ignored -> Date.class;
      case Time ignored -> LocalTime.class;
      case LocalTime ignored -> LocalTime.class;
      case Timestamp ignored -> Timestamp.class;
      case LocalDateTime ignored -> Timestamp.class;
      case OffsetDateTime ignored -> OffsetDateTime.class;
      case ZonedDateTime ignored -> OffsetDateTime.class;
      case Instant ignored -> OffsetDateTime.class;
      default -> value.getClass();
    };
  }

  /**
   * Converts a temporal leaf to the inferred family without losing fractional
   * seconds or explicit offsets. Local values promoted to zoned timestamps use
   * the zone's rules on their own date; local times use today's date.
   *
   * @param value
   *          non-NULL, compatible leaf
   * @param family
   *          inferred canonical family
   * @return promoted temporal value, or the original non-temporal value
   */
  private static Object coerceTemporal(Object value, Class<?> family) {
    if (family == Date.class) {
      return DateTimeFunctions.toDate(value);
    }
    if (family == Timestamp.class) {
      return value instanceof Date date
          ? Timestamp.valueOf(date.toLocalDate().atStartOfDay())
          : DateTimeFunctions.toTimestamp(value);
    }
    if (family == LocalTime.class) {
      return value instanceof Time time ? DateTimeFunctions.toLocalTime(time) : value;
    }
    if (family == OffsetDateTime.class) {
      return switch (value) {
        case OffsetDateTime dateTime -> dateTime;
        case ZonedDateTime dateTime -> dateTime.toOffsetDateTime();
        case Instant instant -> instant.atZone(DateTimeFunctions.defaultZone()).toOffsetDateTime();
        case Date date -> date.toLocalDate().atStartOfDay(DateTimeFunctions.defaultZone()).toOffsetDateTime();
        case LocalDate date -> date.atStartOfDay(DateTimeFunctions.defaultZone()).toOffsetDateTime();
        default -> DateTimeFunctions.toTimestamp(value).toLocalDateTime().atZone(DateTimeFunctions.defaultZone())
            .toOffsetDateTime();
      };
    }
    if (family == OffsetTime.class && !(value instanceof OffsetTime)) {
      LocalTime time = value instanceof Time sqlTime ? DateTimeFunctions.toLocalTime(sqlTime) : (LocalTime) value;
      ZoneId zone = DateTimeFunctions.defaultZone();
      return time.atDate(LocalDate.now(zone)).atZone(zone).toOffsetDateTime().toOffsetTime();
    }
    return value;
  }

  /**
   * Names an element family using SQL terminology.
   *
   * @param family
   *          normalized Java element family
   * @return SQL type name, or the simple Java name for an unmapped custom class
   */
  private static String sqlTypeName(Class<?> family) {
    if (family == Number.class) {
      return "NUMERIC";
    }
    if (Map.class.isAssignableFrom(family)) {
      return "MAP";
    }
    if (IndexedRecord.class.isAssignableFrom(family)) {
      return "STRUCT";
    }
    JDBCType jdbcType = JDBCType.valueOf(JdbcTypeMapper.mapJavaClassNameToJdbcType(family.getName()));
    if (jdbcType != JDBCType.OTHER) {
      return jdbcType.getName();
    }
    return switch (family.getName()) {
      case "java.time.LocalDate" -> "DATE";
      case "java.time.LocalTime" -> "TIME";
      case "java.time.LocalDateTime" -> "TIMESTAMP";
      case "java.time.OffsetTime" -> "TIME_WITH_TIMEZONE";
      case "java.time.OffsetDateTime", "java.time.ZonedDateTime", "java.time.Instant" -> "TIMESTAMP_WITH_TIMEZONE";
      default -> family.getSimpleName();
    };
  }

  /**
   * Promotes previously validated numeric elements to their common type.
   *
   * @param values
   *          numeric values, optionally including NULLs
   * @param target
   *          common numeric type
   * @return unmodifiable list of promoted values
   */
  private static List<Object> convertNumericValues(List<?> values, NumericType target) {
    List<Object> converted = new ArrayList<>(values.size());
    for (Object value : values) {
      if (value == null) {
        converted.add(null);
        continue;
      }
      Number number = (Number) value;
      converted.add(switch (target) {
        case BIG_DECIMAL -> NumericFunctions.toBigDecimal(number);
        case DOUBLE -> number.doubleValue();
        case FLOAT -> number.floatValue();
        case LONG -> number.longValue();
        case INTEGER -> number.intValue();
        case SHORT -> number.shortValue();
        case BYTE -> number.byteValue();
        case NONE -> number;
      });
    }
    return Collections.unmodifiableList(converted);
  }

  /** Common element family and numeric promotion target at one array depth. */
  private static final class ElementType {
    private Class<?> family;
    private NumericType numeric = NumericType.NONE;
    private ElementType nested;
    private Schema interval;
  }

  private enum NumericType {
    NONE, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BIG_DECIMAL;

    static NumericType of(Number value) {
      if (value instanceof BigDecimal || value instanceof java.math.BigInteger) {
        return BIG_DECIMAL;
      }
      String className = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);
      return switch (className) {
        case "double" -> DOUBLE;
        case "float" -> FLOAT;
        case "long" -> LONG;
        case "integer", "int" -> INTEGER;
        case "short" -> SHORT;
        case "byte" -> BYTE;
        default -> DOUBLE;
      };
    }

    static NumericType promote(NumericType current, NumericType candidate) {
      if (candidate == null || candidate == NONE) {
        return current;
      }
      if (current == NONE) {
        return candidate;
      }
      return values()[Math.max(current.ordinal(), candidate.ordinal())];
    }
  }
}
