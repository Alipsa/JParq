package se.alipsa.jparq.engine.function;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
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
   * arrays; datetime leaves must have the same fields (DATE, TIME, or TIMESTAMP),
   * and zoned leaves promote local timestamps using their date's zone rules.
   * Date-free local times use the execution's current displacement. SQL and
   * java.time representations share a family; temporal leaves are always
   * converted to the JDBC representation of their family. NULLs and empty arrays
   * do not constrain the inferred leaf type. Nested reference arrays, including
   * stored Parquet array columns, are normalized to unmodifiable lists. Primitive
   * arrays remain scalar values. MAP keys and values are recursively validated
   * and normalized across sibling maps. STRUCT elements must have matching field
   * names, order, and compatible SQL field types, resolved recursively with
   * numeric promotion. Nullability, Avro names, and storage properties do not
   * distinguish SQL types.
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
    ElementType type = arrayType(values);
    ZoneOffset offset = requiresOffset(type) ? DateTimeFunctions.sessionOffset() : null;
    return coerceArrayValues(values, type, offset);
  }

  /**
   * Validates decoded values once and applies the declared common datetime type.
   * Source-aware callers decode UTC timestamps to offset values before invoking
   * this overload, retaining instants when a zoned type is required.
   *
   * @param values
   *          decoded ARRAY elements
   * @param declaration
   *          common ARRAY declaration
   * @return normalized elements with the declared datetime representation
   * @throws IllegalArgumentException
   *           if values cannot share the declared type
   */
  public static List<Object> homogenizeArrayValues(List<Object> values, Schema declaration) {
    ElementType type = arrayType(values);
    applyTemporalDeclaration(type, JdbcTypeMapper.nonNullSchema(declaration).getElementType());
    return coerceArrayValues(values, type, requiresOffset(type) ? DateTimeFunctions.sessionOffset() : null);
  }

  /**
   * Applies a known datetime leaf family after ordinary runtime compatibility
   * checks.
   *
   * @param type
   *          inferred element family
   * @param schema
   *          declared element schema
   * @throws IllegalArgumentException
   *           if an inferred family is incompatible
   */
  private static void applyTemporalDeclaration(ElementType type, Schema schema) {
    Schema effective = JdbcTypeMapper.nonNullSchema(schema);
    if (effective.getType() == Schema.Type.ARRAY) {
      determineObjectTarget(type.family, List.class);
      type.family = List.class;
      if (type.nested == null) {
        type.nested = new ElementType();
      }
      applyTemporalDeclaration(type.nested, effective.getElementType());
      return;
    }
    Class<?> family = switch (JdbcTypeMapper.mapSchemaToJdbcType(effective)) {
      case Types.DATE -> Date.class;
      case Types.TIME -> Time.class;
      case Types.TIMESTAMP -> Timestamp.class;
      case Types.TIME_WITH_TIMEZONE -> OffsetTime.class;
      case Types.TIMESTAMP_WITH_TIMEZONE -> OffsetDateTime.class;
      default -> type.family;
    };
    determineObjectTarget(type.family, family);
    type.family = family;
  }

  /**
   * Applies the ordinary runtime ARRAY compatibility checks without converting
   * local datetime fields to the JDBC storage representation.
   *
   * @param values
   *          evaluated ARRAY elements
   * @throws IllegalArgumentException
   *           if values cannot share a common ARRAY type
   */
  public static void validateArrayValues(List<?> values) {
    arrayType(values);
  }

  /**
   * Resolves and validates the common runtime type used by ARRAY consumers.
   *
   * @param values
   *          evaluated ARRAY elements
   * @return common runtime type
   */
  private static ElementType arrayType(List<?> values) {
    ElementType type = new ElementType();
    if (values != null) {
      inferElementType(values, type);
    }
    return type;
  }

  /**
   * Determines whether any depth promotes local temporal values to a zoned type.
   *
   * @param type
   *          inferred type of the outermost depth
   * @return true when a session time zone displacement is needed
   */
  private static boolean requiresOffset(ElementType type) {
    return type != null && (type.family == OffsetDateTime.class || type.family == OffsetTime.class
        || requiresOffset(type.nested) || requiresOffset(type.mapKey) || requiresOffset(type.mapValue));
  }

  /**
   * Infers shared types across sibling arrays, MAP entries, and STRUCT schemas.
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
      inferValue(value, type);
    }
  }

  /**
   * Accumulates one value without allocating singleton collections.
   *
   * @param value
   *          raw element or map entry component
   * @param type
   *          accumulated common type
   * @throws IllegalArgumentException
   *           if the value has an incompatible type
   */
  private static void inferValue(Object value, ElementType type) {
    if (value == null) {
      return;
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
    } else if (value instanceof IndexedRecord) {
      candidate = IndexedRecord.class;
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
    } else if (value instanceof Map<?, ?> map) {
      if (type.mapKey == null) {
        type.mapKey = new ElementType();
        type.mapValue = new ElementType();
      }
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        inferValue(entry.getKey(), type.mapKey);
        inferValue(entry.getValue(), type.mapValue);
      }
    } else if (value instanceof IndexedRecord record) {
      if (type.structSchemas == null) {
        type.structSchemas = new IdentityHashMap<>();
        type.structConversions = new IdentityHashMap<>();
      }
      Schema schema = record.getSchema();
      if (type.structSchemas.put(schema, Boolean.TRUE) == null) {
        type.struct = type.struct == null ? schema : StructArrayTypes.common(type.struct, schema);
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
   * @param offset
   *          SQL-session time zone displacement for zoned promotion, or null when
   *          no depth requires it
   * @return recursively normalized, unmodifiable array
   */
  private static List<Object> coerceArrayValues(List<?> values, ElementType type, ZoneOffset offset) {
    List<Object> coerced = new ArrayList<>(values.size());
    for (Object value : values) {
      coerced.add(coerceValue(value, type, offset));
    }
    return Collections.unmodifiableList(coerced);
  }

  /**
   * Converts one value to its inferred common type.
   *
   * @param value
   *          validated value
   * @param type
   *          common type
   * @param offset
   *          execution displacement for date-free TIME and existing instants
   * @return normalized value, retaining unchanged scalar instances
   * @throws IllegalArgumentException
   *           if conversion fails
   */
  private static Object coerceValue(Object value, ElementType type, ZoneOffset offset) {
    if (value == null) {
      return null;
    }
    if (type.numeric != NumericType.NONE) {
      return convertNumericValue((Number) value, type.numeric);
    }
    if (type.family == String.class) {
      return value.toString();
    }
    if (type.interval != null) {
      return IntervalType.project((TemporalInterval) value, type.interval);
    }
    if (type.family == List.class) {
      return coerceArrayValues(arrayValues(value), type.nested, offset);
    }
    if (type.family == Map.class) {
      return coerceMap((Map<?, ?>) value, type, offset);
    }
    if (value instanceof IndexedRecord record) {
      return type.structConversions
          .computeIfAbsent(record.getSchema(), source -> StructArrayTypes.converter(source, type.struct)).apply(record);
    }
    return coerceTemporal(value, type.family, offset);
  }

  /**
   * Normalizes map entries with the common key and value types. Unchanged maps
   * retain their identity; converted maps are unmodifiable.
   *
   * @param map
   *          validated map
   * @param type
   *          shared map element type
   * @param offset
   *          execution displacement for date-free TIME and existing instants
   * @return map with normalized keys and values
   * @throws IllegalArgumentException
   *           if key promotion creates duplicate keys; the cause carries SQLState
   *           42000
   */
  private static Map<?, ?> coerceMap(Map<?, ?> map, ElementType type, ZoneOffset offset) {
    Map<Object, Object> result = null;
    int index = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object key = coerceValue(entry.getKey(), type.mapKey, offset);
      Object value = coerceValue(entry.getValue(), type.mapValue, offset);
      if (result == null && (key != entry.getKey() || value != entry.getValue())) {
        result = new LinkedHashMap<>();
        int previous = 0;
        for (Map.Entry<?, ?> original : map.entrySet()) {
          if (previous++ == index) {
            break;
          }
          result.put(original.getKey(), original.getValue());
        }
      }
      if (result != null) {
        if (result.containsKey(key)) {
          throw SqlExceptions.incompatibleArrayElements("MAP keys", "duplicate keys after type promotion");
        }
        result.put(key, value);
      }
      index++;
    }
    return result == null ? map : Collections.unmodifiableMap(result);
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
    if ((current == Timestamp.class || current == OffsetDateTime.class)
        && (candidate == Timestamp.class || candidate == OffsetDateTime.class)) {
      return OffsetDateTime.class;
    }
    if ((current == Time.class || current == OffsetTime.class)
        && (candidate == Time.class || candidate == OffsetTime.class)) {
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
      case Time ignored -> Time.class;
      case LocalTime ignored -> Time.class;
      case Timestamp ignored -> Timestamp.class;
      case LocalDateTime ignored -> Timestamp.class;
      case OffsetDateTime ignored -> OffsetDateTime.class;
      case ZonedDateTime ignored -> OffsetDateTime.class;
      case Instant ignored -> OffsetDateTime.class;
      default -> value.getClass();
    };
  }

  /**
   * Converts a temporal leaf to the JDBC representation of the inferred family.
   * Explicit offsets are preserved; local values promoted to a zoned type receive
   * the SQL-session time zone displacement. TIME values retain millisecond
   * precision.
   *
   * @param value
   *          non-NULL, compatible leaf
   * @param family
   *          inferred canonical family
   * @param offset
   *          SQL-session time zone displacement used for zoned promotion
   * @return promoted temporal value, or the original non-temporal value
   */
  private static Object coerceTemporal(Object value, Class<?> family, ZoneOffset offset) {
    if (family == Date.class) {
      return DateTimeFunctions.toDate(value);
    }
    if (family == Timestamp.class) {
      return DateTimeFunctions.toTimestamp(value);
    }
    if (family == Time.class) {
      return DateTimeFunctions.toTime(value);
    }
    if (family == OffsetDateTime.class) {
      return switch (value) {
        case OffsetDateTime dateTime -> dateTime;
        case ZonedDateTime dateTime -> dateTime.toOffsetDateTime();
        case Instant instant -> instant.atOffset(offset);
        case LocalDateTime dateTime -> DateTimeFunctions.offsetTimestamp(dateTime);
        case Timestamp timestamp -> timestamp.toInstant().atZone(DateTimeFunctions.defaultZone()).toOffsetDateTime();
        default ->
          throw SqlExceptions.incompatibleArrayElements(sqlTypeName(temporalFamily(value)), sqlTypeName(family));
      };
    }
    if (family == OffsetTime.class && !(value instanceof OffsetTime)) {
      LocalTime time = value instanceof Time sqlTime ? DateTimeFunctions.toLocalTime(sqlTime) : (LocalTime) value;
      return time.atOffset(offset);
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
      case "java.time.OffsetTime" -> "TIME_WITH_TIMEZONE";
      case "java.time.OffsetDateTime" -> "TIMESTAMP_WITH_TIMEZONE";
      default -> family.getSimpleName();
    };
  }

  /**
   * Promotes a numeric value, retaining its instance when the type already
   * matches.
   *
   * @param number
   *          validated numeric value
   * @param target
   *          common numeric type
   * @return promoted value
   */
  private static Number convertNumericValue(Number number, NumericType target) {
    if (number.getClass() == target.javaClass()) {
      return number;
    }
    return switch (target) {
      case BIG_DECIMAL -> NumericFunctions.toBigDecimal(number);
      case DOUBLE -> number.doubleValue();
      case FLOAT -> number.floatValue();
      case LONG -> number.longValue();
      case INTEGER -> number.intValue();
      case SHORT -> number.shortValue();
      case BYTE -> number.byteValue();
      case NONE -> number;
    };
  }

  /** Common element family and recursive container types at one nesting depth. */
  private static final class ElementType {
    private Class<?> family;
    private NumericType numeric = NumericType.NONE;
    private ElementType nested;
    private Schema interval;
    private ElementType mapKey;
    private ElementType mapValue;
    private Schema struct;
    private Map<Schema, Boolean> structSchemas;
    private Map<Schema, Function<Object, Object>> structConversions;
  }

  private enum NumericType {
    NONE, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BIG_DECIMAL;

    /**
     * Returns the canonical Java representation for this numeric family.
     *
     * @return numeric wrapper class, or Number for the unknown family
     */
    Class<?> javaClass() {
      return switch (this) {
        case BYTE -> Byte.class;
        case SHORT -> Short.class;
        case INTEGER -> Integer.class;
        case LONG -> Long.class;
        case FLOAT -> Float.class;
        case DOUBLE -> Double.class;
        case BIG_DECIMAL -> BigDecimal.class;
        case NONE -> Number.class;
      };
    }

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
