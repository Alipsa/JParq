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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
   * and zoned leaves promote compatible local values using the SQL-session time
   * zone displacement (the active time zone's current offset). SQL and java.time
   * representations share a family; temporal leaves are always converted to the
   * JDBC representation of their family. NULLs and empty arrays do not constrain
   * the inferred leaf type. Nested reference arrays, including stored Parquet
   * array columns, are normalized to unmodifiable lists. Primitive arrays remain
   * scalar values. MAP keys and values are recursively validated and normalized
   * across sibling maps. STRUCT elements must have matching field names, order,
   * and schemas, including logical types; Avro record names are ignored.
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
    ZoneOffset offset = requiresOffset(type) ? DateTimeFunctions.sessionOffset() : null;
    return coerceArrayValues(values, type, offset);
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
        inferElementType(new ArrayList<>(map.keySet()), type.mapKey);
        inferElementType(new ArrayList<>(map.values()), type.mapValue);
      } else if (value instanceof IndexedRecord record) {
        Schema schema = record.getSchema();
        if (type.struct != null && !matchingSchemas(type.struct, schema, new HashSet<>())) {
          throw SqlExceptions.incompatibleArrayElements("STRUCT " + type.struct, "STRUCT " + schema);
        }
        type.struct = schema;
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
        coerced.add(coerceArrayValues(arrayValues(value), type.nested, offset));
      } else if (type.family == Map.class) {
        coerced.add(coerceMap((Map<?, ?>) value, type, offset));
      } else {
        coerced.add(coerceTemporal(value, type.family, offset));
      }
    }
    return Collections.unmodifiableList(coerced);
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
   *          session displacement for temporal promotion
   * @return map with normalized keys and values
   * @throws IllegalArgumentException
   *           if key promotion creates duplicate keys; the cause carries
   *           SQLState 42000
   */
  private static Map<?, ?> coerceMap(Map<?, ?> map, ElementType type, ZoneOffset offset) {
    List<Object> keys = coerceArrayValues(new ArrayList<>(map.keySet()), type.mapKey, offset);
    List<Object> values = coerceArrayValues(new ArrayList<>(map.values()), type.mapValue, offset);
    Map<Object, Object> result = new LinkedHashMap<>();
    boolean changed = false;
    int index = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object key = keys.get(index);
      Object value = values.get(index++);
      if (result.containsKey(key)) {
        throw SqlExceptions.incompatibleArrayElements("MAP keys", "duplicate keys after type promotion");
      }
      result.put(key, value);
      changed |= key != entry.getKey() || value != entry.getValue();
    }
    return changed ? Collections.unmodifiableMap(result) : map;
  }

  /**
   * Compares STRUCT field declarations recursively, ignoring Avro record names,
   * documentation, and defaults, which do not affect SQL field types.
   *
   * @param left
   *          first field schema
   * @param right
   *          second field schema
   * @param visited
   *          schema pairs already compared, to support recursive records
   * @return true when the schemas have the same field types
   */
  private static boolean matchingSchemas(Schema left, Schema right, Set<List<Schema>> visited) {
    if (left.equals(right)) {
      return true;
    }
    if (left.getType() != right.getType() || !left.getObjectProps().equals(right.getObjectProps())) {
      return false;
    }
    if (!visited.add(List.of(left, right))) {
      return true;
    }
    return switch (left.getType()) {
      case RECORD -> {
        if (left.getFields().size() != right.getFields().size()) {
          yield false;
        }
        boolean matches = true;
        for (int index = 0; index < left.getFields().size(); index++) {
          Schema.Field first = left.getFields().get(index);
          Schema.Field second = right.getFields().get(index);
          if (!first.name().equals(second.name()) || !matchingSchemas(first.schema(), second.schema(), visited)) {
            matches = false;
            break;
          }
        }
        yield matches;
      }
      case ARRAY -> matchingSchemas(left.getElementType(), right.getElementType(), visited);
      case MAP -> matchingSchemas(left.getValueType(), right.getValueType(), visited);
      case UNION -> {
        if (left.getTypes().size() != right.getTypes().size()) {
          yield false;
        }
        boolean matches = true;
        for (int index = 0; index < left.getTypes().size(); index++) {
          matches &= matchingSchemas(left.getTypes().get(index), right.getTypes().get(index), visited);
        }
        yield matches;
      }
      default -> left.equals(right);
    };
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
        case LocalDateTime dateTime -> dateTime.atOffset(offset);
        case Timestamp timestamp -> timestamp.toLocalDateTime().atOffset(offset);
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

  /** Common element family and recursive container types at one nesting depth. */
  private static final class ElementType {
    private Class<?> family;
    private NumericType numeric = NumericType.NONE;
    private ElementType nested;
    private Schema interval;
    private ElementType mapKey;
    private ElementType mapValue;
    private Schema struct;
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
