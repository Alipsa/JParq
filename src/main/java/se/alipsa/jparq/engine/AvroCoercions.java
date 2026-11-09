package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalValueType;

/**
 * Utility class for coercing Avro values to Java types and vice versa.
 *
 * <p>
 * This is a minimal implementation focused on literals and unwrapping Avro
 * values for JDBC result sets.
 */
public final class AvroCoercions {

  private AvroCoercions() {
  }

  /**
   * Tests whether a schema represents any supported timestamp precision.
   * 
   * @param schema
   *          the non-null schema
   * @return whether it has a timestamp logical type
   */
  public static boolean isTimestamp(Schema schema) {
    String name = schema.getProp("logicalType");
    return name != null && (name.startsWith("timestamp-") || name.startsWith("local-timestamp-"));
  }

  /**
   * Tests whether timestamps store wall-clock values without a time zone.
   * 
   * @param schema
   *          the non-null schema
   * @return whether timestamps are local
   */
  public static boolean isLocalTimestamp(Schema schema) {
    String name = schema.getProp("logicalType");
    return name != null && name.startsWith("local-timestamp-");
  }

  /**
   * Returns the physical timestamp units per second.
   * 
   * @param schema
   *          the timestamp schema
   * @return 1000, 1000000, or 1000000000
   */
  public static long timestampUnits(Schema schema) {
    String name = schema.getProp("logicalType");
    return name.endsWith("nanos") ? 1_000_000_000L : name.endsWith("micros") ? 1_000_000L : 1000L;
  }

  /**
   * Decodes timestamp epoch units, including negative fractional epochs. Local
   * timestamp units use the same UTC origin to represent wall-clock fields.
   *
   * @param epoch
   *          stored timestamp units
   * @param schema
   *          non-null timestamp declaration
   * @return instant representing the stored units
   */
  public static Instant timestampInstant(long epoch, Schema schema) {
    long units = timestampUnits(schema);
    return Instant.ofEpochSecond(Math.floorDiv(epoch, units), Math.floorMod(epoch, units) * (1_000_000_000L / units));
  }

  /**
   * Identifies Parquet half-precision floating point values.
   * 
   * @param schema
   *          the non-null schema
   * @return whether the physical fixed bytes encode float16
   */
  public static boolean isFloat16(Schema schema) {
    return "float16".equals(schema.getProp("jparq.logicalType"));
  }

  /**
   * Collapse nullable unions to their non-null branch, else return input.
   *
   * @param s
   *          the schema
   * @return effective schema
   */
  static Schema effectiveSchema(Schema s) {
    if (s.getType() == Schema.Type.UNION) {
      for (Schema t : s.getTypes()) {
        if (t.getType() != Schema.Type.NULL) {
          return t;
        }
      }
    }
    return s;
  }

  /**
   * Coerces a literal value to match the target schema type where possible.
   *
   * @param lit
   *          the literal value
   * @param s
   *          the target schema
   * @return coerced value
   * @throws IllegalArgumentException
   *           if an interval would be implicitly converted to text, including a
   *           temporal parsing fallback, carrying SQLState 42000
   */
  @SuppressWarnings("PMD.EmptyCatchBlock")
  public static Object coerceLiteral(Object lit, Schema s) {
    if (lit == null) {
      return null;
    }
    Schema effective = effectiveSchema(s);
    if (lit instanceof byte[] || lit instanceof ByteBuffer) {
      int targetType = JdbcTypeMapper.mapSchemaToJdbcType(effective);
      if (targetType != java.sql.Types.OTHER && targetType != java.sql.Types.NULL
          && !JdbcTypeMapper.isBinaryType(targetType)) {
        throw SqlExceptions.syntaxFailure("Cannot compare binary and nonbinary operands");
      }
    }
    if (IntervalType.matches(effective)) {
      return lit;
    }
    if (effective.getLogicalType() instanceof LogicalTypes.Decimal) {
      try {
        return new BigDecimal(lit.toString());
      } catch (NumberFormatException exception) {
        return lit;
      }
    }
    switch (effective.getType()) {
      case STRING, ENUM:
        return SqlText.valueOf(lit);

      case INT: {
        if (LogicalTypes.date().equals(effective.getLogicalType())) {
          if (lit instanceof Date) {
            return lit;
          }
          try {
            return Date.valueOf(lit.toString());
          } catch (Exception ignore) {
          }
          try {
            return Integer.parseInt(lit.toString());
          } catch (Exception ignore) {
          }
          return SqlText.valueOf(lit);
        }
        try {
          return Integer.parseInt(lit.toString());
        } catch (Exception ignore) {
          return lit;
        }
      }

      case LONG: {
        if (isTimestamp(effective)) {
          if (lit instanceof Timestamp) {
            return lit;
          }
          try {
            return Timestamp.valueOf(lit.toString());
          } catch (Exception ignore) {
          }
          try {
            return new Timestamp(Long.parseLong(lit.toString()));
          } catch (Exception ignore) {
          }
          return SqlText.valueOf(lit);
        }
        try {
          return Long.parseLong(lit.toString());
        } catch (Exception ignore) {
          return lit;
        }
      }

      case FLOAT, DOUBLE:
        try {
          return new BigDecimal(lit.toString());
        } catch (Exception ignore) {
          return lit;
        }

      case BOOLEAN:
        if (lit instanceof Boolean) {
          return lit;
        }
        if ("true".equalsIgnoreCase(lit.toString()) || "false".equalsIgnoreCase(lit.toString())) {
          return Boolean.parseBoolean(lit.toString());
        }
        return lit;

      default:
        return lit; // bytes/arrays/etc. not coerced for WHERE in this minimal impl
    }
  }

  /**
   * Resolves a column value from a GenericRecord using case-insensitive lookup.
   *
   * @param columnName
   *          the name of the column to look up
   * @param record
   *          the GenericRecord to retrieve the value from (may be {@code null})
   * @param fieldSchemas
   *          map from exact field name to schema
   * @param caseInsensitiveIndex
   *          map from normalized field name to canonical field name
   * @return the unwrapped column value, or {@code null} if not found
   */
  public static Object resolveColumnValue(String columnName, GenericRecord record, Map<String, Schema> fieldSchemas,
      Map<String, String> caseInsensitiveIndex) {
    return resolveColumnValue(columnName, record, fieldSchemas, caseInsensitiveIndex, AvroCoercions::unwrap);
  }

  /**
   * Resolves a field with the normal identifier lookup and a caller's decoder.
   *
   * @param columnName
   *          field reference
   * @param record
   *          input row, possibly null
   * @param fieldSchemas
   *          schemas indexed by exact field names
   * @param caseInsensitiveIndex
   *          normalized names mapped to exact names
   * @param decoder
   *          schema-aware field decoder
   * @return decoded value, or null when the row or field is missing
   */
  public static Object resolveColumnValue(String columnName, GenericRecord record, Map<String, Schema> fieldSchemas,
      Map<String, String> caseInsensitiveIndex, BiFunction<Object, Schema, Object> decoder) {
    if (columnName == null || record == null) {
      return null;
    }
    String lookup = columnName;
    Schema schema = fieldSchemas.get(columnName);
    if (schema == null) {
      String canonical = caseInsensitiveIndex.get(Identifier.lookupKey(columnName));
      if (canonical != null) {
        lookup = canonical;
        schema = fieldSchemas.get(canonical);
      }
    }
    if (schema == null) {
      return null;
    }
    return decoder.apply(record.get(lookup), schema);
  }

  /**
   * Decodes local timestamp fields without the JVM-zone normalization of JDBC
   * Timestamp. Arrays preserve these local fields recursively; other types keep
   * their normal JDBC decoding.
   *
   * @param value
   *          raw field value
   * @param schema
   *          declared field schema
   * @return decoded value with lossless local timestamp fields
   */
  public static Object unwrapLocalFields(Object value, Schema schema) {
    if (value == null) {
      return null;
    }
    Schema effective = effectiveSchema(schema);
    if (isLocalTimestamp(effective) && value instanceof Number number) {
      return LocalDateTime.ofInstant(timestampInstant(number.longValue(), effective), ZoneOffset.UTC);
    }
    if (effective.getType() == Schema.Type.ARRAY) {
      return unwrapArray(value, effective.getElementType(), true);
    }
    return unwrap(value, schema);
  }

  /**
   * Unwraps an Avro value to a corresponding Java type. Record values retain
   * their Avro schema and raw field values as GenericRecord objects rather than
   * JSON text; record fields are not recursively unwrapped. TIME logical types
   * become JDBC Time values with millisecond precision, matching typed TIME
   * literals; sub-millisecond digits are truncated.
   *
   * @param v
   *          The Avro value to unwrap.
   * @param s
   *          The Avro schema for the value.
   * @return The unwrapped Java object.
   * @throws IllegalArgumentException
   *           if a stored TIME is outside its clock range, carrying SQLState
   *           22008
   */
  public static Object unwrap(Object v, Schema s) {
    if (v == null) {
      return null;
    }
    Schema effective = effectiveSchema(s);
    TemporalValueType temporal = TemporalValueType.fromSchema(effective);
    if (temporal != null) {
      return temporal.decode(v);
    }
    if (IntervalType.matches(effective)) {
      return IntervalType.decode(v, effective);
    }
    switch (effective.getType()) {
      case STRING:
        if (v instanceof CharSequence) {
          return v.toString();
        }
        if (v instanceof ByteBuffer byteBuffer) {
          ByteBuffer duplicate = byteBuffer.duplicate();
          byte[] bytes = new byte[duplicate.remaining()];
          duplicate.get(bytes);
          return new String(bytes, StandardCharsets.UTF_8);
        }
        if (v instanceof byte[] byteArray) {
          return new String(byteArray, StandardCharsets.UTF_8);
        }
        return v.toString();
      case INT:
        if (effective.getLogicalType() instanceof LogicalTypes.TimeMillis) {
          return unwrapTime(v, 1000L);
        }
        if (LogicalTypes.date().equals(effective.getLogicalType())) {
          if (v instanceof Date) {
            return v;
          }
          int days = (Integer) v;
          return Date.valueOf(LocalDate.ofEpochDay(days));
        }
        return ((Number) v).intValue();
      case LONG:
        if (effective.getLogicalType() instanceof LogicalTypes.TimeMicros) {
          return unwrapTime(v, 1_000_000L);
        }
        if (isTimestamp(effective)) {
          if (v instanceof Timestamp) {
            return v;
          }
          Instant instant = timestampInstant(((Number) v).longValue(), effective);
          return isLocalTimestamp(effective)
              ? Timestamp.valueOf(LocalDateTime.ofInstant(instant, ZoneOffset.UTC))
              : Timestamp.from(instant);
        }
        return ((Number) v).longValue();
      case FLOAT:
        return ((Number) v).floatValue();
      case DOUBLE:
        return ((Number) v).doubleValue();
      case BOOLEAN:
        return v;
      case BYTES:
        boolean preferBinary = isBinaryPreferred(effective);
        if (effective.getLogicalType() instanceof LogicalTypes.Decimal dec) {
          if (v instanceof BigDecimal) {
            return v;
          }
          ByteBuffer bb = ((ByteBuffer) v).duplicate();
          byte[] bytes = new byte[bb.remaining()];
          bb.get(bytes);
          java.math.BigInteger bi = new java.math.BigInteger(bytes);
          return new BigDecimal(bi, dec.getScale());
        }
        if (v instanceof ByteBuffer byteBuffer) {
          byte[] bytes = copyBytes(byteBuffer);
          if (!preferBinary && (hasUtf8Semantic(effective) || isUtf8Text(bytes))) {
            return new String(bytes, StandardCharsets.UTF_8);
          }
          return bytes;
        }
        if (v instanceof byte[] bytes) {
          if (!preferBinary && (hasUtf8Semantic(effective) || isUtf8Text(bytes))) {
            return new String(bytes, StandardCharsets.UTF_8);
          }
          return bytes.clone();
        }
        return v;
      case RECORD:
        return v;
      case ENUM:
        return v.toString();
      case ARRAY:
        return unwrapArray(v, effective.getElementType());
      case FIXED:
        if (isFloat16(effective) && v instanceof GenericData.Fixed fixed) {
          byte[] bytes = fixed.bytes();
          return Float.float16ToFloat((short) ((bytes[0] & 255) | (bytes[1] & 255) << 8));
        }
        if (effective.getLogicalType() instanceof LogicalTypes.Decimal) {
          if (v instanceof BigDecimal) {
            return v;
          }
          LogicalTypes.Decimal dec = (LogicalTypes.Decimal) effective.getLogicalType();
          byte[] bytes = ((GenericData.Fixed) v).bytes();
          java.math.BigInteger bi = new java.math.BigInteger(bytes);
          return new BigDecimal(bi, dec.getScale());
        }
        if (v instanceof GenericData.Fixed fixed) {
          return fixed.bytes().clone();
        }
        return v;
      default:
        return v;
    }
  }

  /**
   * Decodes scalar array elements before JDBC projection or correlation binding.
   * Records remain available to structured consumers such as UNNEST. Previously
   * decoded logical values are retained when materialized arrays are read again.
   *
   * @param value
   *          Avro array, collection, or object array
   * @param elementSchema
   *          schema shared by the array elements
   * @return a decoded object array, or the original unsupported container
   */
  private static Object unwrapArray(Object value, Schema elementSchema) {
    return unwrapArray(value, elementSchema, false);
  }

  /**
   * Decodes array elements with the requested local timestamp representation.
   *
   * @param value
   *          stored array or decoded array
   * @param elementSchema
   *          element declaration
   * @param localFields
   *          whether to retain local timestamp fields without JDBC normalization
   * @return decoded elements, or the original unsupported container
   */
  private static Object unwrapArray(Object value, Schema elementSchema, boolean localFields) {
    Object[] elements;
    if (value instanceof Collection<?> collection) {
      elements = collection.toArray();
    } else if (value instanceof Object[] objects) {
      elements = Arrays.copyOf(objects, objects.length, Object[].class);
    } else {
      return value;
    }
    Schema element = effectiveSchema(elementSchema);
    for (int index = 0; index < elements.length; index++) {
      Object item = elements[index];
      if (item == null || element.getType() == Schema.Type.RECORD) {
        continue;
      }
      elements[index] = localFields ? unwrapLocalFields(item, elementSchema) : unwrap(item, elementSchema);
    }
    return elements;
  }

  /**
   * Create a defensive copy of the bytes remaining in the supplied
   * {@link ByteBuffer} without mutating its position.
   *
   * @param buffer
   *          source buffer
   * @return copied byte array
   */
  private static byte[] copyBytes(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }

  private static boolean isBinaryPreferred(Schema schema) {
    if (schema == null) {
      return false;
    }
    Object objectProp = schema.getObjectProp("jparq.binary");
    if (objectProp instanceof Boolean b && b) {
      return true;
    }
    if (objectProp != null && Boolean.parseBoolean(objectProp.toString())) {
      return true;
    }
    String prop = schema.getProp("jparq.binary");
    return prop != null && Boolean.parseBoolean(prop);
  }

  /**
   * Decodes and validates a stored time before scaling its clock units.
   *
   * @param value
   *          stored numeric clock value or a decoded JDBC time
   * @param unitsPerSecond
   *          number of stored units per second
   * @return JDBC time retaining milliseconds
   * @throws IllegalArgumentException
   *           if the value is outside one day (22008)
   */
  private static Time unwrapTime(Object value, long unitsPerSecond) {
    if (value instanceof Time time) {
      return DateTimeFunctions.toTime(time);
    }
    long units = ((Number) value).longValue();
    if (units < 0 || units >= 86_400L * unitsPerSecond) {
      String message = "Stored TIME value out of range: " + units;
      throw new IllegalArgumentException(message, new SQLDataException(message, "22008"));
    }
    return DateTimeFunctions.toTime(LocalTime.ofNanoOfDay(units * (1_000_000_000L / unitsPerSecond)));
  }

  private static boolean isUtf8Text(byte[] bytes) {
    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    decoder.onMalformedInput(CodingErrorAction.REPORT);
    decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    try {
      decoder.decode(ByteBuffer.wrap(bytes));
      return true;
    } catch (CharacterCodingException e) {
      return false;
    }
  }

  private static boolean hasUtf8Semantic(Schema schema) {
    if (schema == null) {
      return false;
    }
    if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
      return false;
    }
    String logicalType = schema.getProp("logicalType");
    if (logicalType != null && "string".equalsIgnoreCase(logicalType)) {
      return true;
    }
    String originalType = schema.getProp("originalType");
    if (originalType != null && "UTF8".equalsIgnoreCase(originalType)) {
      return true;
    }
    String convertedType = schema.getProp("convertedType");
    if (convertedType != null && "UTF8".equalsIgnoreCase(convertedType)) {
      return true;
    }
    Object javaString = schema.getObjectProp("avro.java.string");
    return javaString != null;
  }
}
