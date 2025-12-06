package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

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
   */
  @SuppressWarnings("PMD.EmptyCatchBlock")
  public static Object coerceLiteral(Object lit, Schema s) {
    if (lit == null) {
      return null;
    }
    Schema effective = effectiveSchema(s);
    switch (effective.getType()) {
      case STRING, ENUM:
        return lit.toString();

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
          return lit.toString();
        }
        try {
          return Integer.parseInt(lit.toString());
        } catch (Exception ignore) {
          return lit;
        }
      }

      case LONG: {
        if (effective.getLogicalType() instanceof LogicalTypes.TimestampMillis
            || effective.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
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
          return lit.toString();
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
        return Boolean.parseBoolean(lit.toString());

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
    return unwrap(record.get(lookup), schema);
  }

  /**
   * Unwraps an Avro value to a corresponding Java type.
   *
   * @param v
   *          The Avro value to unwrap.
   * @param s
   *          The Avro schema for the value.
   * @return The unwrapped Java object.
   */
  public static Object unwrap(Object v, Schema s) {
    if (v == null) {
      return null;
    }
    Schema effective = effectiveSchema(s);
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
        if (LogicalTypes.date().equals(effective.getLogicalType())) {
          int days = (Integer) v;
          return Date.valueOf(LocalDate.ofEpochDay(days));
        }
        return ((Number) v).intValue();
      case LONG:
        if (effective.getLogicalType() instanceof LogicalTypes.TimestampMillis
            || effective.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
          long epoch = ((Number) v).longValue();
          if (effective.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
            epoch /= 1000L;
          }
          return Timestamp.from(Instant.ofEpochMilli(epoch));
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
        return v.toString();
      case ENUM:
        return v.toString();
      case ARRAY:
        if (v instanceof GenericData.Array) {
          return ((GenericData.Array<?>) v).toArray();
        }
        return v;
      case FIXED:
        if (effective.getLogicalType() instanceof LogicalTypes.Decimal) {
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
