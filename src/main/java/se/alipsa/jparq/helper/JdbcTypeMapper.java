package se.alipsa.jparq.helper;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import se.alipsa.jparq.engine.AvroCoercions;

/**
 * Utility methods for mapping Avro schemas to JDBC metadata representations.
 */
public final class JdbcTypeMapper {
  private static final String DECLARED_JDBC_TYPE = "jparq.jdbcType";

  private JdbcTypeMapper() {
  }

  /**
   * Identifies the fixed and varying character types retained by value schemas.
   *
   * @param type
   *          JDBC type
   * @return whether the type is CHAR, VARCHAR, NCHAR, or NVARCHAR
   */
  public static boolean isCharacterType(int type) {
    return type == Types.CHAR || type == Types.VARCHAR || type == Types.NCHAR || type == Types.NVARCHAR;
  }

  /**
   * Finds the common fixed or varying character type of two character inputs.
   *
   * @param left
   *          first JDBC character type
   * @param right
   *          second JDBC character type
   * @return common type retaining national and varying declarations
   */
  public static JDBCType commonCharacterType(int left, int right) {
    boolean national = left == Types.NCHAR || left == Types.NVARCHAR || right == Types.NCHAR || right == Types.NVARCHAR;
    boolean varying = left == Types.VARCHAR || left == Types.NVARCHAR || right == Types.VARCHAR
        || right == Types.NVARCHAR;
    return national ? varying ? JDBCType.NVARCHAR : JDBCType.NCHAR : varying ? JDBCType.VARCHAR : JDBCType.CHAR;
  }

  /**
   * Identifies binary string declarations, including large objects.
   *
   * @param type
   *          JDBC type
   * @return whether the type is BINARY, VARBINARY, or BLOB
   */
  public static boolean isBinaryType(int type) {
    return type == Types.BINARY || type == Types.VARBINARY || type == Types.BLOB;
  }

  /**
   * Finds the common binary type, preferring large objects and varying strings.
   *
   * @param left
   *          first JDBC binary type
   * @param right
   *          second JDBC binary type
   * @return common binary type
   */
  public static JDBCType commonBinaryType(int left, int right) {
    return left == Types.BLOB || right == Types.BLOB
        ? JDBCType.BLOB
        : left == Types.VARBINARY || right == Types.VARBINARY ? JDBCType.VARBINARY : JDBCType.BINARY;
  }

  /**
   * Retains a declared character or binary JDBC type on its Avro storage schema.
   *
   * @param type
   *          character or binary SQL type
   * @return value schema retaining fixed or varying type information
   * @throws IllegalArgumentException
   *           if the type is not supported by this mapping
   */
  public static Schema declaredSchema(JDBCType type) {
    Schema.Type storage = switch (type) {
      case CHAR, VARCHAR, NCHAR, NVARCHAR -> Schema.Type.STRING;
      case BINARY, VARBINARY, BLOB -> Schema.Type.BYTES;
      default -> throw new IllegalArgumentException("Not a character or binary type: " + type);
    };
    Schema schema = Schema.create(storage);
    schema.addProp(DECLARED_JDBC_TYPE, type.getName());
    if (storage == Schema.Type.BYTES) {
      schema.addProp("jparq.binary", true);
    }
    return schema;
  }

  /**
   * Reads optional character and binary type declarations.
   *
   * @param schema
   *          value schema
   * @param fallback
   *          JDBC type for unmarked schemas
   * @return declared JDBC type or the fallback
   */
  private static int declaredJdbcType(Schema schema, int fallback) {
    String declaration = schema.getProp(DECLARED_JDBC_TYPE);
    if (declaration == null) {
      return fallback;
    }
    return switch (declaration) {
      case "CHAR" -> Types.CHAR;
      case "NCHAR" -> Types.NCHAR;
      case "NVARCHAR" -> Types.NVARCHAR;
      case "VARBINARY" -> Types.VARBINARY;
      case "BLOB" -> Types.BLOB;
      default -> fallback;
    };
  }

  /**
   * Resolve the non-null portion of an Avro schema, unwrapping optional unions
   * when present.
   *
   * @param schema
   *          the schema to examine (may be {@code null})
   * @return the non-null schema or {@code null} when it cannot be resolved
   */
  public static Schema nonNullSchema(Schema schema) {
    if (schema == null || schema.getType() != Schema.Type.UNION) {
      return schema;
    }
    return schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst().orElse(schema);
  }

  /**
   * Resolves the innermost element type of a possibly nested ARRAY declaration,
   * returning the schema itself when it is not an ARRAY.
   *
   * @param schema
   *          array or leaf declaration (may be {@code null})
   * @return the innermost non-null element type, or {@code null} when the input
   *         is null
   */
  public static Schema innermostElementType(Schema schema) {
    Schema element = nonNullSchema(schema);
    while (element != null && element.getType() == Schema.Type.ARRAY) {
      element = nonNullSchema(element.getElementType());
    }
    return element;
  }

  /**
   * Determine whether the supplied schema represents a nullable value.
   *
   * @param schema
   *          the schema describing the value
   * @return {@code true} if the schema allows {@code null} values, otherwise
   *         {@code false}
   */
  public static boolean isNullable(Schema schema) {
    return schema != null && (schema.getType() == Schema.Type.NULL || schema.getType() == Schema.Type.UNION
        && schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL));
  }

  /**
   * Map an Avro schema to the corresponding JDBC {@link Types} constant.
   *
   * @param schema
   *          the schema describing the value
   * @return the matching JDBC type constant
   */
  public static int mapSchemaToJdbcType(Schema schema) {
    Schema base = nonNullSchema(schema);
    if (base == null) {
      return Types.OTHER;
    }
    TemporalValueType temporal = TemporalValueType.fromSchema(base);
    if (temporal != null) {
      return temporal.jdbcType();
    }
    if (IntervalType.matches(base)) {
      return Types.OTHER;
    }
    return switch (base.getType()) {
      case STRING, ENUM -> declaredJdbcType(base, Types.VARCHAR);
      case INT -> (LogicalTypes.date().equals(base.getLogicalType())
          ? Types.DATE
          : base.getLogicalType() instanceof LogicalTypes.TimeMillis ? Types.TIME : Types.INTEGER);
      case LONG -> {
        if (AvroCoercions.isTimestamp(base)) {
          yield Types.TIMESTAMP;
        }
        if (base.getLogicalType() instanceof LogicalTypes.TimeMicros) {
          yield Types.TIME;
        }
        yield Types.BIGINT;
      }
      case FLOAT -> Types.REAL;
      case DOUBLE -> Types.DOUBLE;
      case BOOLEAN -> Types.BOOLEAN;
      case BYTES, FIXED -> {
        if (AvroCoercions.isFloat16(base)) {
          yield Types.REAL;
        }
        if (base.getLogicalType() instanceof LogicalTypes.Decimal) {
          yield Types.DECIMAL;
        }
        yield isUtf8String(base) ? Types.VARCHAR : declaredJdbcType(base, Types.BINARY);
      }
      case NULL -> Types.NULL;
      case RECORD -> Types.STRUCT;
      case ARRAY -> Types.ARRAY;
      case MAP -> Types.STRUCT;
      default -> Types.OTHER;
    };
  }

  /**
   * Map an Avro schema to the fully qualified Java class name representing values
   * of that schema.
   *
   * @param schema
   *          the schema describing the value
   * @return the corresponding Java class name
   */
  public static String mapSchemaToJavaClassName(Schema schema) {
    Schema base = nonNullSchema(schema);
    if (base == null) {
      return Object.class.getName();
    }
    TemporalValueType temporal = TemporalValueType.fromSchema(base);
    if (temporal != null) {
      return temporal.className();
    }
    if (IntervalType.matches(base)) {
      return TemporalInterval.class.getName();
    }
    return switch (base.getType()) {
      case STRING, ENUM -> String.class.getName();
      case INT -> {
        if (LogicalTypes.date().equals(base.getLogicalType())) {
          yield Date.class.getName();
        }
        if (base.getLogicalType() instanceof LogicalTypes.TimeMillis) {
          yield Time.class.getName();
        }
        yield Integer.class.getName();
      }
      case LONG -> {
        if (AvroCoercions.isTimestamp(base)) {
          yield Timestamp.class.getName();
        }
        if (base.getLogicalType() instanceof LogicalTypes.TimeMicros) {
          yield Time.class.getName();
        }
        yield Long.class.getName();
      }
      case FLOAT -> Float.class.getName();
      case DOUBLE -> Double.class.getName();
      case BOOLEAN -> Boolean.class.getName();
      case BYTES,
          FIXED ->
        AvroCoercions.isFloat16(base)
            ? Float.class.getName()
            : (base.getLogicalType() instanceof LogicalTypes.Decimal)
                ? BigDecimal.class.getName()
                : byte[].class.getName();
      case RECORD -> org.apache.avro.generic.GenericRecord.class.getName();
      case ARRAY -> Object[].class.getName();
      case MAP -> Map.class.getName();
      default -> Object.class.getName();
    };
  }

  /**
   * Map a JDBC {@link Types} constant to the Java class name typically used to
   * represent the value.
   *
   * @param jdbcType
   *          the JDBC type constant
   * @return the fully qualified Java class name
   */
  public static String mapJdbcTypeToClassName(int jdbcType) {
    return switch (jdbcType) {
      case Types.BIT, Types.BOOLEAN -> Boolean.class.getName();
      case Types.TINYINT -> Byte.class.getName();
      case Types.SMALLINT -> Short.class.getName();
      case Types.INTEGER -> Integer.class.getName();
      case Types.BIGINT -> Long.class.getName();
      case Types.REAL -> Float.class.getName();
      case Types.FLOAT, Types.DOUBLE -> Double.class.getName();
      case Types.NUMERIC, Types.DECIMAL -> BigDecimal.class.getName();
      case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR ->
        String.class.getName();
      case Types.DATE -> Date.class.getName();
      case Types.TIME, Types.TIME_WITH_TIMEZONE -> Time.class.getName();
      case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> Timestamp.class.getName();
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> byte[].class.getName();
      case Types.ARRAY -> Object[].class.getName();
      case Types.CLOB, Types.NCLOB -> java.sql.Clob.class.getName();
      case Types.STRUCT -> org.apache.avro.generic.GenericRecord.class.getName();
      default -> Object.class.getName();
    };
  }

  /**
   * Map a fully qualified Java class name to the corresponding JDBC type.
   *
   * @param className
   *          the Java class name describing the values
   * @return the matching {@link Types} constant, or {@link Types#OTHER} when the
   *         class is not recognised
   */
  public static int mapJavaClassNameToJdbcType(String className) {
    if (className == null || className.isBlank()) {
      return Types.OTHER;
    }
    return switch (className) {
      case "java.lang.Boolean" -> Types.BOOLEAN;
      case "java.lang.Byte" -> Types.TINYINT;
      case "java.lang.Short" -> Types.SMALLINT;
      case "java.lang.Integer" -> Types.INTEGER;
      case "java.lang.Long" -> Types.BIGINT;
      case "java.lang.Float" -> Types.REAL;
      case "java.lang.Double" -> Types.DOUBLE;
      case "java.math.BigDecimal" -> Types.DECIMAL;
      case "java.lang.String", "org.apache.avro.util.Utf8" -> Types.VARCHAR;
      case "java.sql.Date" -> Types.DATE;
      case "java.sql.Time" -> Types.TIME;
      case "java.sql.Timestamp" -> Types.TIMESTAMP;
      case "[B", "byte[]" -> Types.VARBINARY;
      case "java.util.List", "java.util.Collection" -> Types.ARRAY;
      case "[Ljava.lang.Object;", "java.lang.Object[]" -> Types.ARRAY;
      case "java.util.Map" -> Types.STRUCT;
      default -> Types.OTHER;
    };
  }

  private static boolean isUtf8String(Schema schema) {
    if (schema == null) {
      return false;
    }
    if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
      return false;
    }
    String logicalType = schema.getProp("logicalType");
    if (logicalType != null && "decimal".equalsIgnoreCase(logicalType)) {
      return false;
    }
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
