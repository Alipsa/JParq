package se.alipsa.jparq.helper;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/**
 * Utility methods for mapping Avro schemas to JDBC metadata representations.
 */
public final class JdbcTypeMapper {

  private JdbcTypeMapper() {
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
   * Determine whether the supplied schema represents a nullable value.
   *
   * @param schema
   *          the schema describing the value
   * @return {@code true} if the schema allows {@code null} values, otherwise
   *         {@code false}
   */
  public static boolean isNullable(Schema schema) {
    return schema != null && schema.getType() == Schema.Type.UNION
        && schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
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
    return switch (base.getType()) {
      case STRING, ENUM -> Types.VARCHAR;
      case INT -> (LogicalTypes.date().equals(base.getLogicalType()) ? Types.DATE
          : base.getLogicalType() instanceof LogicalTypes.TimeMillis ? Types.TIME : Types.INTEGER);
      case LONG -> {
        if (base.getLogicalType() instanceof LogicalTypes.TimestampMillis
            || base.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
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
        if (base.getLogicalType() instanceof LogicalTypes.Decimal) {
          yield Types.DECIMAL;
        }
        yield isUtf8String(base) ? Types.VARCHAR : Types.BINARY;
      }
      case RECORD -> Types.STRUCT;
      case ARRAY -> Types.ARRAY;
      case MAP -> Types.STRUCT;
      default -> Types.OTHER;
    };
  }

  /**
   * Map an Avro schema to the fully qualified Java class name representing
   * values of that schema.
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
        if (base.getLogicalType() instanceof LogicalTypes.TimestampMillis
            || base.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
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
      case BYTES, FIXED -> (base.getLogicalType() instanceof LogicalTypes.Decimal) ? BigDecimal.class.getName()
          : byte[].class.getName();
      case RECORD -> Map.class.getName();
      case ARRAY -> List.class.getName();
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
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> byte[].class.getName();
      case Types.ARRAY -> List.class.getName();
      case Types.CLOB, Types.NCLOB -> java.sql.Clob.class.getName();
      case Types.BLOB -> java.sql.Blob.class.getName();
      case Types.STRUCT -> Map.class.getName();
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

