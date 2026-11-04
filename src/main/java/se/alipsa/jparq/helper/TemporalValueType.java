package se.alipsa.jparq.helper;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import org.apache.avro.Schema;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/**
 * Lossless internal schemas for java.time values crossing materialization
 * boundaries.
 */
public enum TemporalValueType {
  /** Local wall-clock time with nanosecond precision. */
  LOCAL_TIME(Types.TIME, LocalTime.class),
  /** Local date and time with nanosecond precision. */
  LOCAL_TIMESTAMP(Types.TIMESTAMP, LocalDateTime.class),
  /** Time retaining its UTC offset. */
  OFFSET_TIME(Types.TIME_WITH_TIMEZONE, OffsetTime.class),
  /** Timestamp retaining its UTC offset. */
  OFFSET_TIMESTAMP(Types.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.class);

  private static final String PROPERTY = "jparq.temporalType";
  private final int jdbcType;
  private final Class<?> javaClass;

  /**
   * Creates a temporal descriptor.
   *
   * @param jdbcType
   *          JDBC type
   * @param javaClass
   *          value class
   */
  TemporalValueType(int jdbcType, Class<?> javaClass) {
    this.jdbcType = jdbcType;
    this.javaClass = javaClass;
  }

  /**
   * Returns the JDBC type.
   *
   * @return JDBC type of this temporal value
   */
  public int jdbcType() {
    return jdbcType;
  }

  /**
   * Returns the Java value class name.
   *
   * @return fully qualified class name of decoded values
   */
  public String className() {
    return javaClass.getName();
  }

  /**
   * Creates the lossless internal schema.
   *
   * @return internal string schema retaining the temporal descriptor
   */
  public Schema schema() {
    Schema schema = Schema.create(Schema.Type.STRING);
    schema.addProp(PROPERTY, name());
    return schema;
  }

  /**
   * Resolves a temporal descriptor from an internal schema.
   *
   * @param schema
   *          nullable schema
   * @return descriptor, or null for other schemas
   */
  public static TemporalValueType fromSchema(Schema schema) {
    Schema base = JdbcTypeMapper.nonNullSchema(schema);
    String name = base == null ? null : base.getProp(PROPERTY);
    if (name == null) {
      return null;
    }
    for (TemporalValueType type : values()) {
      if (type.name().equals(name)) {
        return type;
      }
    }
    return null;
  }

  /**
   * Classifies a java.time value.
   *
   * @param value
   *          value to classify
   * @return descriptor, or null for other values
   */
  public static TemporalValueType fromValue(Object value) {
    for (TemporalValueType type : values()) {
      if (type.javaClass.isInstance(value)) {
        return type;
      }
    }
    return null;
  }

  /**
   * Normalizes compatible temporal values without copying values of this type.
   * Local values acquire the SQL session offset when an offset is required.
   *
   * @param value
   *          temporal value or ISO representation
   * @return normalized temporal value
   * @throws java.time.DateTimeException
   *           if the representation is invalid
   */
  public Object normalize(Object value) {
    if (value == null || javaClass.isInstance(value)) {
      return value;
    }
    Object local = switch (value) {
      case Time time -> DateTimeFunctions.toLocalTime(time);
      case Timestamp timestamp -> timestamp.toLocalDateTime();
      default -> value;
    };
    if ((this == LOCAL_TIME && local instanceof LocalTime)
        || (this == LOCAL_TIMESTAMP && local instanceof LocalDateTime)) {
      return local;
    }
    if (this == OFFSET_TIME && local instanceof LocalTime time) {
      return time.atOffset(DateTimeFunctions.sessionOffset());
    }
    if (this == OFFSET_TIMESTAMP && local instanceof LocalDateTime timestamp) {
      return timestamp.atOffset(DateTimeFunctions.sessionOffset());
    }
    return decode(local);
  }

  /**
   * Decodes the lossless ISO representation stored in an internal record.
   *
   * @param value
   *          encoded temporal value
   * @return value with its original precision and offset
   * @throws java.time.format.DateTimeParseException
   *           if the encoded representation is invalid
   */
  public Object decode(Object value) {
    String text = value.toString();
    return switch (this) {
      case LOCAL_TIME -> LocalTime.parse(text);
      case LOCAL_TIMESTAMP -> LocalDateTime.parse(text);
      case OFFSET_TIME -> OffsetTime.parse(text);
      case OFFSET_TIMESTAMP -> OffsetDateTime.parse(text);
    };
  }
}
