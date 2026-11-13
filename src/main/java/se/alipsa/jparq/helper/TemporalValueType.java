package se.alipsa.jparq.helper;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import org.apache.avro.Schema;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/**
 * Lossless internal schemas for temporal values crossing materialization
 * boundaries.
 */
public enum TemporalValueType {
  /** Local wall-clock time with nanosecond precision. */
  LOCAL_TIME(Types.TIME, LocalTime.class),
  /** Local date and time with nanosecond precision. */
  LOCAL_TIMESTAMP(Types.TIMESTAMP, LocalDateTime.class),
  /** SQL TIMESTAMP WITHOUT TIME ZONE retaining the JDBC Timestamp value class. */
  JDBC_LOCAL_TIMESTAMP(Types.TIMESTAMP, Timestamp.class),
  /**
   * UTC-adjusted TIMESTAMP preserving instants and nanoseconds across time-zone
   * overlaps.
   */
  UTC_TIMESTAMP(Types.TIMESTAMP, Timestamp.class),
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
   * Tests whether a value already has this type's Java representation, in which
   * case {@link #normalize(Object)} returns it unchanged.
   *
   * @param value
   *          value to test, possibly null
   * @return whether the value needs no conversion
   */
  public boolean isInstance(Object value) {
    return value != null && javaClass.isInstance(value);
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
   * Identifies offset datetime declarations, including nested ARRAY leaves.
   *
   * @param schema
   *          nullable scalar or ARRAY declaration
   * @return whether the leaf type carries a time zone
   */
  public static boolean hasTimeZone(Schema schema) {
    Schema leaf = JdbcTypeMapper.nonNullSchema(schema);
    while (leaf != null && leaf.getType() == Schema.Type.ARRAY) {
      leaf = JdbcTypeMapper.nonNullSchema(leaf.getElementType());
    }
    int type = JdbcTypeMapper.mapSchemaToJdbcType(leaf);
    return type == Types.TIME_WITH_TIMEZONE || type == Types.TIMESTAMP_WITH_TIMEZONE;
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
   * Classifies a java.time value. JDBC timestamps retain their existing
   * value-based inference; UTC_TIMESTAMP is selected from declared schemas.
   *
   * @param value
   *          value to classify
   * @return descriptor, or null for other values
   */
  public static TemporalValueType fromValue(Object value) {
    for (TemporalValueType type : values()) {
      if (type != UTC_TIMESTAMP && type != JDBC_LOCAL_TIMESTAMP && type.javaClass.isInstance(value)) {
        return type;
      }
    }
    return null;
  }

  /**
   * Normalizes compatible temporal values without copying values of this type.
   * Local timestamps use their date's zone rules; date-free times use the
   * execution's current displacement when an offset is required.
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
    if (this == UTC_TIMESTAMP) {
      return switch (value) {
        case Instant instant -> Timestamp.from(instant);
        case OffsetDateTime timestamp -> Timestamp.from(timestamp.toInstant());
        case LocalDateTime timestamp -> Timestamp.valueOf(timestamp);
        default -> decode(value);
      };
    }
    if (this == JDBC_LOCAL_TIMESTAMP) {
      return value instanceof LocalDateTime timestamp ? Timestamp.valueOf(timestamp) : decode(value);
    }
    if (this == OFFSET_TIMESTAMP && value instanceof Timestamp timestamp) {
      // Promote by the retained instant, not the wall-clock fields, so the two
      // instants sharing a fall-back overlap stay distinct.
      return timestamp.toInstant().atZone(DateTimeFunctions.defaultZone()).toOffsetDateTime();
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
      return DateTimeFunctions.offsetTimestamp(timestamp);
    }
    return decode(local);
  }

  /**
   * Encodes a value without losing the instant of a UTC-adjusted JDBC timestamp.
   *
   * @param value
   *          temporal value or ISO representation, possibly null
   * @return lossless ISO text, or null for a null value
   * @throws java.time.DateTimeException
   *           if the input representation is invalid
   */
  public String encode(Object value) {
    Object normalized = normalize(value);
    if (normalized == null) {
      return null;
    }
    return switch (this) {
      case UTC_TIMESTAMP -> ((Timestamp) normalized).toInstant().toString();
      case JDBC_LOCAL_TIMESTAMP -> ((Timestamp) normalized).toLocalDateTime().toString();
      default -> normalized.toString();
    };
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
      case JDBC_LOCAL_TIMESTAMP ->
        value instanceof Timestamp ? value : Timestamp.valueOf(LocalDateTime.parse(text.replace(' ', 'T')));
      case UTC_TIMESTAMP -> value instanceof Timestamp ? value : Timestamp.from(Instant.parse(text));
      case OFFSET_TIME -> OffsetTime.parse(text);
      case OFFSET_TIMESTAMP -> OffsetDateTime.parse(text);
    };
  }
}
