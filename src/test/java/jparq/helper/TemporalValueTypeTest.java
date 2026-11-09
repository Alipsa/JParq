package jparq.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalValueType;

/** Verifies lossless temporal encoding independently of the system clock. */
class TemporalValueTypeTest {

  /**
   * Checks nanosecond and non-hour offset preservation through nullable Avro
   * schemas.
   */
  @Test
  void roundTripsTemporalValues() {
    for (Object value : List.of(LocalTime.parse("12:34:56.123456789"),
        LocalDateTime.parse("1960-02-03T12:34:56.123456789"), OffsetTime.parse("12:34:56.123456789-03:30"),
        OffsetDateTime.parse("1960-02-03T12:34:56.123456789+05:45"))) {
      var type = TemporalValueType.fromValue(value);
      assertSame(value, type.normalize(value));
      var schema = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), type.schema()));
      assertEquals(type, TemporalValueType.fromSchema(schema));
      assertEquals(value, AvroCoercions.unwrap(new Utf8(type.encode(value)), schema));
      assertEquals(value.getClass().getName(), JdbcTypeMapper.mapSchemaToJavaClassName(schema));
      assertEquals(type.jdbcType(), JdbcTypeMapper.mapSchemaToJdbcType(schema));
      assertNull(AvroCoercions.unwrap(null, schema));
    }
  }

  /**
   * Keeps UTC-adjusted timestamps lossless while leaving ordinary JDBC value
   * inference unchanged.
   */
  @Test
  void roundTripsUtcTimestampInstants() {
    var type = TemporalValueType.UTC_TIMESTAMP;
    var schema = Schema.createUnion(Schema.create(Schema.Type.NULL), type.schema());
    for (String text : List.of("0001-01-01T00:00:00Z", "9999-12-31T00:00:00.123456789Z", "2026-10-25T00:30:00Z",
        "2026-10-25T01:30:00Z")) {
      var instant = java.time.Instant.parse(text);
      var value = java.sql.Timestamp.from(instant);
      assertNull(TemporalValueType.fromValue(value));
      assertSame(value, type.normalize(value));
      assertSame(value, type.decode(value));
      assertEquals(value, type.normalize(instant));
      assertEquals(value, type.normalize(instant.atOffset(java.time.ZoneOffset.UTC)));
      assertEquals(instant.toString(), type.encode(value));
      assertEquals(value, AvroCoercions.unwrap(new Utf8(type.encode(value)), schema));
      assertEquals(java.sql.Timestamp.class.getName(), JdbcTypeMapper.mapSchemaToJavaClassName(schema));
      assertEquals(java.sql.Types.TIMESTAMP, JdbcTypeMapper.mapSchemaToJdbcType(schema));
    }
    assertNull(type.encode(null));
  }

  /**
   * Preserves local JDBC timestamp fields, precision, and value-class metadata.
   */
  @Test
  void roundTripsLocalJdbcTimestampFields() {
    var type = TemporalValueType.JDBC_LOCAL_TIMESTAMP;
    var fields = LocalDateTime.parse("2024-01-02T03:04:05.123456789");
    var timestamp = java.sql.Timestamp.valueOf(fields);
    assertSame(timestamp, type.normalize(timestamp));
    assertSame(timestamp, type.decode(timestamp));
    assertEquals(timestamp, type.normalize(fields));
    assertEquals(fields.toString(), type.encode(timestamp));
    assertEquals(timestamp, AvroCoercions.unwrap(new Utf8(type.encode(timestamp)), type.schema()));
    assertEquals(java.sql.Timestamp.class.getName(), JdbcTypeMapper.mapSchemaToJavaClassName(type.schema()));
    assertEquals(java.sql.Types.TIMESTAMP, JdbcTypeMapper.mapSchemaToJdbcType(type.schema()));
    assertNull(TemporalValueType.fromValue(timestamp));
    assertNull(type.encode(null));
  }

  /** Ordinary strings and NULL values do not acquire temporal descriptors. */
  @Test
  void ignoresOtherValuesAndSchemas() {
    assertNull(TemporalValueType.fromValue(null));
    assertNull(TemporalValueType.fromValue("12:34:56"));
    assertNull(TemporalValueType.fromSchema(null));
    assertNull(TemporalValueType.fromSchema(Schema.create(Schema.Type.STRING)));
    Schema unknown = Schema.create(Schema.Type.STRING);
    unknown.addProp("jparq.temporalType", "FUTURE_TIMESTAMP");
    assertNull(TemporalValueType.fromSchema(unknown));
    assertEquals(java.sql.Types.VARCHAR, JdbcTypeMapper.mapSchemaToJdbcType(unknown));
  }
  /** Checks direct JDBC conversions preserve fractional seconds. */
  @Test
  void normalizesJdbcValuesDirectly() {
    LocalDateTime timestamp = LocalDateTime.parse("1960-02-03T12:34:56.123456789");
    assertEquals(timestamp, TemporalValueType.LOCAL_TIMESTAMP.normalize(java.sql.Timestamp.valueOf(timestamp)));
    LocalTime time = LocalTime.of(12, 34, 56);
    assertEquals(time, TemporalValueType.LOCAL_TIME.normalize(java.sql.Time.valueOf(time)));
    assertNull(TemporalValueType.LOCAL_TIME.normalize(null));
    assertThrows(java.time.format.DateTimeParseException.class,
        () -> TemporalValueType.LOCAL_TIMESTAMP.normalize("invalid"));
  }

}
