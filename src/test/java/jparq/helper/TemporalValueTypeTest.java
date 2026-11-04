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
      assertEquals(value, AvroCoercions.unwrap(new Utf8(value.toString()), schema));
      assertEquals(value.getClass().getName(), JdbcTypeMapper.mapSchemaToJavaClassName(schema));
      assertEquals(type.jdbcType(), JdbcTypeMapper.mapSchemaToJdbcType(schema));
      assertNull(AvroCoercions.unwrap(null, schema));
    }
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
