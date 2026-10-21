package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.Identifier;

/** Unit tests for {@link AvroCoercions}. */
class AvroCoercionsTest {

  /**
   * Ensures negative epochs use floor division and local timestamps retain their
   * wall clock.
   */
  @Test
  void unwrapsAllTimestampPrecisionsBeforeTheEpoch() {
    var previous = java.util.TimeZone.getDefault();
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/New_York"));
    try {
      for (String precision : java.util.List.of("millis", "micros", "nanos")) {
        for (boolean local : new boolean[]{
            true, false
        }) {
          Schema timestamp = Schema.create(Schema.Type.LONG);
          timestamp.addProp("logicalType", (local ? "local-" : "") + "timestamp-" + precision);
          long nanos = precision.equals("millis") ? 1_000_000L : precision.equals("micros") ? 1000L : 1L;
          var instant = java.time.Instant.ofEpochSecond(0, -nanos);
          Timestamp expected = local
              ? Timestamp.valueOf(java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC))
              : Timestamp.from(instant);
          assertEquals(expected, AvroCoercions.unwrap(-1L, timestamp));
          Schema arraySchema = Schema.createArray(timestamp);
          Object[] decoded = assertInstanceOf(Object[].class, AvroCoercions.unwrap(new Object[]{
              -1L, null
          }, arraySchema));
          assertArrayEquals(new Object[]{
              expected, null
          }, decoded);
          assertArrayEquals(decoded, assertInstanceOf(Object[].class, AvroCoercions.unwrap(decoded, arraySchema)));
          assertEquals(expected, AvroCoercions.coerceLiteral(expected.toString(), timestamp));
        }
      }
    } finally {
      java.util.TimeZone.setDefault(previous);
    }
  }

  /**
   * Tests the half-float decoder for signs, subnormal values, infinities, and
   * NaN.
   */
  @Test
  void unwrapsFloat16SpecialValues() {
    Schema half = SchemaBuilder.fixed("half").size(2);
    half.addProp("jparq.logicalType", "float16");
    for (short bits : new short[]{
        0, 1, (short) 0xbe00, 0x7c00, (short) 0xfc00, 0x7e00
    }) {
      var value = new GenericData.Fixed(half, new byte[]{
          (byte) bits, (byte) (bits >>> 8)
      });
      assertEquals(Float.float16ToFloat(bits), AvroCoercions.unwrap(value, half));
    }
  }

  @Test
  void coercesLiteralsForDatesAndTimestamps() {
    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Object date = AvroCoercions.coerceLiteral("2024-02-03", dateSchema);
    assertEquals(Date.valueOf(LocalDate.of(2024, 2, 3)), date);

    Schema tsSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Object timestamp = AvroCoercions.coerceLiteral("2024-02-03 10:11:12", tsSchema);
    assertEquals(Timestamp.valueOf("2024-02-03 10:11:12"), timestamp);
  }

  @Test
  void unwrapsBinaryValuesRespectingPreferences() {
    Schema utf8Bytes = Schema.create(Schema.Type.BYTES);
    utf8Bytes.addProp("convertedType", "UTF8");
    assertEquals("hello", AvroCoercions.unwrap("hello".getBytes(), utf8Bytes));

    Schema binaryPreferred = Schema.create(Schema.Type.BYTES);
    binaryPreferred.addProp("jparq.binary", true);
    Object binary = AvroCoercions.unwrap(ByteBuffer.wrap("hi".getBytes()), binaryPreferred);
    assertTrue(binary instanceof byte[]);
    assertEquals("hi", new String((byte[]) binary));
  }

  /** Keeps UTF-8 array elements binary when the element schema requests it. */
  @Test
  void unwrapsBinaryArrayElementsRespectingPreferences() {
    Schema element = Schema.create(Schema.Type.BYTES);
    element.addProp("jparq.binary", true);
    byte[] expected = "abc".getBytes(StandardCharsets.UTF_8);
    Object[] decoded = assertInstanceOf(Object[].class, AvroCoercions.unwrap(new Object[]{
        ByteBuffer.wrap(expected)
    }, Schema.createArray(element)));
    assertEquals(1, decoded.length);
    assertArrayEquals(expected, assertInstanceOf(byte[].class, decoded[0]));
  }

  @Test
  void unwrapsDecimalsFromBytesAndFixed() {
    Schema decimalBytes = LogicalTypes.decimal(4, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    byte[] value = new BigDecimal("12.34").unscaledValue().toByteArray();
    assertEquals(new BigDecimal("12.34"), AvroCoercions.unwrap(ByteBuffer.wrap(value), decimalBytes));

    Schema fixedSchema = SchemaBuilder.fixed("dec").size(4);
    LogicalTypes.decimal(4, 2).addToSchema(fixedSchema);
    GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema, value);
    assertEquals(new BigDecimal("12.34"), AvroCoercions.unwrap(fixed, fixedSchema));
  }

  /**
   * Decodes logical array elements across containers without changing storage
   * values.
   */
  @Test
  void unwrapsLogicalArrayContainersWithoutMutatingStorage() {
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema decimal = LogicalTypes.decimal(4, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema fixed = LogicalTypes.decimal(4, 2).addToSchema(SchemaBuilder.fixed("array_decimal").size(4));
    ByteBuffer bytes = ByteBuffer.wrap(new BigDecimal("12.34").unscaledValue().toByteArray());
    Object[] stored = {
        1, bytes, new GenericData.Fixed(fixed, new byte[]{
            0, 0, 4, (byte) 210
        })
    };
    Object[] expected = {
        Date.valueOf("1970-01-02"), new BigDecimal("12.34"), new BigDecimal("12.34")
    };
    var schemas = java.util.List.of(date, decimal, fixed);
    for (int index = 0; index < schemas.size(); index++) {
      Schema array = Schema.createArray(Schema.createUnion(Schema.create(Schema.Type.NULL), schemas.get(index)));
      Object[] raw = {
          stored[index], null
      };
      var values = java.util.Arrays.asList(raw);
      Object[] typed = switch (index) {
        case 0 -> new Integer[]{
            1, null
          };
        case 1 -> new ByteBuffer[]{
            bytes, null
          };
        case 2 -> new GenericData.Fixed[]{
            (GenericData.Fixed) stored[index], null
          };
        default -> throw new IllegalArgumentException("Unexpected logical type index " + index);
      };
      for (Object container : java.util.List.of(raw, typed, values, new GenericData.Array<>(array, values))) {
        Object[] decoded = assertInstanceOf(Object[].class, AvroCoercions.unwrap(container, array));
        assertArrayEquals(new Object[]{
            expected[index], null
        }, decoded);
        assertArrayEquals(decoded, assertInstanceOf(Object[].class, AvroCoercions.unwrap(decoded, array)));
      }
      assertSame(stored[index], raw[0]);
    }
    assertEquals(0, bytes.position());
  }

  @Test
  void resolvesColumnsCaseInsensitively() {
    Schema recordSchema = SchemaBuilder.record("Rec").fields().name("Name").type().stringType().noDefault()
        .name("value").type().intType().noDefault().endRecord();
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("Name", "Ada");
    record.put("value", 7);

    Map<String, Schema> fieldSchemas = new HashMap<>();
    fieldSchemas.put("Name", Schema.create(Schema.Type.STRING));
    fieldSchemas.put("value", Schema.create(Schema.Type.INT));

    Map<String, String> caseInsensitive = new HashMap<>();
    caseInsensitive.put(Identifier.lookupKey("Name"), "Name");
    caseInsensitive.put(Identifier.lookupKey("value"), "value");

    assertEquals("Ada", AvroCoercions.resolveColumnValue("name", record, fieldSchemas, caseInsensitive));
    assertEquals(7, AvroCoercions.resolveColumnValue("value", record, fieldSchemas, caseInsensitive));
    assertNull(AvroCoercions.resolveColumnValue("missing", record, fieldSchemas, caseInsensitive));
  }

  @Test
  void unwrapsArraysAndRecords() {
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    GenericData.Array<String> avroArray = new GenericData.Array<>(arraySchema, java.util.List.of("a", "b"));
    Object arrayResult = AvroCoercions.unwrap(avroArray, arraySchema);
    assertTrue(arrayResult instanceof Object[]);
    assertArrayEquals(new Object[]{
        "a", "b"
    }, (Object[]) arrayResult);

    Schema recordSchema = SchemaBuilder.record("R").fields().name("x").type().stringType().noDefault().endRecord();
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("x", "value");
    assertTrue(AvroCoercions.unwrap(record, recordSchema).toString().toLowerCase(Locale.ROOT).contains("value"));
  }
}
