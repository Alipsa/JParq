package se.alipsa.jparq.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
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

/** Unit tests for {@link AvroCoercions}. */
class AvroCoercionsTest {

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
