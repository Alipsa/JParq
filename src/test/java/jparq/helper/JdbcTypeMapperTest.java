package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/** Unit tests for {@link JdbcTypeMapper}. */
class JdbcTypeMapperTest {

  @Test
  void resolvesNullableAndNonNullSchemas() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema union = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), stringSchema));

    assertEquals(stringSchema, JdbcTypeMapper.nonNullSchema(union));
    assertTrue(JdbcTypeMapper.isNullable(union));
    assertFalse(JdbcTypeMapper.isNullable(stringSchema));
    assertNull(JdbcTypeMapper.nonNullSchema(null));
  }

  @Test
  void mapsAvroSchemasToJdbcTypes() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    assertEquals(Types.INTEGER, JdbcTypeMapper.mapSchemaToJdbcType(intSchema));

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    assertEquals(Types.DATE, JdbcTypeMapper.mapSchemaToJdbcType(dateSchema));

    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    assertEquals(Types.TIME, JdbcTypeMapper.mapSchemaToJdbcType(timeSchema));

    Schema timestampSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    assertEquals(Types.TIMESTAMP, JdbcTypeMapper.mapSchemaToJdbcType(timestampSchema));

    Schema timeMicros = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    assertEquals(Types.TIME, JdbcTypeMapper.mapSchemaToJdbcType(timeMicros));

    Schema decimalSchema = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    assertEquals(Types.DECIMAL, JdbcTypeMapper.mapSchemaToJdbcType(decimalSchema));

    Schema utf8Bytes = Schema.create(Schema.Type.BYTES);
    utf8Bytes.addProp("originalType", "UTF8");
    assertEquals(Types.VARCHAR, JdbcTypeMapper.mapSchemaToJdbcType(utf8Bytes));

    Schema recordSchema = SchemaBuilder.record("rec").fields().endRecord();
    assertEquals(Types.STRUCT, JdbcTypeMapper.mapSchemaToJdbcType(recordSchema));
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    assertEquals(Types.ARRAY, JdbcTypeMapper.mapSchemaToJdbcType(arraySchema));

    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    assertEquals(Types.STRUCT, JdbcTypeMapper.mapSchemaToJdbcType(mapSchema));
    assertEquals(Types.OTHER, JdbcTypeMapper.mapSchemaToJdbcType(null));
  }

  @Test
  void mapsAvroSchemasToJavaClasses() {
    assertEquals(String.class.getName(), JdbcTypeMapper.mapSchemaToJavaClassName(Schema.create(Schema.Type.STRING)));

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    assertEquals(java.sql.Date.class.getName(), JdbcTypeMapper.mapSchemaToJavaClassName(dateSchema));

    Schema timeSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    assertEquals(Time.class.getName(), JdbcTypeMapper.mapSchemaToJavaClassName(timeSchema));

    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    assertEquals(Timestamp.class.getName(), JdbcTypeMapper.mapSchemaToJavaClassName(timestampSchema));

    Schema decimalSchema = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    assertEquals(BigDecimal.class.getName(), JdbcTypeMapper.mapSchemaToJavaClassName(decimalSchema));

    assertEquals(Object.class.getName(), JdbcTypeMapper.mapSchemaToJavaClassName(null));
  }

  @Test
  void mapsJdbcTypesToJavaClasses() {
    assertEquals(Boolean.class.getName(), JdbcTypeMapper.mapJdbcTypeToClassName(Types.BOOLEAN));
    assertEquals(Time.class.getName(), JdbcTypeMapper.mapJdbcTypeToClassName(Types.TIME_WITH_TIMEZONE));
    assertEquals(Timestamp.class.getName(), JdbcTypeMapper.mapJdbcTypeToClassName(Types.TIMESTAMP));
    assertEquals(Object.class.getName(), JdbcTypeMapper.mapJdbcTypeToClassName(Integer.MIN_VALUE));
  }

  @Test
  void mapsJavaClassNamesToJdbcTypes() {
    assertEquals(Types.VARCHAR, JdbcTypeMapper.mapJavaClassNameToJdbcType("java.lang.String"));
    assertEquals(Types.VARBINARY, JdbcTypeMapper.mapJavaClassNameToJdbcType("[B"));
    assertEquals(Types.ARRAY, JdbcTypeMapper.mapJavaClassNameToJdbcType("java.util.Collection"));
    assertEquals(Types.OTHER, JdbcTypeMapper.mapJavaClassNameToJdbcType(""));
    assertEquals(Types.OTHER, JdbcTypeMapper.mapJavaClassNameToJdbcType("unknown.Type"));
  }

  @Test
  void detectsUtf8ByteSchemas() {
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    bytesSchema.addProp("avro.java.string", "String");
    assertEquals(Types.VARCHAR, JdbcTypeMapper.mapSchemaToJdbcType(bytesSchema));
    assertEquals(byte[].class.getName(), JdbcTypeMapper.mapSchemaToJavaClassName(bytesSchema));
    assertEquals(Types.OTHER, JdbcTypeMapper.mapJavaClassNameToJdbcType(null));
  }
}
