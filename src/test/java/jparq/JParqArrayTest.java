package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.time.Duration;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqArray;

/**
 * Verifies JDBC array slicing, type metadata, element cursors, and lifecycle.
 */
class JParqArrayTest {

  /** Full and sliced access preserve nulls and use one-based indexes. */
  @Test
  void readsCopiesAndSlices() throws Exception {
    Object[] source = {
        1, null, 3
    };
    var array = new JParqArray(source, Schema.create(Schema.Type.INT));
    source[0] = 99;
    assertEquals(Types.INTEGER, array.getBaseType());
    assertEquals("INTEGER", array.getBaseTypeName());
    assertArrayEquals(new Object[]{
        1, null, 3
    }, (Object[]) array.getArray());
    Object[] copy = (Object[]) array.getArray();
    copy[0] = 99;
    assertArrayEquals(new Object[]{
        1, null, 3
    }, (Object[]) array.getArray(Map.of()));
    assertArrayEquals(new Object[]{
        null, 3
    }, (Object[]) array.getArray(2, 2));
    assertArrayEquals(new Object[]{
        3
    }, (Object[]) array.getArray(3, Integer.MAX_VALUE, Map.of()));
    assertArrayEquals(new Object[0], (Object[]) array.getArray(4, 0));
    assertArrayEquals(new Object[0], (Object[]) array.getArray(1, 0));
    assertThrows(SQLException.class, () -> array.getArray(0, 1));
    assertThrows(SQLException.class, () -> array.getArray(1, -1));
    assertThrows(SQLException.class, () -> array.getArray(5, 0));
    assertThrows(SQLException.class, () -> array.getArray(Long.MAX_VALUE, 1));
  }

  /** Element cursors expose ordered original indexes, typed values, and nulls. */
  @Test
  void elementCursorsRetainIndexesAndTypes() throws Exception {
    var array = new JParqArray(new Object[]{
        1, null, 3
    }, Schema.create(Schema.Type.INT));
    try (var rows = array.getResultSet()) {
      assertEquals(Types.BIGINT, rows.getMetaData().getColumnType(1));
      assertEquals(Types.INTEGER, rows.getMetaData().getColumnType(2));
      assertEquals(ResultSetMetaData.columnNoNulls, rows.getMetaData().isNullable(1));
      assertEquals(ResultSetMetaData.columnNullable, rows.getMetaData().isNullable(2));
      assertEquals(ResultSet.CONCUR_READ_ONLY, rows.getConcurrency());
      for (int i = 1; i <= 3; i++) {
        assertTrue(rows.next());
        assertEquals(i, rows.getLong("INDEX"));
        assertFalse(rows.rowInserted());
        rows.updateObject(2, 99);
        assertEquals(i == 2 ? null : i, rows.getObject("VALUE"));
        assertEquals(i == 2, rows.wasNull());
      }
      assertFalse(rows.next());
    }
    try (var rows = array.getResultSet(2, 2, Map.of())) {
      assertTrue(rows.next());
      assertEquals(2, rows.getLong(1));
      assertNull(rows.getObject(2));
      assertTrue(rows.next());
      assertEquals(3, rows.getLong(1));
      assertEquals(3, rows.getInt(2));
      assertFalse(rows.next());
    }
    try (var rows = array.getResultSet(Map.of())) {
      assertTrue(rows.next());
      assertEquals(1, rows.getLong(1));
    }
    var empty = new JParqArray(new Object[0], Schema.create(Schema.Type.INT));
    assertEquals(Types.INTEGER, empty.getBaseType());
    assertArrayEquals(new Object[0], (Object[]) empty.getArray());
    try (var rows = empty.getResultSet()) {
      assertFalse(rows.next());
    }
  }

  /** Nested arrays retain their containers and expose JDBC arrays in cursors. */
  @Test
  void nestedArraysAreIndependent() throws Exception {
    Object[] nested = {
        1, 2
    };
    var array = new JParqArray(new Object[]{
        nested
    }, Schema.createArray(Schema.create(Schema.Type.INT)));
    nested[0] = 99;
    ((Object[]) ((Object[]) array.getArray())[0])[0] = 88;
    assertArrayEquals(new Object[]{
        new Object[]{
            1, 2
        }
    }, (Object[]) array.getArray());
    assertEquals(Types.ARRAY, array.getBaseType());
    try (var rows = array.getResultSet()) {
      assertTrue(rows.next());
      assertArrayEquals(new Object[]{
          1, 2
      }, (Object[]) rows.getArray(2).getArray());
      assertEquals(Types.INTEGER, rows.getArray(2).getBaseType());
    }
  }

  /** Declared schemas preserve logical types even when every element is null. */
  @Test
  void logicalAndUnknownElementTypes() throws Exception {
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    var dates = new JParqArray(new Object[]{
        null, Date.valueOf("2026-01-02")
    }, date);
    assertEquals(Types.DATE, dates.getBaseType());
    assertEquals("DATE", dates.getBaseTypeName());
    assertEquals(Types.DATE, new JParqArray(new Object[]{
        null
    }, date).getBaseType());
    Schema decimal = LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    assertEquals(Types.DECIMAL, new JParqArray(new Object[]{
        new BigDecimal("1.25")
    }, decimal).getBaseType());
    assertEquals(Types.INTEGER, new JParqArray(new Object[]{
        null, 1, 2
    }, null).getBaseType());
    assertEquals(Types.OTHER, new JParqArray(new Object[0], null).getBaseType());
    assertEquals(Types.OTHER, new JParqArray(new Object[]{
        null
    }, null).getBaseType());
    assertEquals(Types.OTHER, new JParqArray(new Object[]{
        1, "a"
    }, null).getBaseType());
    assertEquals(Types.TIME_WITH_TIMEZONE, new JParqArray(new Object[]{
        java.time.OffsetTime.parse("12:30:00+02:00")
    }, null).getBaseType());
    Schema record = Schema.createRecord("element", null, "example", false);
    record.setFields(java.util.List.of());
    var structs = new JParqArray(new Object[]{
        null
    }, record);
    assertEquals("example.element", structs.getBaseTypeName());
    assertArrayEquals(new Object[]{
        null
    }, (Object[]) structs.getArray(Map.of("unrelated", String.class)));
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> structs.getArray(Map.of("example.element", String.class)));
  }

  /** Large element cursors retain order and avoid quadratic front insertions. */
  @Test
  void largeElementCursorBuildsInOrder() {
    assertTimeout(Duration.ofSeconds(5), () -> {
      Object[] values = new Object[200_000];
      for (int i = 0; i < values.length; i++) {
        values[i] = i;
      }
      try (var rows = new JParqArray(values, Schema.create(Schema.Type.INT)).getResultSet()) {
        for (int i = 0; i < values.length; i++) {
          assertTrue(rows.next());
          assertEquals(i + 1, rows.getLong(1));
          assertEquals(i, rows.getInt(2));
        }
        assertFalse(rows.next());
      }
    });
  }

  /** Binary and record elements are detached on input and on every accessor. */
  @Test
  void mutableElementsAreIndependent() throws Exception {
    byte[] bytes = {
        1, 2
    };
    var binary = new JParqArray(new Object[]{
        bytes
    }, Schema.create(Schema.Type.BYTES));
    bytes[0] = 9;
    ((byte[]) ((Object[]) binary.getArray())[0])[0] = 8;
    ((byte[]) ((Object[]) binary.getArray(1, 1))[0])[0] = 7;
    try (var rows = binary.getResultSet()) {
      assertTrue(rows.next());
      ((byte[]) rows.getObject(2))[0] = 6;
    }
    assertArrayEquals(new byte[]{
        1, 2
    }, (byte[]) ((Object[]) binary.getArray())[0]);

    Schema schema = SchemaBuilder.record("element").namespace("example").fields().requiredBytes("payload").endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("payload", new byte[]{
        3, 4
    });
    var structs = new JParqArray(new Object[]{
        record
    }, schema);
    ((byte[]) record.get("payload"))[0] = 9;
    GenericRecord copy = (GenericRecord) ((Object[]) structs.getArray())[0];
    ((byte[]) copy.get("payload"))[0] = 8;
    try (var rows = structs.getResultSet()) {
      assertTrue(rows.next());
      assertEquals("example.element", rows.getMetaData().getColumnTypeName(2));
      ((GenericRecord) rows.getObject(2)).put("payload", new byte[]{
          7
      });
    }
    GenericRecord retained = (GenericRecord) ((Object[]) structs.getArray())[0];
    assertArrayEquals(new byte[]{
        3, 4
    }, (byte[]) retained.get("payload"));
  }

  /** Mutable fields inside records retain independent copies at every depth. */
  @Test
  void nestedMutableRecordValuesAreIndependent() throws Exception {
    Schema schema = SchemaBuilder.record("complex").fields().name("attributes").type().map().values().bytesType()
        .noDefault().name("items").type().array().items().bytesType().noDefault().name("fixed").type().fixed("pair")
        .size(2).noDefault().name("text").type().stringType().noDefault().name("date")
        .type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault().endRecord();
    var buffer = java.nio.ByteBuffer.wrap(new byte[]{
        0, 1, 2
    });
    buffer.position(1);
    var text = new org.apache.avro.util.Utf8("original");
    var date = Date.valueOf("2026-01-02");
    GenericRecord record = new GenericData.Record(schema);
    record.put("attributes", Map.of("binary", buffer));
    record.put("items", java.util.List.of(new byte[]{
        3, 4
    }));
    var fixed = new GenericData.Fixed(schema.getField("fixed").schema(), new byte[]{
        5, 6
    });
    record.put("fixed", fixed);
    record.put("text", text);
    record.put("date", date);
    final var array = new JParqArray(new Object[]{
        record
    }, schema);
    buffer.put(1, (byte) 9);
    fixed.bytes()[0] = 9;
    text.set("changed");
    date.setTime(0);
    GenericRecord copy = (GenericRecord) ((Object[]) array.getArray())[0];
    var copiedBuffer = (java.nio.ByteBuffer) ((Map<?, ?>) copy.get("attributes")).get("binary");
    assertEquals(java.nio.ByteBuffer.wrap(new byte[]{
        1, 2
    }), copiedBuffer);
    assertEquals(1, buffer.position());
    assertArrayEquals(new byte[]{
        5, 6
    }, ((GenericFixed) copy.get("fixed")).bytes());
    ((GenericFixed) copy.get("fixed")).bytes()[0] = 8;
    assertEquals("original", copy.get("text").toString());
    assertEquals(Date.valueOf("2026-01-02"), copy.get("date"));
    ((byte[]) ((java.util.List<?>) copy.get("items")).getFirst())[0] = 9;
    copiedBuffer.put(0, (byte) 8);
    ((org.apache.avro.util.Utf8) copy.get("text")).set("changed again");
    ((Date) copy.get("date")).setTime(0);
    GenericRecord retained = (GenericRecord) ((Object[]) array.getArray())[0];
    assertArrayEquals(new byte[]{
        3, 4
    }, (byte[]) ((java.util.List<?>) retained.get("items")).getFirst());
    assertEquals(java.nio.ByteBuffer.wrap(new byte[]{
        1, 2
    }), ((Map<?, ?>) retained.get("attributes")).get("binary"));
    assertArrayEquals(new byte[]{
        5, 6
    }, ((GenericFixed) retained.get("fixed")).bytes());
    assertEquals("original", retained.get("text").toString());
    assertEquals(Date.valueOf("2026-01-02"), retained.get("date"));
  }

  /** VALUE permits nulls for scalar, logical, and nested element declarations. */
  @Test
  void nullableElementMetadata() throws Exception {
    for (Schema schema : java.util.List.of(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING),
        LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES)),
        Schema.createArray(Schema.create(Schema.Type.INT)))) {
      try (var rows = new JParqArray(new Object[]{
          null
      }, schema).getResultSet()) {
        assertEquals(ResultSetMetaData.columnNullable, rows.getMetaData().isNullable(2));
        assertTrue(rows.next());
        assertNull(rows.getObject(2));
        assertTrue(rows.wasNull());
      }
    }
  }

  /** Free is idempotent and rejects every subsequent read operation. */
  @Test
  void freedArraysRejectAccess() throws Exception {
    var array = new JParqArray(new Object[]{
        1
    }, null);
    array.free();
    array.free();
    assertThrows(SQLException.class, array::getBaseType);
    assertThrows(SQLException.class, array::getBaseTypeName);
    assertThrows(SQLException.class, array::getArray);
    assertThrows(SQLException.class, () -> array.getArray(Map.of()));
    assertThrows(SQLException.class, () -> array.getArray(1, 1));
    assertThrows(SQLException.class, () -> array.getArray(1, 1, Map.of()));
    assertThrows(SQLException.class, array::getResultSet);
    assertThrows(SQLException.class, () -> array.getResultSet(Map.of()));
    assertThrows(SQLException.class, () -> array.getResultSet(1, 1));
    assertThrows(SQLException.class, () -> array.getResultSet(1, 1, Map.of()));
  }
}
