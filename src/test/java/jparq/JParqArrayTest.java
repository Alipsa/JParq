package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
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
      for (int i = 1; i <= 3; i++) {
        assertTrue(rows.next());
        assertEquals(i, rows.getLong("INDEX"));
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
    assertArrayEquals(new Object[]{
        null
    }, (Object[]) structs.getArray(Map.of("unrelated", String.class)));
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> structs.getArray(Map.of("example.element", String.class)));
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
