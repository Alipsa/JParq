package jparq.meta;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.model.MetaDataResultSet;

/**
 * Tests for {@link MetaDataResultSet}.
 */
public class MetaDataResultSetTest {

  /**
   * Verifies that null rows are handled gracefully when accessed by index or
   * column label.
   */
  @Test
  public void testNullRowAccessIsSafe() throws Exception {
    String[] headers = {
        "A", "B"
    };
    List<Object[]> rows = new ArrayList<>();
    rows.add(null);
    rows.add(new Object[]{
        "value", "other"
    });

    try (MetaDataResultSet resultSet = new MetaDataResultSet(headers, rows)) {
      Assertions.assertTrue(resultSet.next());
      Assertions.assertNull(resultSet.getString(1));
      Assertions.assertTrue(resultSet.wasNull());
      Assertions.assertNull(resultSet.getString("A"));
      Assertions.assertTrue(resultSet.wasNull());

      Assertions.assertTrue(resultSet.next());
      Assertions.assertEquals("value", resultSet.getString("A"));
      Assertions.assertFalse(resultSet.wasNull());
      Assertions.assertEquals("other", resultSet.getString(2));
      Assertions.assertFalse(resultSet.wasNull());
    }
  }

  @Test
  public void testGetObject() throws Exception {
    String[] headers = {
        "A", "B"
    };
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{
        "value", 123
    });
    try (MetaDataResultSet resultSet = new MetaDataResultSet(headers, rows)) {
      resultSet.next();

      Assertions.assertEquals("value", resultSet.getObject("A"));
      Assertions.assertEquals(123, resultSet.getObject("B"));
      Assertions.assertEquals("value", resultSet.getObject(1));
      Assertions.assertEquals(123, resultSet.getObject(2));
    }
  }

  @Test
  public void testGetInt() throws Exception {
    String[] headers = {
        "A", "B"
    };
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{
        "123", 456
    });

    try (MetaDataResultSet resultSet = new MetaDataResultSet(headers, rows)) {
      resultSet.next();

      Assertions.assertEquals(123, resultSet.getInt("A"));
      Assertions.assertEquals(456, resultSet.getInt("B"));
      Assertions.assertEquals(123, resultSet.getInt(1));
      Assertions.assertEquals(456, resultSet.getInt(2));
    }
  }

  @Test
  public void testGetLong() throws Exception {
    String[] headers = {
        "A", "B"
    };
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{
        "1234567890", 9876543210L
    });

    try (MetaDataResultSet resultSet = new MetaDataResultSet(headers, rows)) {
      resultSet.next();
      Assertions.assertEquals(1234567890L, resultSet.getLong("A"));
      Assertions.assertEquals(9876543210L, resultSet.getLong("B"));
      Assertions.assertEquals(1234567890L, resultSet.getLong(1));
      Assertions.assertEquals(9876543210L, resultSet.getLong(2));
    }
  }

  @Test
  public void testFindColumn() throws Exception {
    String[] headers = {
        "A", "B"
    };
    List<Object[]> rows = new ArrayList<>();
    try (MetaDataResultSet resultSet = new MetaDataResultSet(headers, rows)) {
      Assertions.assertEquals(1, resultSet.findColumn("A"));
      Assertions.assertEquals(2, resultSet.findColumn("B"));
      Assertions.assertEquals(1, resultSet.findColumn("a"));
      Assertions.assertEquals(-1, resultSet.findColumn("C"));
    }
  }

  @Test
  public void testGetMetaData() throws Exception {
    String[] headers = {
        "A", "B"
    };
    List<Object[]> rows = new ArrayList<>();
    try (MetaDataResultSet resultSet = new MetaDataResultSet(headers, rows)) {
      ResultSetMetaData metaData = resultSet.getMetaData();

      Assertions.assertEquals(2, metaData.getColumnCount());
      Assertions.assertEquals("A", metaData.getColumnLabel(1));
      Assertions.assertEquals("B", metaData.getColumnName(2));
      Assertions.assertEquals(Types.VARCHAR, metaData.getColumnType(1));
      Assertions.assertEquals("VARCHAR", metaData.getColumnTypeName(1));
      Assertions.assertNull(metaData.unwrap(Object.class));
      Assertions.assertFalse(metaData.isWrapperFor(Object.class));
      Assertions.assertEquals("", metaData.getCatalogName(1));
      Assertions.assertEquals(String.class.getName(), metaData.getColumnClassName(1));
      Assertions.assertEquals(0, metaData.getColumnDisplaySize(1));
      Assertions.assertEquals("", metaData.getSchemaName(1));
      Assertions.assertEquals(0, metaData.getPrecision(1));
      Assertions.assertEquals(0, metaData.getScale(1));
      Assertions.assertFalse(metaData.isAutoIncrement(1));
      Assertions.assertTrue(metaData.isCaseSensitive(1));
      Assertions.assertFalse(metaData.isCurrency(1));
      Assertions.assertFalse(metaData.isDefinitelyWritable(1));
      Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown, metaData.isNullable(1));
      Assertions.assertTrue(metaData.isReadOnly(1));
      Assertions.assertTrue(metaData.isSearchable(1));
      Assertions.assertFalse(metaData.isSigned(1));
      Assertions.assertFalse(metaData.isWritable(1));
      Assertions.assertEquals("", metaData.getTableName(1));
    }
  }
}
