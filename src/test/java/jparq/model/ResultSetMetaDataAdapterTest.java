
package jparq.model;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.model.ResultSetMetaDataAdapter;

public class ResultSetMetaDataAdapterTest {

  private ResultSetMetaDataAdapter metaData;

  @BeforeEach
  public void setUp() {
    metaData = new ResultSetMetaDataAdapter();
  }

  @Test
  public void testGetColumnCount() throws SQLException {
    assertEquals(0, metaData.getColumnCount());
  }

  @Test
  public void testIsAutoIncrement() throws SQLException {
    assertFalse(metaData.isAutoIncrement(1));
  }

  @Test
  public void testIsCaseSensitive() throws SQLException {
    assertFalse(metaData.isCaseSensitive(1));
  }

  @Test
  public void testIsSearchable() throws SQLException {
    assertFalse(metaData.isSearchable(1));
  }

  @Test
  public void testIsCurrency() throws SQLException {
    assertFalse(metaData.isCurrency(1));
  }

  @Test
  public void testIsNullable() throws SQLException {
    assertEquals(0, metaData.isNullable(1));
  }

  @Test
  public void testIsSigned() throws SQLException {
    assertFalse(metaData.isSigned(1));
  }

  @Test
  public void testGetColumnDisplaySize() throws SQLException {
    assertEquals(0, metaData.getColumnDisplaySize(1));
  }

  @Test
  public void testGetColumnLabel() throws SQLException {
    assertEquals("", metaData.getColumnLabel(1));
  }

  @Test
  public void testGetColumnName() throws SQLException {
    assertEquals("", metaData.getColumnName(1));
  }

  @Test
  public void testGetSchemaName() throws SQLException {
    assertEquals("", metaData.getSchemaName(1));
  }

  @Test
  public void testGetPrecision() throws SQLException {
    assertEquals(0, metaData.getPrecision(1));
  }

  @Test
  public void testGetScale() throws SQLException {
    assertEquals(0, metaData.getScale(1));
  }

  @Test
  public void testGetTableName() throws SQLException {
    assertEquals("", metaData.getTableName(1));
  }

  @Test
  public void testGetCatalogName() throws SQLException {
    assertEquals("", metaData.getCatalogName(1));
  }

  @Test
  public void testGetColumnType() throws SQLException {
    assertEquals(0, metaData.getColumnType(1));
  }

  @Test
  public void testGetColumnTypeName() throws SQLException {
    assertEquals("", metaData.getColumnTypeName(1));
  }

  @Test
  public void testIsReadOnly() throws SQLException {
    assertFalse(metaData.isReadOnly(1));
  }

  @Test
  public void testIsWritable() throws SQLException {
    assertFalse(metaData.isWritable(1));
  }

  @Test
  public void testIsDefinitelyWritable() throws SQLException {
    assertFalse(metaData.isDefinitelyWritable(1));
  }

  @Test
  public void testGetColumnClassName() throws SQLException {
    assertEquals("", metaData.getColumnClassName(1));
  }

  @Test
  public void testUnwrap() throws SQLException {
    assertNull(metaData.unwrap(null));
  }

  @Test
  public void testIsWrapperFor() throws SQLException {
    assertFalse(metaData.isWrapperFor(null));
  }
}
