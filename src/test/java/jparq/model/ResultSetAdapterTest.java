
package jparq.model;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.model.ResultSetAdapter;

public class ResultSetAdapterTest {

  private ResultSetAdapter resultSetAdapter;

  @BeforeEach
  public void setUp() {
    resultSetAdapter = new ResultSetAdapter();
  }

  @Test
  public void testNext() throws SQLException {
    assertFalse(resultSetAdapter.next());
  }

  @Test
  public void testClose() {
    assertDoesNotThrow(() -> resultSetAdapter.close());
  }

  @Test
  public void testWasNull() throws SQLException {
    assertFalse(resultSetAdapter.wasNull());
  }

  @Test
  public void testGetString() throws SQLException {
    assertEquals("", resultSetAdapter.getString(1));
    assertEquals("", resultSetAdapter.getString("label"));
  }

  @Test
  public void testGetBoolean() throws SQLException {
    assertFalse(resultSetAdapter.getBoolean(1));
    assertFalse(resultSetAdapter.getBoolean("label"));
  }

  @Test
  public void testGetByte() throws SQLException {
    assertEquals((byte) 0, resultSetAdapter.getByte(1));
    assertEquals((byte) 0, resultSetAdapter.getByte("label"));
  }

  @Test
  public void testGetShort() throws SQLException {
    assertEquals((short) 0, resultSetAdapter.getShort(1));
    assertEquals((short) 0, resultSetAdapter.getShort("label"));
  }

  @Test
  public void testGetInt() throws SQLException {
    assertEquals(0, resultSetAdapter.getInt(1));
    assertEquals(0, resultSetAdapter.getInt("label"));
  }

  @Test
  public void testGetLong() throws SQLException {
    assertEquals(0L, resultSetAdapter.getLong(1));
    assertEquals(0L, resultSetAdapter.getLong("label"));
  }

  @Test
  public void testGetFloat() throws SQLException {
    assertEquals(0.0f, resultSetAdapter.getFloat(1), 0.0);
    assertEquals(0.0f, resultSetAdapter.getFloat("label"), 0.0);
  }

  @Test
  public void testGetDouble() throws SQLException {
    assertEquals(0.0d, resultSetAdapter.getDouble(1), 0.0);
    assertEquals(0.0d, resultSetAdapter.getDouble("label"), 0.0);
  }

  @Test
  public void testGetBigDecimal() throws SQLException {
    assertNull(resultSetAdapter.getBigDecimal(1, 2));
    assertNull(resultSetAdapter.getBigDecimal("label", 2));
    assertNull(resultSetAdapter.getBigDecimal(1));
    assertNull(resultSetAdapter.getBigDecimal("label"));
  }

  @Test
  public void testGetBytes() throws SQLException {
    assertArrayEquals(new byte[0], resultSetAdapter.getBytes(1));
    assertArrayEquals(new byte[0], resultSetAdapter.getBytes("label"));
  }

  @Test
  public void testGetDate() throws SQLException {
    assertNull(resultSetAdapter.getDate(1));
    assertNull(resultSetAdapter.getDate("label"));
    assertNull(resultSetAdapter.getDate(1, Calendar.getInstance()));
    assertNull(resultSetAdapter.getDate("label", Calendar.getInstance()));
  }

  @Test
  public void testGetTime() throws SQLException {
    assertNull(resultSetAdapter.getTime(1));
    assertNull(resultSetAdapter.getTime("label"));
    assertNull(resultSetAdapter.getTime(1, Calendar.getInstance()));
    assertNull(resultSetAdapter.getTime("label", Calendar.getInstance()));
  }

  @Test
  public void testGetTimestamp() throws SQLException {
    assertNull(resultSetAdapter.getTimestamp(1));
    assertNull(resultSetAdapter.getTimestamp("label"));
    assertNull(resultSetAdapter.getTimestamp(1, Calendar.getInstance()));
    assertNull(resultSetAdapter.getTimestamp("label", Calendar.getInstance()));
  }

  @Test
  public void testGetAsciiStream() throws SQLException {
    assertNull(resultSetAdapter.getAsciiStream(1));
    assertNull(resultSetAdapter.getAsciiStream("label"));
  }

  @Test
  public void testGetUnicodeStream() throws SQLException {
    assertNull(resultSetAdapter.getUnicodeStream(1));
    assertNull(resultSetAdapter.getUnicodeStream("label"));
  }

  @Test
  public void testGetBinaryStream() throws SQLException {
    assertNull(resultSetAdapter.getBinaryStream(1));
    assertNull(resultSetAdapter.getBinaryStream("label"));
  }

  @Test
  public void testGetWarnings() throws SQLException {
    assertNull(resultSetAdapter.getWarnings());
  }

  @Test
  public void testClearWarnings() {
    assertDoesNotThrow(() -> resultSetAdapter.clearWarnings());
  }

  @Test
  public void testGetCursorName() throws SQLException {
    assertEquals("", resultSetAdapter.getCursorName());
  }

  @Test
  public void testGetMetaData() throws SQLException {
    assertNull(resultSetAdapter.getMetaData());
  }

  @Test
  public void testGetObject() throws SQLException {
    assertNull(resultSetAdapter.getObject(1));
    assertNull(resultSetAdapter.getObject("label"));
    Map<String, Class<?>> map = new HashMap<>();
    assertNull(resultSetAdapter.getObject(1, map));
    assertNull(resultSetAdapter.getObject("label", map));
    assertNull(resultSetAdapter.getObject(1, String.class));
    assertNull(resultSetAdapter.getObject("label", String.class));
  }

  @Test
  public void testFindColumn() throws SQLException {
    assertEquals(0, resultSetAdapter.findColumn("label"));
  }

  @Test
  public void testGetCharacterStream() throws SQLException {
    assertNull(resultSetAdapter.getCharacterStream(1));
    assertNull(resultSetAdapter.getCharacterStream("label"));
  }

  @Test
  public void testIsBeforeFirst() throws SQLException {
    assertFalse(resultSetAdapter.isBeforeFirst());
  }

  @Test
  public void testIsAfterLast() throws SQLException {
    assertFalse(resultSetAdapter.isAfterLast());
  }

  @Test
  public void testIsFirst() throws SQLException {
    assertFalse(resultSetAdapter.isFirst());
  }

  @Test
  public void testIsLast() throws SQLException {
    assertFalse(resultSetAdapter.isLast());
  }

  @Test
  public void testBeforeFirst() {
    assertDoesNotThrow(() -> resultSetAdapter.beforeFirst());
  }

  @Test
  public void testAfterLast() {
    assertDoesNotThrow(() -> resultSetAdapter.afterLast());
  }

  @Test
  public void testFirst() throws SQLException {
    assertFalse(resultSetAdapter.first());
  }

  @Test
  public void testLast() throws SQLException {
    assertFalse(resultSetAdapter.last());
  }

  @Test
  public void testGetRow() throws SQLException {
    assertEquals(0, resultSetAdapter.getRow());
  }

  @Test
  public void testAbsolute() throws SQLException {
    assertFalse(resultSetAdapter.absolute(1));
  }

  @Test
  public void testRelative() throws SQLException {
    assertFalse(resultSetAdapter.relative(1));
  }

  @Test
  public void testPrevious() throws SQLException {
    assertFalse(resultSetAdapter.previous());
  }

  @Test
  public void testSetFetchDirection() {
    assertDoesNotThrow(() -> resultSetAdapter.setFetchDirection(ResultSet.FETCH_FORWARD));
  }

  @Test
  public void testGetFetchDirection() throws SQLException {
    assertEquals(0, resultSetAdapter.getFetchDirection());
  }

  @Test
  public void testSetFetchSize() {
    assertDoesNotThrow(() -> resultSetAdapter.setFetchSize(10));
  }

  @Test
  public void testGetFetchSize() throws SQLException {
    assertEquals(0, resultSetAdapter.getFetchSize());
  }

  @Test
  public void testGetType() throws SQLException {
    assertEquals(0, resultSetAdapter.getType());
  }

  @Test
  public void testGetConcurrency() throws SQLException {
    assertEquals(0, resultSetAdapter.getConcurrency());
  }

  @Test
  public void testRowUpdated() throws SQLException {
    assertFalse(resultSetAdapter.rowUpdated());
  }

  @Test
  public void testRowInserted() throws SQLException {
    assertFalse(resultSetAdapter.rowInserted());
  }

  @Test
  public void testRowDeleted() throws SQLException {
    assertFalse(resultSetAdapter.rowDeleted());
  }

  @Test
  public void testUpdateNull() {
    assertDoesNotThrow(() -> resultSetAdapter.updateNull(1));
    assertDoesNotThrow(() -> resultSetAdapter.updateNull("label"));
  }

  @Test
  public void testUpdateBoolean() {
    assertDoesNotThrow(() -> resultSetAdapter.updateBoolean(1, true));
    assertDoesNotThrow(() -> resultSetAdapter.updateBoolean("label", true));
  }

  @Test
  public void testUpdateByte() {
    assertDoesNotThrow(() -> resultSetAdapter.updateByte(1, (byte) 1));
    assertDoesNotThrow(() -> resultSetAdapter.updateByte("label", (byte) 1));
  }

  @Test
  public void testUpdateShort() {
    assertDoesNotThrow(() -> resultSetAdapter.updateShort(1, (short) 1));
    assertDoesNotThrow(() -> resultSetAdapter.updateShort("label", (short) 1));
  }

  @Test
  public void testUpdateInt() {
    assertDoesNotThrow(() -> resultSetAdapter.updateInt(1, 1));
    assertDoesNotThrow(() -> resultSetAdapter.updateInt("label", 1));
  }

  @Test
  public void testUpdateLong() {
    assertDoesNotThrow(() -> resultSetAdapter.updateLong(1, 1L));
    assertDoesNotThrow(() -> resultSetAdapter.updateLong("label", 1L));
  }

  @Test
  public void testUpdateFloat() {
    assertDoesNotThrow(() -> resultSetAdapter.updateFloat(1, 1.0f));
    assertDoesNotThrow(() -> resultSetAdapter.updateFloat("label", 1.0f));
  }

  @Test
  public void testUpdateDouble() {
    assertDoesNotThrow(() -> resultSetAdapter.updateDouble(1, 1.0d));
    assertDoesNotThrow(() -> resultSetAdapter.updateDouble("label", 1.0d));
  }

  @Test
  public void testUpdateBigDecimal() {
    assertDoesNotThrow(() -> resultSetAdapter.updateBigDecimal(1, BigDecimal.ONE));
    assertDoesNotThrow(() -> resultSetAdapter.updateBigDecimal("label", BigDecimal.ONE));
  }

  @Test
  public void testUpdateString() {
    assertDoesNotThrow(() -> resultSetAdapter.updateString(1, "test"));
    assertDoesNotThrow(() -> resultSetAdapter.updateString("label", "test"));
  }

  @Test
  public void testUpdateBytes() {
    assertDoesNotThrow(() -> resultSetAdapter.updateBytes(1, new byte[0]));
    assertDoesNotThrow(() -> resultSetAdapter.updateBytes("label", new byte[0]));
  }

  @Test
  public void testUpdateDate() {
    assertDoesNotThrow(() -> resultSetAdapter.updateDate(1, new Date(0)));
    assertDoesNotThrow(() -> resultSetAdapter.updateDate("label", new Date(0)));
  }

  @Test
  public void testUpdateTime() {
    assertDoesNotThrow(() -> resultSetAdapter.updateTime(1, new Time(0)));
    assertDoesNotThrow(() -> resultSetAdapter.updateTime("label", new Time(0)));
  }

  @Test
  public void testUpdateTimestamp() {
    assertDoesNotThrow(() -> resultSetAdapter.updateTimestamp(1, new Timestamp(0)));
    assertDoesNotThrow(() -> resultSetAdapter.updateTimestamp("label", new Timestamp(0)));
  }

  @Test
  public void testUpdateAsciiStream() {
    assertDoesNotThrow(() -> resultSetAdapter.updateAsciiStream(1, null, 0));
    assertDoesNotThrow(() -> resultSetAdapter.updateAsciiStream("label", null, 0));
    assertDoesNotThrow(() -> resultSetAdapter.updateAsciiStream(1, null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateAsciiStream("label", null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateAsciiStream(1, null));
    assertDoesNotThrow(() -> resultSetAdapter.updateAsciiStream("label", null));
  }

  @Test
  public void testUpdateBinaryStream() {
    assertDoesNotThrow(() -> resultSetAdapter.updateBinaryStream(1, null, 0));
    assertDoesNotThrow(() -> resultSetAdapter.updateBinaryStream("label", null, 0));
    assertDoesNotThrow(() -> resultSetAdapter.updateBinaryStream(1, null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateBinaryStream("label", null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateBinaryStream(1, null));
    assertDoesNotThrow(() -> resultSetAdapter.updateBinaryStream("label", null));
  }

  @Test
  public void testUpdateCharacterStream() {
    assertDoesNotThrow(() -> resultSetAdapter.updateCharacterStream(1, null, 0));
    assertDoesNotThrow(() -> resultSetAdapter.updateCharacterStream("label", null, 0));
    assertDoesNotThrow(() -> resultSetAdapter.updateCharacterStream(1, null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateCharacterStream("label", null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateCharacterStream(1, null));
    assertDoesNotThrow(() -> resultSetAdapter.updateCharacterStream("label", null));
  }

  @Test
  public void testUpdateObject() {
    assertDoesNotThrow(() -> resultSetAdapter.updateObject(1, null, 0));
    assertDoesNotThrow(() -> resultSetAdapter.updateObject("label", null, 0));
    assertDoesNotThrow(() -> resultSetAdapter.updateObject(1, null));
    assertDoesNotThrow(() -> resultSetAdapter.updateObject("label", null));
  }

  @Test
  public void testInsertRow() {
    assertDoesNotThrow(() -> resultSetAdapter.insertRow());
  }

  @Test
  public void testUpdateRow() {
    assertDoesNotThrow(() -> resultSetAdapter.updateRow());
  }

  @Test
  public void testDeleteRow() {
    assertDoesNotThrow(() -> resultSetAdapter.deleteRow());
  }

  @Test
  public void testRefreshRow() {
    assertDoesNotThrow(() -> resultSetAdapter.refreshRow());
  }

  @Test
  public void testCancelRowUpdates() {
    assertDoesNotThrow(() -> resultSetAdapter.cancelRowUpdates());
  }

  @Test
  public void testMoveToInsertRow() {
    assertDoesNotThrow(() -> resultSetAdapter.moveToInsertRow());
  }

  @Test
  public void testMoveToCurrentRow() {
    assertDoesNotThrow(() -> resultSetAdapter.moveToCurrentRow());
  }

  @Test
  public void testGetStatement() throws SQLException {
    assertNull(resultSetAdapter.getStatement());
  }

  @Test
  public void testGetRef() throws SQLException {
    assertNull(resultSetAdapter.getRef(1));
    assertNull(resultSetAdapter.getRef("label"));
  }

  @Test
  public void testGetBlob() throws SQLException {
    assertNull(resultSetAdapter.getBlob(1));
    assertNull(resultSetAdapter.getBlob("label"));
  }

  @Test
  public void testGetClob() throws SQLException {
    assertNull(resultSetAdapter.getClob(1));
    assertNull(resultSetAdapter.getClob("label"));
  }

  @Test
  public void testGetArray() throws SQLException {
    assertNull(resultSetAdapter.getArray(1));
    assertNull(resultSetAdapter.getArray("label"));
  }

  @Test
  public void testGetURL() throws SQLException, MalformedURLException {
    assertNull(resultSetAdapter.getURL(1));
    assertNull(resultSetAdapter.getURL("label"));
  }

  @Test
  public void testUpdateRef() {
    assertDoesNotThrow(() -> resultSetAdapter.updateRef(1, null));
    assertDoesNotThrow(() -> resultSetAdapter.updateRef("label", null));
  }

  @Test
  public void testUpdateBlob() {
    assertDoesNotThrow(() -> resultSetAdapter.updateBlob(1, (Blob) null));
    assertDoesNotThrow(() -> resultSetAdapter.updateBlob("label", (Blob) null));
    assertDoesNotThrow(() -> resultSetAdapter.updateBlob(1, (InputStream) null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateBlob("label", (InputStream) null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateBlob(1, (InputStream) null));
    assertDoesNotThrow(() -> resultSetAdapter.updateBlob("label", (InputStream) null));
  }

  @Test
  public void testUpdateClob() {
    assertDoesNotThrow(() -> resultSetAdapter.updateClob(1, (Clob) null));
    assertDoesNotThrow(() -> resultSetAdapter.updateClob("label", (Clob) null));
    assertDoesNotThrow(() -> resultSetAdapter.updateClob(1, (Reader) null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateClob("label", (Reader) null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateClob(1, (Reader) null));
    assertDoesNotThrow(() -> resultSetAdapter.updateClob("label", (Reader) null));
  }

  @Test
  public void testUpdateArray() {
    assertDoesNotThrow(() -> resultSetAdapter.updateArray(1, null));
    assertDoesNotThrow(() -> resultSetAdapter.updateArray("label", null));
  }

  @Test
  public void testGetRowId() throws SQLException {
    assertNull(resultSetAdapter.getRowId(1));
    assertNull(resultSetAdapter.getRowId("label"));
  }

  @Test
  public void testUpdateRowId() {
    assertDoesNotThrow(() -> resultSetAdapter.updateRowId(1, null));
    assertDoesNotThrow(() -> resultSetAdapter.updateRowId("label", null));
  }

  @Test
  public void testGetHoldability() throws SQLException {
    assertEquals(0, resultSetAdapter.getHoldability());
  }

  @Test
  public void testIsClosed() throws SQLException {
    assertFalse(resultSetAdapter.isClosed());
  }

  @Test
  public void testUpdateNString() {
    assertDoesNotThrow(() -> resultSetAdapter.updateNString(1, "test"));
    assertDoesNotThrow(() -> resultSetAdapter.updateNString("label", "test"));
  }

  @Test
  public void testUpdateNClob() {
    assertDoesNotThrow(() -> resultSetAdapter.updateNClob(1, (NClob) null));
    assertDoesNotThrow(() -> resultSetAdapter.updateNClob("label", (NClob) null));
    assertDoesNotThrow(() -> resultSetAdapter.updateNClob(1, (Reader) null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateNClob("label", (Reader) null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateNClob(1, (Reader) null));
    assertDoesNotThrow(() -> resultSetAdapter.updateNClob("label", (Reader) null));
  }

  @Test
  public void testGetNClob() throws SQLException {
    assertNull(resultSetAdapter.getNClob(1));
    assertNull(resultSetAdapter.getNClob("label"));
  }

  @Test
  public void testGetSqlXml() throws SQLException {
    assertNull(resultSetAdapter.getSQLXML(1));
    assertNull(resultSetAdapter.getSQLXML("label"));
  }

  @Test
  public void testUpdateSqlXml() {
    assertDoesNotThrow(() -> resultSetAdapter.updateSQLXML(1, null));
    assertDoesNotThrow(() -> resultSetAdapter.updateSQLXML("label", null));
  }

  @Test
  public void testGetNString() throws SQLException {
    assertEquals("", resultSetAdapter.getNString(1));
    assertEquals("", resultSetAdapter.getNString("label"));
  }

  @Test
  public void testGetNCharacterStream() throws SQLException {
    assertNull(resultSetAdapter.getNCharacterStream(1));
    assertNull(resultSetAdapter.getNCharacterStream("label"));
  }

  @Test
  public void testUpdateNCharacterStream() {
    assertDoesNotThrow(() -> resultSetAdapter.updateNCharacterStream(1, null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateNCharacterStream("label", null, 0L));
    assertDoesNotThrow(() -> resultSetAdapter.updateNCharacterStream(1, null));
    assertDoesNotThrow(() -> resultSetAdapter.updateNCharacterStream("label", null));
  }

  @Test
  public void testUnwrap() throws SQLException {
    assertNull(resultSetAdapter.unwrap(ResultSet.class));
  }

  @Test
  public void testIsWrapperFor() throws SQLException {
    assertFalse(resultSetAdapter.isWrapperFor(ResultSet.class));
  }
}
