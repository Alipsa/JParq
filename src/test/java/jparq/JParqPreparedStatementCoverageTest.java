package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqPreparedStatement;

/** Additional coverage for {@link JParqPreparedStatement}. */
class JParqPreparedStatementCoverageTest {

  private JParqConnection connection;
  private PreparedStatement preparedStatement;

  @TempDir
  File tempDir;

  @BeforeEach
  void setUp() throws SQLException, IOException {
    File parquetFile = new File(tempDir, "cars.parquet");
    try (var in = getClass().getResourceAsStream("/datasets/mtcars.parquet")) {
      Files.copy(in, parquetFile.toPath());
    }
    connection = new JParqConnection("jdbc:jparq:" + tempDir.getAbsolutePath(), new Properties());
    preparedStatement = connection.prepareStatement("SELECT * FROM cars");
  }

  @AfterEach
  void tearDown() throws SQLException {
    preparedStatement.close();
    connection.close();
  }

  @Test
  void parameterSettersDoNotThrow() throws SQLException {
    preparedStatement.setString(1, "value");
    preparedStatement.setInt(2, 5);
    preparedStatement.setObject(3, "value");
    preparedStatement.setObject(4, "value", java.sql.Types.VARCHAR);
    preparedStatement.setObject(5, "value", java.sql.Types.VARCHAR, 10);
    preparedStatement.setNull(6, java.sql.Types.VARCHAR);
    preparedStatement.setNull(7, java.sql.Types.VARCHAR, "type");
    preparedStatement.setBoolean(8, true);
    preparedStatement.setByte(9, (byte) 1);
    preparedStatement.setShort(10, (short) 2);
    preparedStatement.setLong(11, 3L);
    preparedStatement.setFloat(12, 1.5f);
    preparedStatement.setDouble(13, 2.5);
    preparedStatement.setBigDecimal(14, java.math.BigDecimal.TEN);
    preparedStatement.setBytes(15, new byte[]{
        1, 2
    });
    preparedStatement.setDate(16, java.sql.Date.valueOf("2020-01-01"));
    preparedStatement.setDate(17, java.sql.Date.valueOf("2020-01-01"), null);
    preparedStatement.setTime(18, java.sql.Time.valueOf("10:00:00"));
    preparedStatement.setTime(19, java.sql.Time.valueOf("10:00:00"), null);
    preparedStatement.setTimestamp(20, java.sql.Timestamp.valueOf("2020-01-01 00:00:00"));
    preparedStatement.setTimestamp(21, java.sql.Timestamp.valueOf("2020-01-01 00:00:00"), null);
    preparedStatement.setAsciiStream(22, new ByteArrayInputStream(new byte[0]), 0);
    preparedStatement.setAsciiStream(23, new ByteArrayInputStream(new byte[0]), 0L);
    preparedStatement.setAsciiStream(24, new ByteArrayInputStream(new byte[0]));
    preparedStatement.setBinaryStream(25, new ByteArrayInputStream(new byte[0]), 0);
    preparedStatement.setBinaryStream(26, new ByteArrayInputStream(new byte[0]), 0L);
    preparedStatement.setBinaryStream(27, new ByteArrayInputStream(new byte[0]));
    preparedStatement.setCharacterStream(28, new java.io.StringReader("a"), 1);
    preparedStatement.setCharacterStream(29, new java.io.StringReader("b"), 1L);
    preparedStatement.setCharacterStream(30, new java.io.StringReader("c"));
    // preparedStatement.setUnicodeStream(31, new ByteArrayInputStream(new byte[0]),
    // 0);
    preparedStatement.setRef(32, null);
    preparedStatement.setBlob(33, (java.sql.Blob) null);
    preparedStatement.setBlob(34, new ByteArrayInputStream(new byte[0]), 0L);
    preparedStatement.setBlob(35, new ByteArrayInputStream(new byte[0]));
    preparedStatement.setClob(36, (java.sql.Clob) null);
    preparedStatement.setClob(37, new java.io.StringReader("c"), 1L);
    preparedStatement.setClob(38, new java.io.StringReader("c"));
    preparedStatement.setArray(39, null);
    preparedStatement.setURL(40, null);
    preparedStatement.setRowId(41, null);
    preparedStatement.setNString(42, "str");
    preparedStatement.setNCharacterStream(43, new java.io.StringReader("n"), 1L);
    preparedStatement.setNCharacterStream(44, new java.io.StringReader("n"));
    preparedStatement.setNClob(45, (java.sql.NClob) null);
    preparedStatement.setNClob(46, new java.io.StringReader("n"), 1L);
    preparedStatement.setNClob(47, new java.io.StringReader("n"));
    preparedStatement.setSQLXML(48, null);
    preparedStatement.clearParameters();
  }

  @Test
  void batchExecutionReturnsCountsAndClearsState() throws SQLException {
    preparedStatement.addBatch();
    preparedStatement.addBatch();
    int[] counts = preparedStatement.executeBatch();
    assertArrayEquals(new int[]{
        Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO
    }, counts);
    assertNull(preparedStatement.getResultSet());
    assertArrayEquals(new int[0], preparedStatement.executeBatch());
  }

  @Test
  void clearBatchRemovesQueuedEntries() throws SQLException {
    preparedStatement.addBatch();
    preparedStatement.clearBatch();
    assertArrayEquals(new int[0], preparedStatement.executeBatch());
  }

  @Test
  void addBatchWithSqlIsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> preparedStatement.addBatch("SELECT 1"));
  }

  @Test
  void executeUpdateVariantsAreUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> preparedStatement.executeUpdate());
    assertThrows(SQLFeatureNotSupportedException.class, () -> preparedStatement.executeUpdate("SELECT 1"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> preparedStatement.executeUpdate("SELECT 1", new int[0]));
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> preparedStatement.executeUpdate("SELECT 1", Statement.RETURN_GENERATED_KEYS));
    assertThrows(SQLFeatureNotSupportedException.class, () -> preparedStatement.executeUpdate("SELECT 1", new String[]{
        "col"
    }));
    assertThrows(SQLFeatureNotSupportedException.class, () -> preparedStatement.execute("SELECT 1"));
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> preparedStatement.execute("SELECT 1", Statement.RETURN_GENERATED_KEYS));
    assertThrows(SQLFeatureNotSupportedException.class, () -> preparedStatement.execute("SELECT 1", new int[]{
        1
    }));
    assertThrows(SQLFeatureNotSupportedException.class, () -> preparedStatement.execute("SELECT 1", new String[]{
        "col"
    }));
  }

  @Test
  void providesBasicStatementInformation() throws SQLException {
    ResultSet rs = preparedStatement.executeQuery();
    assertNotNull(rs);
    assertTrue(preparedStatement.execute());
    assertNotNull(preparedStatement.getResultSet());
    assertEquals(connection, preparedStatement.getConnection());
    assertEquals(0, preparedStatement.getUpdateCount());
    assertFalse(preparedStatement.getMoreResults());
    assertFalse(preparedStatement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
    assertEquals(ResultSet.FETCH_FORWARD, preparedStatement.getFetchDirection());
    assertEquals(0, preparedStatement.getFetchSize());
    assertNull(preparedStatement.getWarnings());
    preparedStatement.clearWarnings();
    preparedStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
    preparedStatement.setFetchSize(5);
    preparedStatement.setPoolable(true);
    preparedStatement.closeOnCompletion();
    assertFalse(preparedStatement.isCloseOnCompletion());
    assertFalse(preparedStatement.isPoolable());
    assertFalse(preparedStatement.isClosed());
    assertNull(preparedStatement.getMetaData());
    assertNull(preparedStatement.getParameterMetaData());
    rs.close();
  }
}
