package jparq;

import static org.junit.jupiter.api.Assertions.*;

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
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.JParqStatement;

public class JParqStatementTest {

  private JParqConnection connection;
  private JParqStatement statement;

  @TempDir
  File tempDir;

  private File parquetFile;

  @BeforeEach
  void setUp() throws SQLException, IOException {
    parquetFile = new File(tempDir, "test.parquet");
    try (var in = getClass().getResourceAsStream("/datasets/mtcars.parquet")) {
      Files.copy(in, parquetFile.toPath());
    }
    Properties props = new Properties();
    connection = new JParqConnection("jdbc:jparq:" + tempDir.getAbsolutePath(), props);
    statement = (JParqStatement) connection.createStatement();
  }

  @AfterEach
  void tearDown() throws SQLException {
    statement.close();
    connection.close();
  }

  @Test
  void testPrepare() throws SQLException {
    String sql = "SELECT * FROM test";
    PreparedStatement pstmt = statement.prepare(sql);
    assertNotNull(pstmt);
    assertTrue(pstmt instanceof JParqPreparedStatement);
    assertEquals(sql, statement.getCurrentSql());
  }

  @Test
  void testGetConn() {
    assertEquals(connection, statement.getConn());
  }

  @Test
  void testGetAndSetCurrentRs() throws SQLException {
    ResultSet rs = statement.executeQuery("SELECT * FROM test");
    assertEquals(rs, statement.getCurrentRs());
    statement.setCurrentRs(null);
    assertNull(statement.getCurrentRs());
    statement.setCurrentRs((JParqResultSet) rs);
    assertEquals(rs, statement.getCurrentRs());
    rs.close();
  }

  @Test
  void testExecuteQuery() throws SQLException {
    String sql = "SELECT * FROM test";
    ResultSet rs = statement.executeQuery(sql);
    assertNotNull(rs);
    assertEquals(rs, statement.getCurrentRs());
    assertEquals(sql, statement.getCurrentSql());
  }

  @Test
  void testExecuteUpdateThrowsException() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate("any"));
  }

  @Test
  void testBatchOperationsThrowException() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.addBatch("any"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.clearBatch());
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeBatch());
  }

  @Test
  void testClose() throws SQLException {
    String sql = "SELECT * FROM test";
    ResultSet rs = statement.executeQuery(sql);
    statement.close();
    assertTrue(rs.isClosed());
  }

  @Test
  void testExecute() throws SQLException {
    String sql = "SELECT * FROM test";
    assertTrue(statement.execute(sql));
  }

  @Test
  void testGetResultSet() throws SQLException {
    String sql = "SELECT * FROM test";
    statement.execute(sql);
    assertNotNull(statement.getResultSet());
  }

  @Test
  void testGetUpdateCount() {
    assertEquals(-1, statement.getUpdateCount());
  }

  @Test
  void testGetMoreResults() throws SQLException {
    assertFalse(statement.getMoreResults());
    assertFalse(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
  }

  @Test
  void testStatementDefaultsAndUnsupportedOperations() throws SQLException {
    assertEquals(0, statement.getMaxFieldSize());
    statement.setMaxFieldSize(10);
    assertEquals(0, statement.getMaxRows());
    statement.setMaxRows(5);
    statement.setEscapeProcessing(true);
    assertEquals(0, statement.getQueryTimeout());
    statement.setQueryTimeout(1);
    statement.cancel();
    assertNull(statement.getWarnings());
    statement.clearWarnings();
    statement.setCursorName("cur");
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> statement.execute("SELECT * FROM test", Statement.RETURN_GENERATED_KEYS));
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute("SELECT * FROM test", new int[]{
        1
    }));
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute("SELECT * FROM test", new String[]{
        "col"
    }));
    assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, statement.getResultSetHoldability());
    assertFalse(statement.isClosed());
    statement.setPoolable(true);
    assertFalse(statement.isPoolable());
    statement.closeOnCompletion();
    assertFalse(statement.isCloseOnCompletion());
    assertNull(statement.unwrap(Object.class));
    assertFalse(statement.isWrapperFor(Object.class));
  }

  @Test
  void testGetFetchDirection() {
    assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
  }

  @Test
  void testGetResultSetConcurrency() {
    assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
  }

  @Test
  void testGetResultSetType() {
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
  }

  @Test
  void testGetConnection() throws SQLException {
    assertEquals(connection, statement.getConnection());
  }
}
