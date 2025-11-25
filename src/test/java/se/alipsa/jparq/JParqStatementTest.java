package se.alipsa.jparq;

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
import org.mockito.Mockito;

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
  void testGetAndSetCurrentRs() {
    JParqResultSet rs = Mockito.mock(JParqResultSet.class);
    assertNull(statement.getCurrentRs());
    statement.setCurrentRs(rs);
    assertEquals(rs, statement.getCurrentRs());
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