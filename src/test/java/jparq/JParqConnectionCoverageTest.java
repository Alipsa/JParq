package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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

/** Additional coverage for {@link JParqConnection}. */
class JParqConnectionCoverageTest {

  private JParqConnection connection;

  @TempDir
  File tempDir;

  @BeforeEach
  void setUp() throws SQLException, IOException {
    File parquetFile = new File(tempDir, "cars.parquet");
    try (var in = getClass().getResourceAsStream("/datasets/mtcars.parquet")) {
      Files.copy(in, parquetFile.toPath());
    }
    connection = new JParqConnection("jdbc:jparq:" + tempDir.getAbsolutePath(), new Properties());
  }

  @AfterEach
  void tearDown() throws SQLException {
    connection.close();
  }

  @Test
  void prepareStatementVariantsValidateOptions() throws SQLException {
    String sql = "SELECT * FROM cars";
    try (var ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      assertNotNull(ps);
    }
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
    try (var ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
      assertNotNull(ps);
    }
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement(sql,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS));
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement(sql, new int[]{
        1
    }));
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement(sql, new String[]{
        "id"
    }));
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareCall(sql));
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
  }

  @Test
  void defaultFactoriesAndSettings() throws SQLException {
    assertNull(connection.getWarnings());
    connection.clearWarnings();
    assertNull(connection.createClob());
    assertNull(connection.createBlob());
    assertNull(connection.createNClob());
    assertNull(connection.createSQLXML());
    assertFalse(connection.isValid(0));
    connection.setClientInfo("k", "v");
    connection.setClientInfo(new Properties());
    assertEquals("", connection.getClientInfo("k"));
    assertNotNull(connection.getClientInfo());
    assertNull(connection.createArrayOf("VARCHAR", new Object[0]));
    assertNull(connection.createStruct("TYPE", new Object[0]));
    connection.setSchema("PUBLIC");
    assertEquals("", connection.getSchema());
    connection.abort(command -> command.run());
    connection.setNetworkTimeout(command -> command.run(), 10);
    assertEquals(0, connection.getNetworkTimeout());
    assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, connection.getHoldability());
    connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    connection.setTypeMap(new java.util.HashMap<>());
    assertTrue(connection.getTypeMap().isEmpty());
    assertNull(connection.unwrap(Object.class));
    assertFalse(connection.isWrapperFor(Object.class));
  }
}
