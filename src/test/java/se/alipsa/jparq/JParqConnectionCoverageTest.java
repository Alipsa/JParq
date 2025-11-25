package se.alipsa.jparq;

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
  void prepareStatementVariantsReturnDefaults() throws SQLException {
    assertNull(connection.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertNull(connection.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT));
    assertNull(connection.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
    assertNull(connection.prepareStatement("SELECT 1", new int[]{
        1
    }));
    assertNull(connection.prepareStatement("SELECT 1", new String[]{
        "id"
    }));
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareCall("SELECT 1"));
    assertNull(connection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertNull(connection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT));
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
    assertEquals(0, connection.getHoldability());
    connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    connection.setTypeMap(new java.util.HashMap<>());
    assertTrue(connection.getTypeMap().isEmpty());
    assertNull(connection.unwrap(Object.class));
    assertFalse(connection.isWrapperFor(Object.class));
  }
}
