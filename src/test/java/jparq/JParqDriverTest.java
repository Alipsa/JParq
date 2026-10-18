package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqDriver;
import se.alipsa.jparq.JParqVersion;

public class JParqDriverTest {

  private final JParqDriver driver = new JParqDriver();

  @Test
  public void testAcceptsURL() {
    assertTrue(driver.acceptsURL("jdbc:jparq:/some/path"));
    assertFalse(driver.acceptsURL("jdbc:other:/some/path"));
    assertFalse(driver.acceptsURL(null));
  }

  @Test
  public void testConnect() throws SQLException {
    Connection connection = driver.connect("jdbc:jparq:src/test/resources/datasets", new Properties());
    assertNotNull(connection);
    assertTrue(connection instanceof JParqConnection);
    connection.close();
  }

  @Test
  public void testConnectWithInvalidUrl() throws SQLException {
    assertNull(driver.connect("jdbc:other:/some/path", new Properties()));
  }

  @Test
  public void testGetPropertyInfo() {
    DriverPropertyInfo[] info = driver.getPropertyInfo("jdbc:jparq:/some/path", new Properties());
    assertEquals(4, info.length);
    assertEquals("datasetLayout", info[3].name);
    assertEquals("none", info[3].value);
    assertEquals("predicatePushdown", info[1].name);
    assertEquals("true", info[1].value);
    assertEquals("projectionPushdown", info[2].name);
    assertEquals("true", info[2].value);
    assertEquals("caseSensitive", info[0].name);
    assertEquals("false", info[0].value);
  }

  @Test
  public void testGetMajorVersion() {
    assertEquals(JParqVersion.getMajor(), driver.getMajorVersion());
  }

  @Test
  public void testGetMinorVersion() {
    assertEquals(JParqVersion.getMinor(), driver.getMinorVersion());
  }

  @Test
  public void testJdbcCompliant() {
    assertFalse(driver.jdbcCompliant());
  }

  @Test
  public void testGetParentLogger() {
    assertNotNull(driver.getParentLogger());
  }
}
