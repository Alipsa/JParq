package se.alipsa.jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

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
    assertEquals(1, info.length);
    assertEquals("caseSensitive", info[0].name);
    assertEquals("false", info[0].value);
  }

  @Test
  public void testGetMajorVersion() {
    assertEquals(0, driver.getMajorVersion());
  }

  @Test
  public void testGetMinorVersion() {
    assertEquals(1, driver.getMinorVersion());
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
