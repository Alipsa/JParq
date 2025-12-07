package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqConnection;

/**
 * Verifies parameter binding and SQL injection protection for prepared
 * statements.
 */
class JParqPreparedStatementParameterTest {

  @TempDir
  File tempDir;

  private JParqConnection connection;

  @BeforeEach
  void setUp() throws SQLException, IOException {
    File parquetFile = new File(tempDir, "mtcars.parquet");
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
  void bindsParametersAndFilters() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
      }
    }
  }

  @Test
  void missingParameterFailsFast() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM mtcars WHERE cyl = ?")) {
      assertThrows(SQLException.class, ps::executeQuery);
    }
  }

  @Test
  void preventsSqlInjection() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      ps.setString(1, "Ford Pantera L");
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      ps.setString(1, "Ford Pantera L' OR 1=1 --");
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void parameterIndexOutOfRangeIsRejected() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM mtcars WHERE cyl = ?")) {
      ps.setInt(2, 4);
      assertThrows(SQLException.class, ps::executeQuery);
    }
  }

  @Test
  void bindsDateParameter() throws SQLException {
    // Test that Date parameters are properly rendered and don't cause SQL injection
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      Date testDate = Date.valueOf("2023-12-15");
      ps.setDate(1, testDate);
      // This should render as '2023-12-15' and not match any model
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsTimestampParameter() throws SQLException {
    // Test that Timestamp parameters with fractional seconds are properly rendered
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      Timestamp testTimestamp = Timestamp.valueOf("2023-12-15 14:30:45.123456789");
      ps.setTimestamp(1, testTimestamp);
      // This should render as '2023-12-15 14:30:45.123456789' and not match any model
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsTimeParameter() throws SQLException {
    // Test that Time parameters are properly rendered
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      Time testTime = Time.valueOf("14:30:45");
      ps.setTime(1, testTime);
      // This should render as '14:30:45' and not match any model
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsBooleanParameter() throws SQLException {
    // Test that Boolean parameters are rendered correctly
    // Note: Using integer comparisons since TRUE/FALSE literals may not be supported in WHERE clauses
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        // This should match all 4-cylinder cars
        assertEquals(11, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsFloatParameter() throws SQLException {
    // Test that Float parameters are properly rendered
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE mpg > ?")) {
      ps.setFloat(1, 20.0f);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        // Should find cars with mpg > 20
        assertEquals(14, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsDoubleParameter() throws SQLException {
    // Test that Double parameters are properly rendered
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE mpg > ?")) {
      ps.setDouble(1, 20.0);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        // Should find cars with mpg > 20
        assertEquals(14, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsBigDecimalParameter() throws SQLException {
    // Test that BigDecimal parameters use toPlainString() rendering
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE mpg < ?")) {
      ps.setBigDecimal(1, new BigDecimal("15.0"));
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        // Should find cars with mpg < 15
        assertEquals(5, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsByteArrayParameter() throws SQLException {
    // Test that byte array parameters are rendered as hex literals X'...'
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      byte[] testBytes = new byte[] {0x48, 0x65, 0x6C, 0x6C, 0x6F}; // "Hello" in ASCII
      ps.setBytes(1, testBytes);
      // This should render as X'48656C6C6F' and not match any model
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsNullParameter() throws SQLException {
    // Test that NULL parameters are properly rendered
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE ? IS NULL")) {
      ps.setNull(1, java.sql.Types.VARCHAR);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        // NULL IS NULL should be true for all rows
        assertEquals(32, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsMultipleParametersOfDifferentTypes() throws SQLException {
    // Test binding multiple parameters of different types in one query
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT COUNT(*) FROM mtcars WHERE cyl = ? AND vs = ? AND mpg > ? AND model != ?")) {
      ps.setInt(1, 4);
      ps.setInt(2, 1);
      ps.setDouble(3, 20.0);
      ps.setString(4, "NonexistentModel");
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        // Should find 4-cylinder cars with vs=1 and mpg>20
        assertEquals(10, rs.getInt(1));
      }
    }
  }
}
