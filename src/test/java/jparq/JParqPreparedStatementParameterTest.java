package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.Date;
import java.sql.ParameterMetaData;
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
  void handlesEscapedQuotesInSqlString() throws SQLException {
    try (PreparedStatement ps = connection
        .prepareStatement("SELECT * FROM mtcars WHERE model = 'O''Brien' AND cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void bindsDateParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      Date testDate = Date.valueOf("2023-12-15");
      ps.setDate(1, testDate);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void exposesBasicParameterMetadata() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM mtcars WHERE cyl = ? AND model = ?")) {
      ParameterMetaData metaData = ps.getParameterMetaData();
      assertNotNull(metaData);
      assertEquals(2, metaData.getParameterCount());
      assertEquals(ParameterMetaData.parameterNullableUnknown, metaData.isNullable(1));
      assertEquals("VARCHAR", metaData.getParameterTypeName(2));
      assertEquals(String.class.getName(), metaData.getParameterClassName(1));
      assertEquals(ParameterMetaData.parameterModeIn, metaData.getParameterMode(2));
    }
  }

  @Test
  void handlesMultipleEscapedQuotesAndParameters() throws SQLException {
    try (PreparedStatement ps = connection
        .prepareStatement("SELECT * FROM mtcars WHERE model = 'O''Brien''s Car' AND cyl = ? AND hp > ?")) {
      ps.setInt(1, 4);
      ps.setInt(2, 100);
      try (ResultSet rs = ps.executeQuery()) {
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void bindsTimestampParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      Timestamp testTimestamp = Timestamp.valueOf("2023-12-15 14:30:45.123456789");
      ps.setTimestamp(1, testTimestamp);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsTimeParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      Time testTime = Time.valueOf("14:30:45");
      ps.setTime(1, testTime);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsBigDecimalParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE mpg < ?")) {
      ps.setBigDecimal(1, new BigDecimal("15.0"));
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsByteArrayParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      byte[] testBytes = new byte[]{
          0x48, 0x65, 0x6C, 0x6C, 0x6F
      };
      ps.setBytes(1, testBytes);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsNullParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE ? IS NULL")) {
      ps.setNull(1, java.sql.Types.VARCHAR);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(32, rs.getInt(1));
      }
    }
  }

  @Test
  void handlesQuestionMarkInsideEscapedQuoteString() throws SQLException {
    try (PreparedStatement ps = connection
        .prepareStatement("SELECT * FROM mtcars WHERE model = 'O''Brien?''s' AND cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void bindsMultipleParametersOfDifferentTypes() throws SQLException {
    try (PreparedStatement ps = connection
        .prepareStatement("SELECT COUNT(*) FROM mtcars WHERE cyl = ? AND vs = ? AND mpg > ? AND model != ?")) {
      ps.setInt(1, 4);
      ps.setInt(2, 1);
      ps.setDouble(3, 20.0);
      ps.setString(4, "NonexistentModel");
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
      }
    }
  }

  @Test
  void syntaxErrorDetectedAtPreparationTime() {
    SQLException exception = assertThrows(SQLException.class,
        () -> connection.prepareStatement("SELECT * FORM mtcars WHERE cyl = ?"));
    assertNotNull(exception.getMessage());
    assertTrue(exception.getMessage().toLowerCase().contains("failed to prepare query")
        || exception.getMessage().toLowerCase().contains("syntax")
        || exception.getMessage().toLowerCase().contains("parse"));
  }
}
