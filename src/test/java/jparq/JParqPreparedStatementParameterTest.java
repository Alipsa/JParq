package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
  void syntaxErrorDetectedAtPreparationTime() {
    // Verify that syntax errors are caught early during prepareStatement(),
    // not deferred to executeQuery() - this is the key benefit of two-phase
    // planning
    SQLException exception = assertThrows(SQLException.class,
        () -> connection.prepareStatement("SELECT * FORM mtcars WHERE cyl = ?"));
    assertNotNull(exception.getMessage());
    assertTrue(exception.getMessage().contains("Failed to prepare query")
        || exception.getMessage().toLowerCase().contains("syntax")
        || exception.getMessage().toLowerCase().contains("parse"));
  }
}
