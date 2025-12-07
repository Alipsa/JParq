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
  void handlesEscapedQuotesInSqlString() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT * FROM mtcars WHERE model = 'O''Brien' AND cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        // Should execute without treating the escaped quote as end of string
        // No results expected since there's no model called "O'Brien" in mtcars
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void handlesMultipleEscapedQuotesAndParameters() throws SQLException {
    // Test with escaped quotes before and after placeholder
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT * FROM mtcars WHERE model = 'O''Brien''s Car' AND cyl = ? AND hp > ?")) {
      ps.setInt(1, 4);
      ps.setInt(2, 100);
      try (ResultSet rs = ps.executeQuery()) {
        // Should correctly identify 2 placeholders despite multiple escaped quotes
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void handlesQuestionMarkInsideEscapedQuoteString() throws SQLException {
    // Test with ? inside a string literal containing escaped quotes
    try (PreparedStatement ps = connection.prepareStatement(
        "SELECT * FROM mtcars WHERE model = 'O''Brien?''s' AND cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        // Should identify only 1 placeholder (the ? inside the string is not a placeholder)
        assertFalse(rs.next());
      }
    }
  }
}
