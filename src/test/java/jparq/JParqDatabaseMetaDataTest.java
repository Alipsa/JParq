package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqConnection;

public class JParqDatabaseMetaDataTest {

  @Test
  public void testGetSchemas(@TempDir File tempDir) throws SQLException {
    // Create some subdirectories to represent schemas
    assertTrue(new File(tempDir, "schema1").mkdir(), "Failed to create schema1 directory");
    assertTrue(new File(tempDir, "schema2").mkdir(), "Failed to create schema2 directory");
    File schema3 = new File(tempDir, "schema3");
    assertTrue(schema3.mkdir(), "Failed to create schema3 directory");
    assertTrue(new File(schema3, "nested").mkdir(), "Failed to create nested directory in schema3");

    String jdbcUrl = "jdbc:jparq:" + tempDir.getAbsolutePath();
    try (JParqConnection conn = (JParqConnection) DriverManager.getConnection(jdbcUrl)) {
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getSchemas()) {
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEM"));
        }
        Collections.sort(schemas);
        assertEquals(4, schemas.size());
        assertEquals("PUBLIC", schemas.get(0));
        assertEquals("SCHEMA1", schemas.get(1));
        assertEquals("SCHEMA2", schemas.get(2));
        assertEquals("SCHEMA3", schemas.get(3));
      }
    }
  }

  @Test
  public void testGetSchemasNoSubdirs(@TempDir File tempDir) throws SQLException {
    String jdbcUrl = "jdbc:jparq:" + tempDir.getAbsolutePath();
    try (JParqConnection conn = (JParqConnection) DriverManager.getConnection(jdbcUrl)) {
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getSchemas()) {
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEM"));
        }
        assertEquals(1, schemas.size());
        assertEquals("PUBLIC", schemas.get(0));
      }
    }
  }

  @Test
  public void testGetSchemasWithPattern(@TempDir File tempDir) throws SQLException, IOException {
    // Create some subdirectories to represent schemas
    assertTrue(new File(tempDir, "schema1").mkdir(), "Failed to create schema1 directory");
    assertTrue(new File(tempDir, "schema2").mkdir(), "Failed to create schema2 directory");
    assertTrue(new File(tempDir, "another").mkdir(), "Failed to create another directory");

    String jdbcUrl = "jdbc:jparq:" + tempDir.getAbsolutePath();
    try (JParqConnection conn = (JParqConnection) DriverManager.getConnection(jdbcUrl)) {
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getSchemas(null, "schema%")) {
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEM"));
        }
        Collections.sort(schemas);
        assertEquals(2, schemas.size());
        assertEquals("SCHEMA1", schemas.get(0));
        assertEquals("SCHEMA2", schemas.get(1));
      }
    }
  }

  @Test
  public void testGetSchemasCaseSensitive(@TempDir File tempDir) throws SQLException, IOException {
    // Create some subdirectories to represent schemas with mixed case
    new File(tempDir, "Schema1").mkdir();
    new File(tempDir, "SCHEMA2").mkdir();
    new File(tempDir, "schema3").mkdir();

    String jdbcUrl = "jdbc:jparq:" + tempDir.getAbsolutePath() + "?caseSensitive=true";
    try (JParqConnection conn = (JParqConnection) DriverManager.getConnection(jdbcUrl)) {
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getSchemas()) {
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEM"));
        }
        Collections.sort(schemas);
        assertEquals(4, schemas.size());
        assertEquals("PUBLIC", schemas.get(0));
        assertEquals("SCHEMA2", schemas.get(1));
        assertEquals("Schema1", schemas.get(2));
        assertEquals("schema3", schemas.get(3));
      }
    }
  }

  @Test
  public void testGetSchemasWithPublicSubdirNoDuplicates(@TempDir File tempDir) throws SQLException {
    // Create subdirectories including variants of "PUBLIC" to verify no duplicates
    new File(tempDir, "PUBLIC").mkdir();
    new File(tempDir, "schema1").mkdir();

    String jdbcUrl = "jdbc:jparq:" + tempDir.getAbsolutePath();
    try (JParqConnection conn = (JParqConnection) DriverManager.getConnection(jdbcUrl)) {
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getSchemas()) {
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEM"));
        }
        Collections.sort(schemas);
        // Should have PUBLIC (merged from default and subdirectory) and schema1
        assertEquals(2, schemas.size(), "PUBLIC subdirectory should not create duplicate schema");
        assertEquals("PUBLIC", schemas.get(0));
        assertEquals("schema1", schemas.get(1));
      }
    }
  }

  @Test
  public void testGetSchemasWithPublicCaseVariantsNoDuplicates(@TempDir File tempDir) throws SQLException {
    // Create subdirectories with case variants of "PUBLIC"
    new File(tempDir, "public").mkdir();
    new File(tempDir, "Public").mkdir();
    new File(tempDir, "schema1").mkdir();

    String jdbcUrl = "jdbc:jparq:" + tempDir.getAbsolutePath();
    try (JParqConnection conn = (JParqConnection) DriverManager.getConnection(jdbcUrl)) {
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getSchemas()) {
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEM"));
        }
        Collections.sort(schemas);
        // All case variants of PUBLIC should be normalized to a single entry
        assertEquals(2, schemas.size(), "Case variants of PUBLIC should not create duplicate schemas");
        assertEquals("PUBLIC", schemas.get(0));
        assertEquals("schema1", schemas.get(1));
      }
    }
  }
}
