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
  public void testGetSchemas(@TempDir File tempDir) throws SQLException, IOException {
    // Create some subdirectories to represent schemas
    new File(tempDir, "schema1").mkdir();
    new File(tempDir, "schema2").mkdir();
    File schema3 = new File(tempDir, "schema3");
    schema3.mkdir();
    new File(schema3, "nested").mkdir();

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
        assertEquals("schema1", schemas.get(1));
        assertEquals("schema2", schemas.get(2));
        assertEquals("schema3", schemas.get(3));
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
    new File(tempDir, "schema1").mkdir();
    new File(tempDir, "schema2").mkdir();
    new File(tempDir, "another").mkdir();

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
        assertEquals("schema1", schemas.get(0));
        assertEquals("schema2", schemas.get(1));
      }
    }
  }
}
