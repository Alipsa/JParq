package jparq.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqSql;

/**
 * Tests covering schema handling and default schema behavior.
 */
class SchemaHandlingTest {

  @Test
  void shouldExposeSchemasFromDirectories(@TempDir Path tempDir) throws Exception {
    writeStringTable(tempDir.resolve("employees.parquet"), "name", "public");
    writeStringTable(tempDir.resolve("salary.parquet"), "name", "public-salary");

    Path carsDir = tempDir.resolve("cars");
    Files.createDirectories(carsDir);
    writeStringTable(carsDir.resolve("mtcars.parquet"), "name", "cars");
    Path nested = carsDir.resolve("foo");
    Files.createDirectories(nested);
    writeStringTable(nested.resolve("diamonds.parquet"), "name", "ignored");

    Path publicDir = tempDir.resolve("public");
    Files.createDirectories(publicDir);
    writeStringTable(publicDir.resolve("perks.parquet"), "name", "public-perks");

    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    Map<String, Set<String>> tablesBySchema = new LinkedHashMap<>();
    sql.query("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME",
        rs -> {
          try {
            while (rs.next()) {
              String schema = rs.getString("TABLE_SCHEMA");
              String tableName = rs.getString("TABLE_NAME");
              tablesBySchema.computeIfAbsent(schema, k -> new LinkedHashSet<>()).add(tableName);
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });

    assertNotNull(tablesBySchema.get("PUBLIC"), "PUBLIC schema should be present");
    assertEquals(Set.of("employees", "perks", "salary"), tablesBySchema.get("PUBLIC"));
    assertNotNull(tablesBySchema.get("CARS"), "CARS schema should be present");
    assertEquals(Set.of("mtcars"), tablesBySchema.get("CARS"));
    assertFalse(tablesBySchema.values().stream().anyMatch(set -> set.contains("diamonds")),
        "Nested directories should be ignored");
    assertEquals("1", fetchSingleString("jdbc:jparq:" + tempDir.toAbsolutePath(), "SELECT COUNT(*) FROM perks"));

    JParqSql caseSensitiveSql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath() + "?caseSensitive=true");
    List<String> schemas = new ArrayList<>();
    caseSensitiveSql.query("SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA", rs -> {
      try {
        while (rs.next()) {
          schemas.add(rs.getString(1));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    assertTrue(schemas.contains("cars"), "Schema casing should be preserved when caseSensitive=true");
    assertTrue(schemas.contains("PUBLIC"), "Default schema should always be present");
  }

  @Test
  void shouldDefaultToPublicWhenSchemaIsOmitted(@TempDir Path tempDir) throws Exception {
    writeStringTable(tempDir.resolve("employees.parquet"), "name", "public-row");
    Path salesDir = tempDir.resolve("sales");
    Files.createDirectories(salesDir);
    writeStringTable(salesDir.resolve("employees.parquet"), "name", "sales-row");
    Path carsDir = tempDir.resolve("cars");
    Files.createDirectories(carsDir);
    writeStringTable(carsDir.resolve("mtcars.parquet"), "name", "cars-row");

    String jdbcUrl = "jdbc:jparq:" + tempDir.toAbsolutePath();
    assertEquals("public-row", fetchSingleString(jdbcUrl, "SELECT name FROM employees"));
    assertEquals("sales-row", fetchSingleString(jdbcUrl, "SELECT name FROM sales.employees"));
    assertEquals("sales-row", fetchSingleString(jdbcUrl, "SELECT name FROM SALES.employees"));
    assertEquals("1", fetchSingleString(jdbcUrl, "SELECT COUNT(*) FROM CARS.mtcars"));

    try (Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT name FROM employees")) {
      ResultSetMetaData meta = rs.getMetaData();
      assertEquals("PUBLIC", meta.getSchemaName(1));
      assertEquals(tempDir.getFileName().toString(), meta.getCatalogName(1));
      var tables = conn.getMetaData().getTables(null, null, "employees", null);
      assertTrue(tables.next(), "employees table should be listed");
      assertEquals("PUBLIC", tables.getString("TABLE_SCHEM"));
      tables.close();
    }
  }

  private String fetchSingleString(String jdbcUrl, String query) throws SQLException {
    try (Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Expected at least one row for " + query);
      String value = rs.getString(1);
      assertFalse(rs.next(), "Expected a single row for " + query);
      return value;
    }
  }

  private void writeStringTable(Path file, String columnName, String value) throws IOException {
    Files.createDirectories(file.getParent());
    Schema schema = SchemaBuilder.record("row").fields().name(columnName).type().stringType().noDefault().endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put(columnName, value);
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(HadoopOutputFile.fromPath(hadoopPath, new Configuration())).withSchema(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      writer.write(record);
    }
  }
}
