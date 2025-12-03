package jparq.quoted;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqSql;

class QuotedIdentifierTest {

  private static JParqSql resourceSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = QuotedIdentifierTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");
    Path datasetPath = Paths.get(mtcarsUrl.toURI()).getParent();
    resourceSql = new JParqSql("jdbc:jparq:" + datasetPath.toAbsolutePath());
  }

  @Test
  void quotedCteRequiresExactCase() {
    resourceSql.query("""
        WITH "MixedCte" ("Val") AS (VALUES (1))
        SELECT "Val" FROM "MixedCte"
        """, rs -> {
      try {
        assertTrue(rs.next(), "Quoted CTE should resolve with matching case");
        assertEquals(1, rs.getInt(1), "Quoted column should retain its value");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });

    RuntimeException exception = assertThrows(RuntimeException.class, () -> resourceSql.query("""
        WITH "MixedCte" ("Val") AS (VALUES (1))
        SELECT "Val" FROM mixedcte
        """, rs -> {
    }));
    assertTrue(exception.getCause() instanceof SQLException, "Unquoted reference should not match quoted CTE");
  }

  @Test
  void quotedColumnNamesRemainCaseSensitive() {
    resourceSql.query("""
        WITH data("MixCase", plain) AS (VALUES (10, 20))
        SELECT "MixCase", plain FROM data
        """, rs -> {
      try {
        assertTrue(rs.next(), "Row should be available from quoted column CTE");
        assertEquals(10, rs.getInt("MixCase"), "Quoted column should be addressable with exact case");
        assertThrows(SQLException.class, () -> rs.getInt("mixcase"),
            "Mismatched case should not resolve quoted identifier");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  void quotedPhysicalColumnsRequireMatchingCase() {
    resourceSql.query("""
        SELECT "mpg" FROM mtcars
        """, rs -> {
      try {
        assertTrue(rs.next(), "Lower-case quoted column should match physical column");
        assertDoesNotThrow(() -> rs.getDouble("mpg"), "Lower-case quoted column should be retrievable");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });

    RuntimeException exception = assertThrows(RuntimeException.class, () -> resourceSql.query("""
        SELECT "MPG" FROM mtcars
        """, rs -> {
    }));
    assertTrue(exception.getCause() instanceof SQLException, "Quoted column with different case should not resolve");
  }

  @Test
  void metadataPatternsRespectQuotedIdentifiers(@TempDir Path tempDir) throws Exception {
    Schema schema = SchemaBuilder.record("CaseTable").fields().requiredInt("Id").requiredString("Value").endRecord();
    org.apache.hadoop.fs.Path output = new org.apache.hadoop.fs.Path(tempDir.resolve("CaseTable.parquet").toUri());
    Configuration conf = new Configuration(false);
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(HadoopOutputFile.fromPath(output, conf)).withConf(conf).withSchema(schema).build()) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("Id", 1);
      record.put("Value", "A");
      writer.write(record);
    }

    try (Connection conn = DriverManager.getConnection("jdbc:jparq:" + tempDir.toAbsolutePath())) {
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet tables = meta.getTables(conn.getCatalog(), null, "\"CaseTable\"", null)) {
        assertTrue(tables.next(), "Quoted pattern should match the mixed case table");
      }
      try (ResultSet tables = meta.getTables(conn.getCatalog(), null, "\"casetable\"", null)) {
        assertFalse(tables.next(), "Quoted pattern with different case should not match");
      }
      try (ResultSet columns = meta.getColumns(conn.getCatalog(), null, "\"CaseTable\"", "\"Id\"")) {
        assertTrue(columns.next(), "Quoted column pattern should match exact case");
        assertEquals("Id", columns.getString("COLUMN_NAME"), "Column name should retain original case");
      }
    }
  }
}
