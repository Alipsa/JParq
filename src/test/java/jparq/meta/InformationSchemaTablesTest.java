package jparq.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqSql;

/** Tests for querying INFORMATION_SCHEMA.TABLES. */
public class InformationSchemaTablesTest {

  private static JParqSql resourceSql;

  @BeforeAll
  static void init() throws URISyntaxException {
    URL mtcarsUrl = InformationSchemaTablesTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must exist on the test classpath");
    Path dir = Paths.get(mtcarsUrl.toURI()).getParent();
    resourceSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void shouldListTablesFromInformationSchema() {
    List<String> tables = new ArrayList<>();
    resourceSql.query("SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.tables ORDER BY TABLE_NAME", rs -> {
      try {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
          assertEquals("BASE TABLE", rs.getString("TABLE_TYPE"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    assertTrue(tables.contains("mtcars"), "information_schema.tables must list mtcars");
  }

  @Test
  void shouldExposeRemarksWhenAvailable(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("doc_table.parquet");
    writeCommentedTable(file, "doc_table", "Stores documentation friendly data");
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    sql.query("SELECT TABLE_NAME, REMARKS FROM information_schema.tables WHERE TABLE_NAME = 'doc_table'", rs -> {
      try {
        assertTrue(rs.next(), "expected a row for doc_table");
        assertEquals("doc_table", rs.getString("TABLE_NAME"));
        assertEquals("Stores documentation friendly data", rs.getString("REMARKS"));
        assertFalse(rs.next(), "only one table should be returned");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  private void writeCommentedTable(Path file, String tableName, String comment) throws IOException {
    Schema schema = SchemaBuilder.record(tableName).fields().requiredInt("id").endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 1);
    Configuration conf = new Configuration(false);
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.toUri())).withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).withSchema(schema)
        .withExtraMetaData(Map.of("comment", comment)).build()) {
      writer.write(record);
    }
  }
}
