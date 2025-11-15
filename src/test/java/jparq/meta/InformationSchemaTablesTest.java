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
import java.util.LinkedHashMap;
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

  /**
   * Initialize the reusable {@link JParqSql} instance that points to the Parquet
   * files bundled with the test resources.
   *
   * @throws URISyntaxException
   *           if the {@code mtcars.parquet} resource cannot be resolved to a
   *           {@link java.nio.file.Path}
   */
  @BeforeAll
  static void init() throws URISyntaxException {
    URL mtcarsUrl = InformationSchemaTablesTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must exist on the test classpath");
    Path dir = Paths.get(mtcarsUrl.toURI()).getParent();
    resourceSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Ensure INFORMATION_SCHEMA.TABLES exposes all Parquet tables stored in the
   * resource directory and that they surface as {@code BASE TABLE} entries.
   */
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

  /**
   * Verify that REMARKS values are populated when the Parquet footer contains a
   * {@code comment} entry.
   *
   * @param tempDir
   *          temporary directory used to create a dedicated Parquet file
   * @throws Exception
   *           if the Parquet file cannot be written or read
   */
  @Test
  void shouldExposeRemarksWhenAvailable(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("doc_table.parquet");
    writeCommentedTable(file, "doc_table", "Stores documentation friendly data");
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    assertSingleRemarkRow(sql, "doc_table", "Stores documentation friendly data");
  }

  /**
   * Confirm that uppercase fully qualified identifiers are normalized so the
   * information schema view can be queried regardless of input casing.
   *
   * @param tempDir
   *          directory that temporarily hosts the test Parquet file
   * @throws Exception
   *           if the Parquet file cannot be created
   */
  @Test
  void shouldQueryUppercaseQualifiedReference(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("upper_ref.parquet");
    writeCommentedTable(file, "upper_ref", "Upper reference");
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    sql.query("SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'upper_ref'", rs -> {
      try {
        assertTrue(rs.next(), "expected row for COUNT(*)");
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next(), "COUNT(*) should return a single row");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  /**
   * Validate that remark keys are matched irrespective of their casing so that
   * uppercase entries still surface in the REMARKS column.
   *
   * @param tempDir
   *          temporary directory populated with a single Parquet file
   * @throws Exception
   *           if the Parquet file cannot be written
   */
  @Test
  void shouldReadRemarksCaseInsensitive(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("upper_comment.parquet");
    writeTableWithMetadata(file, "upper_comment", null, Map.of("COMMENT", "Uppercase remark"));
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    assertSingleRemarkRow(sql, "upper_comment", "Uppercase remark");
  }

  /**
   * Ensure doc metadata keys are recognized, mirroring how several serializers
   * expose table descriptions.
   *
   * @param tempDir
   *          directory used for the temporary Parquet file
   * @throws Exception
   *           if the file cannot be written
   */
  @Test
  void shouldReadDocMetadataKey(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("doc_comment.parquet");
    writeTableWithMetadata(file, "doc_comment", null, Map.of("DoC", "Doc style remark"));
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    assertSingleRemarkRow(sql, "doc_comment", "Doc style remark");
  }

  /**
   * Guarantee that {@code description} metadata entries take precedence over the
   * {@code doc} fallback.
   *
   * @param tempDir
   *          directory holding the generated Parquet file
   * @throws Exception
   *           if writing or querying the file fails
   */
  @Test
  void shouldPrioritizeDescriptionOverDoc(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("description_comment.parquet");
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("DOC", "Doc fallback");
    metadata.put("description", "Description remark");
    writeTableWithMetadata(file, "description_comment", null, metadata);
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    assertSingleRemarkRow(sql, "description_comment", "Description remark");
  }

  /**
   * Confirm that {@code parquet.schema.comment} entries are treated as remarks,
   * matching how schema aware tooling stores table descriptions.
   *
   * @param tempDir
   *          directory that contains the generated Parquet test file
   * @throws Exception
   *           if the Parquet file or metadata cannot be written
   */
  @Test
  void shouldReadParquetSchemaCommentKey(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("schema_comment.parquet");
    writeTableWithMetadata(file, "schema_comment", null, Map.of("parquet.schema.comment", "Schema level comment"));
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    assertSingleRemarkRow(sql, "schema_comment", "Schema level comment");
  }

  /**
   * Write a single row Parquet file with an optional {@code comment} metadata
   * entry.
   *
   * @param file
   *          destination path for the Parquet file
   * @param tableName
   *          logical table name stored in the schema
   * @param comment
   *          optional descriptive comment stored in the file metadata
   * @throws IOException
   *           if the Parquet file cannot be written
   */
  private void writeCommentedTable(Path file, String tableName, String comment) throws IOException {
    writeTableWithMetadata(file, tableName, comment, Map.of());
  }

  /**
   * Write a Parquet file with configurable metadata entries for remark extraction
   * tests.
   *
   * @param file
   *          destination path for the Parquet file
   * @param tableName
   *          logical table name stored in the schema
   * @param comment
   *          optional comment to attach, may be {@code null}
   * @param metadata
   *          extra metadata entries to persist alongside the comment
   * @throws IOException
   *           if the file cannot be written
   */
  private void writeTableWithMetadata(Path file, String tableName, String comment, Map<String, String> metadata)
      throws IOException {
    Schema schema = SchemaBuilder.record(tableName).fields().requiredInt("id").endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 1);
    Configuration conf = new Configuration(false);
    Map<String, String> meta = metadata == null ? Map.of() : metadata;
    if (comment != null && !comment.isBlank()) {
      meta = new LinkedHashMap<>(meta);
      meta.put("comment", comment);
    }
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.toUri())).withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).withSchema(schema).withExtraMetaData(meta).build()) {
      writer.write(record);
    }
  }

  /**
   * Assert that the information schema view exposes the expected remark for a
   * single table row.
   *
   * @param sql
   *          SQL helper connected to the directory containing the Parquet files
   * @param tableName
   *          table identifier to query
   * @param expectedRemark
   *          expected REMARKS column value
   */
  private void assertSingleRemarkRow(JParqSql sql, String tableName, String expectedRemark) {
    sql.query("SELECT TABLE_NAME, REMARKS FROM information_schema.tables WHERE TABLE_NAME = '" + tableName + "'",
        rs -> {
          try {
            assertTrue(rs.next(), () -> "expected a row for " + tableName);
            assertEquals(tableName, rs.getString("TABLE_NAME"));
            assertEquals(expectedRemark, rs.getString("REMARKS"));
            assertFalse(rs.next(), "only one table should be returned");
          } catch (SQLException e) {
            fail(e);
          }
        });
  }
}
