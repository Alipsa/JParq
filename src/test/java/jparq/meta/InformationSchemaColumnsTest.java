package jparq.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqSql;

/** Tests for querying INFORMATION_SCHEMA.COLUMNS. */
public class InformationSchemaColumnsTest {

  /**
   * Ensure that the information schema view exposes column metadata for Parquet
   * tables.
   *
   * @param tempDir
   *          temporary directory hosting the generated Parquet file
   * @throws Exception
   *           if the table cannot be written or queried
   */
  @Test
  void shouldListColumnMetadata(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("info_cols.parquet");
    Schema schema = SchemaBuilder.record("info_cols").fields().name("id").doc("Identifier").type().intType().noDefault()
        .name("name").type().unionOf().nullType().and().stringType().endUnion().nullDefault().endRecord();
    writeTable(file, schema, List.of(record(schema, Map.of("id", 1, "name", "alpha"))), Map.of());
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    List<String> columnNames = new ArrayList<>();
    List<String> dataTypes = new ArrayList<>();
    String listColumnsQuery = """
        SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE
        FROM information_schema.columns
        WHERE TABLE_NAME = 'info_cols'
        ORDER BY ORDINAL_POSITION
        """;
    sql.query(listColumnsQuery, rs -> {
      try {
        while (rs.next()) {
          columnNames.add(rs.getString("COLUMN_NAME"));
          dataTypes.add(rs.getString("DATA_TYPE"));
          assertEquals(columnNames.size(), rs.getInt("ORDINAL_POSITION"));
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });
    assertEquals(List.of("id", "name"), columnNames);
    assertEquals(List.of("INTEGER", "VARCHAR"), dataTypes);
  }

  /**
   * Verify that REMARKS values are populated from Parquet metadata entries when
   * available.
   *
   * @param tempDir
   *          directory where the Parquet file will be written
   * @throws Exception
   *           if the table cannot be created or queried
   */
  @Test
  void shouldExposeColumnRemarksFromMetadata(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("doc_cols.parquet");
    Schema schema = SchemaBuilder.record("doc_cols").fields().name("id").type().intType().noDefault().name("name")
        .type().stringType().noDefault().endRecord();
    writeTable(file, schema, List.of(record(schema, Map.of("id", 1, "name", "bravo"))),
        Map.of("parquet.column.comment.name", "Preferred name"));
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    String remarkQuery = """
        SELECT COLUMN_NAME, REMARKS
        FROM information_schema.columns
        WHERE TABLE_NAME = 'doc_cols' AND COLUMN_NAME = 'name'
        """;
    sql.query(remarkQuery, rs -> {
      try {
        assertTrue(rs.next(), "column row must exist");
        assertEquals("name", rs.getString("COLUMN_NAME"));
        assertEquals("Preferred name", rs.getString("REMARKS"));
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });
  }

  /**
   * Confirm that Avro doc annotations are treated as column remarks when footer
   * metadata is absent.
   *
   * @param tempDir
   *          directory holding the temporary Parquet file
   * @throws Exception
   *           if the table cannot be created or queried
   */
  @Test
  void shouldFallbackToAvroDocForRemarks(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("doc_fallback.parquet");
    Schema schema = SchemaBuilder.record("doc_fallback").fields().name("id").doc("Identifier doc").type().intType()
        .noDefault().name("notes").doc("Free form notes").type().unionOf().nullType().and().stringType().endUnion()
        .nullDefault().endRecord();
    writeTable(file, schema, List.of(record(schema, Map.of("id", 10, "notes", "memo"))), Map.of());
    JParqSql sql = new JParqSql("jdbc:jparq:" + tempDir.toAbsolutePath());
    String fallbackQuery = """
        SELECT COLUMN_NAME, REMARKS
        FROM information_schema.columns
        WHERE TABLE_NAME = 'doc_fallback' AND COLUMN_NAME = 'notes'
        """;
    sql.query(fallbackQuery, rs -> {
      try {
        assertTrue(rs.next(), "column row must exist");
        assertEquals("notes", rs.getString("COLUMN_NAME"));
        assertEquals("Free form notes", rs.getString("REMARKS"));
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });
  }

  private GenericRecord record(Schema schema, Map<String, Object> values) {
    GenericRecord record = new GenericData.Record(schema);
    if (values != null) {
      for (Schema.Field field : schema.getFields()) {
        record.put(field.name(), values.get(field.name()));
      }
    }
    return record;
  }

  private void writeTable(Path file, Schema schema, List<GenericRecord> rows, Map<String, String> metadata)
      throws IOException {
    Configuration conf = new Configuration(false);
    Map<String, String> meta = metadata == null ? Map.of() : metadata;
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.toUri())).withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).withSchema(schema).withExtraMetaData(meta).build()) {
      for (GenericRecord row : rows) {
        writer.write(row);
      }
    }
  }
}
