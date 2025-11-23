package jparq;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

/** Validation tests covering missing schema columns. */
class SchemaValidationTest {

  @Test
  void missingColumnSelectShouldError(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("broken.parquet");
    Schema schema = SchemaBuilder.record("broken").fields().name("id").type().intType().noDefault().endRecord();
    writeTable(file, schema, List.of(record(schema, Map.of("id", 1))));
    Class.forName("se.alipsa.jparq.JParqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jparq:" + tempDir)) {
      SQLException ex = assertThrows(SQLException.class, () -> conn.prepareStatement("SELECT department FROM broken"));
      Throwable cause = ex.getCause() == null ? ex : ex.getCause();
      String message = cause.getMessage().toLowerCase(Locale.ROOT);
      assertTrue(message.contains("missing columns"));
      assertTrue(message.contains("department"));
    }
  }

  @Test
  void missingJoinColumnShouldError(@TempDir Path tempDir) throws Exception {
    Path deptFile = tempDir.resolve("dept_missing.parquet");
    Schema deptSchema = SchemaBuilder.record("dept_missing").fields().name("id").type().intType().noDefault()
        .endRecord();
    writeTable(deptFile, deptSchema, List.of(record(deptSchema, Map.of("id", 1))));

    Path employeeFile = tempDir.resolve("employees.parquet");
    Schema employeeSchema = SchemaBuilder.record("employees").fields().name("id").type().intType().noDefault()
        .name("department_id").type().intType().noDefault().endRecord();
    writeTable(employeeFile, employeeSchema, List.of(record(employeeSchema, Map.of("id", 1, "department_id", 1))));

    Class.forName("se.alipsa.jparq.JParqDriver");
    String sql = """
        SELECT e.id, d.department
        FROM employees e
        JOIN dept_missing d ON e.department_id = d.id
        """;
    try (Connection conn = DriverManager.getConnection("jdbc:jparq:" + tempDir);
        var stmt = conn.prepareStatement(sql)) {
      SQLException ex = assertThrows(SQLException.class, stmt::executeQuery);
      Throwable cause = ex.getCause() == null ? ex : ex.getCause();
      String message = cause.getMessage().toLowerCase(Locale.ROOT);
      assertTrue(message.contains("missing columns"));
      assertTrue(message.contains("department"));
    }
  }

  private GenericRecord record(Schema schema, Map<String, Object> values) {
    GenericRecord rec = new GenericData.Record(schema);
    if (values != null) {
      for (Schema.Field field : schema.getFields()) {
        rec.put(field.name(), values.get(field.name()));
      }
    }
    return rec;
  }

  private void writeTable(Path file, Schema schema, List<GenericRecord> rows) throws Exception {
    Configuration conf = new Configuration(false);
    var outputPath = new org.apache.hadoop.fs.Path(file.toUri());
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(HadoopOutputFile.fromPath(outputPath, conf)).withConf(conf).withSchema(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      for (GenericRecord row : rows) {
        writer.write(row);
      }
    }
  }
}
