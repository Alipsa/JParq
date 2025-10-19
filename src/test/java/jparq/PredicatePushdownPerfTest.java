package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.*;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.*;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class PredicatePushdownPerfTest {

  @Test
  void pushdownIsFasterWhenSchemaIsPresent() throws Exception {
    // 1) Prepare temp dirs + write files

    java.nio.file.Path noSchemaDir = Files.createTempDirectory("jparq-noschema-");
    java.nio.file.Path withSchemaDir = Files.createTempDirectory("jparq-withschema-");

    writeParquetWithoutAvroSchema(noSchemaDir.resolve("data.parquet"));
    writeParquetWithAvroSchema(withSchemaDir.resolve("data.parquet"));

    // 2) Same selective, deterministic query (very few matches)
    // ~ categories repeat every 4; id in [10..20] gives 11 ids; expect about 2-3
    // rows
    String sql = "SELECT id, value FROM data WHERE id BETWEEN 10 AND 20 AND category = 'C'";

    // 3) Run both and collect results + timings
    List<String> baselineRows = new ArrayList<>();
    long tNoSchema = timeQuery("jdbc:jparq:" + noSchemaDir.toAbsolutePath(), sql, baselineRows);

    List<String> schemaRows = new ArrayList<>();
    long tWithSchema = timeQuery("jdbc:jparq:" + withSchemaDir.toAbsolutePath(), sql, schemaRows);

    // 4) Verify same results
    assertEquals(baselineRows, schemaRows, "Results must be identical");

    // 5) Print timings (ms) to help you see the gain locally
    System.out.println("No schema time:   " + TimeUnit.NANOSECONDS.toMillis(tNoSchema) + " ms");
    System.out.println("With schema time: " + TimeUnit.NANOSECONDS.toMillis(tWithSchema) + " ms");

    // 6) Gentle assertion: schema path should not be slower.
    // If you consistently see a good gain, tighten to (tWithSchema * 1.1 <
    // tNoSchema), etc.
    assertTrue(tWithSchema <= tNoSchema, "Expected schema-backed read (with pushdown) to be faster or equal. "
        + "noSchema=" + tNoSchema + " ns, withSchema=" + tWithSchema + " ns");
  }

  // ---------------- helpers ----------------

  private static long timeQuery(String jdbcUrl, String sql, List<String> outRows) {
    long start = System.nanoTime();
    new JParqSql(jdbcUrl).query(sql, rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expect 2 columns: id, value");
        while (rs.next()) {
          outRows.add(rs.getInt(1) + ":" + rs.getDouble(2));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    return System.nanoTime() - start;
  }

  static MessageType getMessageType() {
    return Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()) // ✅ new API
        .named("category").required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("value").named("row");
  }

  /** Write data using Parquet Example writer (NO Avro schema in footer). */
  private static void writeParquetWithoutAvroSchema(java.nio.file.Path file) throws IOException {
    Configuration conf = new Configuration(false);

    MessageType schema = getMessageType();

    // Ensure GroupWriteSupport can find the schema during writer init
    GroupWriteSupport.setSchema(schema, conf);

    org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(file.toUri());
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);

    try (ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(hPath)
        .withConf(conf).withType(schema) // explicit, in addition to the conf setting
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {

      for (int i = 0; i < 1000; i++) {
        String category = pickCategory(i);
        double value = deterministicValue(i);
        Group g = factory.newGroup().append("id", i).append("category", category).append("value", value);
        writer.write(g);
      }
    }
  }

  /**
   * Write data using AvroParquetWriter (Avro schema in footer => pushdown
   * enabled).
   */
  private static void writeParquetWithAvroSchema(java.nio.file.Path file) throws IOException {
    String avroJson = """
        {
          "type": "record",
          "name": "Row",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "category", "type": "string"},
            {"name": "value", "type": "double"}
          ]
        }
        """;

    Schema avroSchema = new Schema.Parser().parse(avroJson);
    Configuration conf = new Configuration(false);

    org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(file.toUri());
    var out = HadoopOutputFile.fromPath(hPath, conf); // ✅ use OutputFile, not Path

    try (ParquetWriter<GenericRecord> writer = org.apache.parquet.avro.AvroParquetWriter.<GenericRecord>builder(out)
        .withConf(conf).withSchema(avroSchema).withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE).build()) {
      for (int i = 0; i < 1000; i++) {
        GenericRecord r = new GenericData.Record(avroSchema);
        r.put("id", i);
        r.put("category", pickCategory(i));
        r.put("value", deterministicValue(i));
        writer.write(r);
      }
    }
  }

  // Deterministic helpers so we can filter predictably
  private static String pickCategory(int i) {
    return switch (i & 3) { // 0..3
      case 0 -> "A";
      case 1 -> "B";
      case 2 -> "C";
      default -> "D";
    };
  }

  private static double deterministicValue(int i) {
    // stable, non-random function of id
    return ((i % 100) * 1.5) + 0.25;
  }
}
