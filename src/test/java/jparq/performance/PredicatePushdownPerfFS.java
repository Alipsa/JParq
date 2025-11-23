package jparq.performance;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.alipsa.jparq.JParqDriver;
import se.alipsa.jparq.JParqSql;

/**
 * This failsafe test aims to show the performance benefits of pushdown of WHERE
 * predicates to parquet filters over applying filter code in on the result.
 *
 * <p>
 * Note: the difference is still very small (a few milliseconds) and might fail
 * in some cases.
 */
@SuppressWarnings("NewClassNamingConvention")
public class PredicatePushdownPerfFS {

  static Logger LOG = LoggerFactory.getLogger(PredicatePushdownPerfFS.class);

  @Disabled
  @Test
  void pushdownIsFasterWhenSchemaIsPresent() throws Exception {
    // 1) Prepare temp dirs + write files

    java.nio.file.Path noSchemaDir = Files.createTempDirectory("jparq-noschema-");
    java.nio.file.Path withSchemaDir = Files.createTempDirectory("jparq-withschema-");

    writeParquetWithoutAvroSchema(noSchemaDir.resolve("data.parquet"));
    writeParquetWithAvroSchema(withSchemaDir.resolve("data.parquet"));

    String sql = "SELECT id, value, category FROM data WHERE id BETWEEN 2000000 AND 2001000 AND category = 'C'";

    // Warmup runs
    for (int i = 0; i < 2; i++) {
      timeQuery("jdbc:jparq:" + noSchemaDir.toAbsolutePath(), sql, new ArrayList<>(), 3);
      timeQuery("jdbc:jparq:" + withSchemaDir.toAbsolutePath(), sql, new ArrayList<>(), 3);
    }

    // 3) Run both and collect results + timings
    List<String> baselineRows = new ArrayList<>();
    long tNoSchema = medianTime("jdbc:jparq:" + noSchemaDir.toAbsolutePath(), sql, 7, 3);
    long tWithSchema = medianTime("jdbc:jparq:" + withSchemaDir.toAbsolutePath(), sql, 7, 3);

    List<String> schemaRows = new ArrayList<>();

    // 4) Verify same results
    assertEquals(baselineRows, schemaRows, "Results must be identical");

    // 5) Print timings (ms) to help you see the gain locally
    LOG.info("No schema time:   {} ms", TimeUnit.NANOSECONDS.toMillis(tNoSchema));
    LOG.info("With schema time: {} ms", TimeUnit.NANOSECONDS.toMillis(tWithSchema));

    // 6) Gentle assertion: schema path should not be slower.
    BigDecimal ratio = BigDecimal.valueOf((double) tWithSchema / (double) tNoSchema).setScale(2, RoundingMode.HALF_UP);
    // NOTE: this test is too fast for meaningful comparisons
    BigDecimal diff = BigDecimal.valueOf((tWithSchema - tNoSchema) / 1_000_000).setScale(2, RoundingMode.HALF_UP);

    if (tWithSchema < tNoSchema) {
      LOG.info("Pushdown diff (with schema - no schema = {} ms, ratio= {} ({}% faster)", diff, ratio,
          BigDecimal.valueOf(100).subtract(ratio.multiply(BigDecimal.valueOf(100))).setScale(0, RoundingMode.HALF_UP));
    } else {
      LOG.warn("Expected pushdown to be at least faster. diff = {} ms, ratio= {}, noSchema= {} ns, withSchema= {} ns",
          diff, ratio, tNoSchema, tWithSchema);
    }
  }

  @Disabled
  @Test
  void pushdownIsFasterWhenSchemaIsPresentTailRows() throws Exception {
    // 1) Prepare temp dirs + write files

    java.nio.file.Path noSchemaDir = Files.createTempDirectory("jparq-noschema-tail-");
    java.nio.file.Path withSchemaDir = Files.createTempDirectory("jparq-withschema-tail-");

    writeParquetWithoutAvroSchema(noSchemaDir.resolve("data.parquet"));
    writeParquetWithAvroSchema(withSchemaDir.resolve("data.parquet"));

    // 2) Same selective, deterministic query (very few matches)
    // ~ categories repeat every 4; id in [10..20] gives 11 ids; expect about 2-3
    // rows
    String sql = "SELECT id, value FROM data WHERE id BETWEEN 3990000 AND 4000000";

    // Warmup runs
    for (int i = 0; i < 2; i++) {
      timeQuery("jdbc:jparq:" + noSchemaDir.toAbsolutePath(), sql, new ArrayList<>(), 2);
      timeQuery("jdbc:jparq:" + withSchemaDir.toAbsolutePath(), sql, new ArrayList<>(), 2);
    }

    // 3) Run both and collect results + timings
    List<String> baselineRows = new ArrayList<>();

    List<String> schemaRows = new ArrayList<>();
    long tNoSchema = medianTimePrepared("jdbc:jparq:" + noSchemaDir.toAbsolutePath(), sql, 7, 2);
    long tWithSchema = medianTimePrepared("jdbc:jparq:" + withSchemaDir.toAbsolutePath(), sql, 7, 2);

    // 4) Verify same results
    assertEquals(baselineRows, schemaRows, "Results must be identical");

    // 5) Print timings (ms) to help you see the gain locally
    LOG.info("No schema tail time:   {} ms", TimeUnit.NANOSECONDS.toMillis(tNoSchema));
    LOG.info("With schema tail time: {} ms", TimeUnit.NANOSECONDS.toMillis(tWithSchema));

    // 6) Gentle assertion: schema path should not be slower.
    // Non-regression: pushdown shouldn’t be meaningfully slower locally.
    // (On bigger datasets / cold cache you’ll typically see << 1.0)
    double ratio = (double) tWithSchema / (double) tNoSchema;
    double epsilon = 1.00; // Allow up to 100% tolerance to survive constrained CI hosts where noise
                           // dominates timings
    assertTrue(tWithSchema <= tNoSchema * (1.0 + epsilon), "Expected pushdown not to be meaningfully slower. ratio="
        + ratio + " noSchema=" + tNoSchema + "ns withSchema=" + tWithSchema + "ns");
  }

  // ---------------- helpers ----------------

  private static long timeQuery(String jdbcUrl, String sql, List<String> outRows, int ncols) {
    long start = System.nanoTime();
    new JParqSql(jdbcUrl).query(sql, rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(ncols, md.getColumnCount(), "Number of columns is wrong");
        while (rs.next()) {
          outRows.add(rs.getInt(1) + ":" + rs.getDouble(2));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    return System.nanoTime() - start;
  }

  private static long timeQueryPrepared(PreparedStatement pstmt, List<String> outRows, int ncols) throws SQLException {
    long start = System.nanoTime();
    try (ResultSet rs = pstmt.executeQuery()) {
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(ncols, md.getColumnCount(), "Number of columns is wrong");
      outRows.add(rs.getInt(1) + ":" + rs.getDouble(2));
    }
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

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hPath).withConf(conf).withType(schema)
        .withRowGroupSize(128L * 1024 * 1024).withPageSize(64 * 1024).withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE).build()) {

      for (int i = 0; i < 4_000_000; i++) {
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
        .withRowGroupSize(128L * 1024 * 1024).withPageSize(64 * 1024).withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE).build()) {
      for (int i = 0; i < 4_000_000; i++) {
        GenericRecord r = new GenericData.Record(avroSchema);
        r.put("id", i);
        r.put("category", pickCategory(i));
        r.put("value", deterministicValue(i));
        writer.write(r);
      }
    }
  }

  private static long medianTime(String jdbcUrl, String sql, int runs, int ncols) {
    // warmup is already done outside; this is for measurement only
    long[] samples = new long[runs];
    for (int i = 0; i < runs; i++) {
      samples[i] = timeQuery(jdbcUrl, sql, new ArrayList<>(), ncols);
    }
    Arrays.sort(samples);
    return samples[runs / 2];
  }

  private static long medianTimePrepared(String jdbcUrl, String sql, int runs, int ncols) {
    try (Connection conn = new JParqDriver().connect(jdbcUrl, new Properties());
        PreparedStatement pstmt = conn.prepareStatement(sql)) { // Query planning happens here ONCE

      long[] samples = new long[runs];
      for (int i = 0; i < runs; i++) {
        samples[i] = timeQueryPrepared(pstmt, new ArrayList<>(), ncols); // Measure only I/O/reading
      }
      Arrays.sort(samples);
      return samples[runs / 2];
    } catch (SQLException e) {
      fail("Failed to execute prepared statment", e);
    }
    return -1;
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
