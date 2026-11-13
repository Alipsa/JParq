package jparq.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.alipsa.jparq.JParqSql;

/**
 * Guards against declaration lookups re-preparing set operation branches per
 * column, which made derived tables and CTEs over set operations several times
 * slower. Compares a UNION ALL of derived tables against a single derived table
 * so the assertion self-normalizes to the machine's speed.
 */
@SuppressWarnings("NewClassNamingConvention")
public class SetOperationDerivedTablePerfFS {

  static final Logger LOG = LoggerFactory.getLogger(SetOperationDerivedTablePerfFS.class);

  private static final int ROWS = 200_000;
  private static final int COLUMNS = 12;

  @Test
  void setOperationDerivedTableDeclarationLookupStaysCheap() throws Exception {
    java.nio.file.Path directory = Files.createTempDirectory("jparq-setop-derived-");
    writeFixture(directory.resolve("wide.parquet"));

    String columns = columnList();
    String setOperationSql = "SELECT " + columns + " FROM (SELECT * FROM wide) d1 UNION ALL SELECT " + columns
        + " FROM (SELECT * FROM wide) d2";
    String plainSql = "SELECT " + columns + " FROM (SELECT * FROM wide) d";

    for (int i = 0; i < 2; i++) {
      timeQuery("jdbc:jparq:" + directory.toAbsolutePath(), setOperationSql);
      timeQuery("jdbc:jparq:" + directory.toAbsolutePath(), plainSql);
    }

    long tSetOperation = medianTime("jdbc:jparq:" + directory.toAbsolutePath(), setOperationSql, 5);
    long tPlain = medianTime("jdbc:jparq:" + directory.toAbsolutePath(), plainSql, 5);

    LOG.info("Set operation derived table time: {} ms", TimeUnit.NANOSECONDS.toMillis(tSetOperation));
    LOG.info("Plain derived table time:         {} ms", TimeUnit.NANOSECONDS.toMillis(tPlain));

    double ratio = BigDecimal.valueOf((double) tSetOperation / (double) tPlain).setScale(2, RoundingMode.HALF_UP)
        .doubleValue();
    // The set operation reads twice the rows, so a ratio near 2 is expected.
    // Per-column branch preparation pushed it to 5+; keep generous headroom.
    assertTrue(ratio < 4.0, "Set operation derived table regressed: ratio=" + ratio + " setOperation=" + tSetOperation
        + "ns plain=" + tPlain + "ns");
  }

  private static long timeQuery(String jdbcUrl, String sql) {
    long start = System.nanoTime();
    new JParqSql(jdbcUrl).query(sql, (ResultSet rs) -> {
      try {
        while (rs.next()) {
          rs.getInt(1);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    return System.nanoTime() - start;
  }

  private static long medianTime(String jdbcUrl, String sql, int runs) {
    long[] samples = new long[runs];
    for (int i = 0; i < runs; i++) {
      samples[i] = timeQuery(jdbcUrl, sql);
    }
    Arrays.sort(samples);
    return samples[runs / 2];
  }

  private static String columnList() {
    List<String> columns = new ArrayList<>(COLUMNS);
    for (int i = 1; i <= COLUMNS; i++) {
      columns.add("c" + i);
    }
    return String.join(", ", columns);
  }

  private static void writeFixture(java.nio.file.Path file) throws IOException {
    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("wide").fields();
    for (int i = 1; i <= COLUMNS; i++) {
      fields = fields.name("c" + i).type().intType().noDefault();
    }
    Schema schema = fields.endRecord();
    Configuration configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(file.toUri()), configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int row = 0; row < ROWS; row++) {
        var record = new GenericData.Record(schema);
        for (int i = 1; i <= COLUMNS; i++) {
          record.put("c" + i, row * i);
        }
        writer.write(record);
      }
    }
  }
}
