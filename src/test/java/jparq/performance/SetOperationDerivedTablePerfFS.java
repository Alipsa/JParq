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
 * slower. The regression only shows when a derived table materializes a set
 * operation whose branches are expensive to prepare, so the guarded query wraps
 * a UNION ALL of derived tables in a derived table. It is compared with the
 * same query projecting a single column: both read and materialize the same
 * inputs, so the ratio self-normalizes to the machine's speed and only grows
 * with the per-column preparation cost the guard targets.
 */
@SuppressWarnings("NewClassNamingConvention")
public class SetOperationDerivedTablePerfFS {

  static final Logger LOG = LoggerFactory.getLogger(SetOperationDerivedTablePerfFS.class);

  private static final int ROWS = 200_000;
  private static final int COLUMNS = 12;
  private static final int RUNS = 5;

  /**
   * Compares the guarded query with all columns against its single-column form.
   *
   * @throws Exception
   *           if the fixture cannot be written or a query fails
   */
  @Test
  void setOperationDerivedTableDeclarationLookupStaysCheap() throws Exception {
    java.nio.file.Path directory = Files.createTempDirectory("jparq-setop-derived-");
    writeFixture(directory.resolve("wide.parquet"));
    String jdbcUrl = "jdbc:jparq:" + directory.toAbsolutePath();

    String allColumnsSql = nestedSetOperation(columnList());
    String singleColumnSql = nestedSetOperation("c1");

    for (int i = 0; i < 2; i++) {
      timeQuery(jdbcUrl, allColumnsSql);
      timeQuery(jdbcUrl, singleColumnSql);
    }

    // Interleave the samples so machine-wide slowdowns affect both queries, and
    // keep the fastest run of each to discard scheduling and GC outliers.
    long tAllColumns = Long.MAX_VALUE;
    long tSingleColumn = Long.MAX_VALUE;
    for (int i = 0; i < RUNS; i++) {
      tAllColumns = Math.min(tAllColumns, timeQuery(jdbcUrl, allColumnsSql));
      tSingleColumn = Math.min(tSingleColumn, timeQuery(jdbcUrl, singleColumnSql));
    }

    LOG.info("Set operation derived table, {} columns: {} ms", COLUMNS, TimeUnit.NANOSECONDS.toMillis(tAllColumns));
    LOG.info("Set operation derived table, 1 column:   {} ms", TimeUnit.NANOSECONDS.toMillis(tSingleColumn));

    double ratio = BigDecimal.valueOf((double) tAllColumns / (double) tSingleColumn).setScale(2, RoundingMode.HALF_UP)
        .doubleValue();
    // Both queries materialize the same branches, so a ratio near 1.4 is
    // expected. Preparing every branch once per column pushed it above 5.
    assertTrue(ratio < 2.5, "Set operation derived table regressed: ratio=" + ratio + " allColumns=" + tAllColumns
        + "ns singleColumn=" + tSingleColumn + "ns");
  }

  /**
   * Builds a derived table over a UNION ALL whose branches are derived tables, so
   * materializing the outer derived table must declare every projected column of
   * an expensive set operation.
   *
   * @param columns
   *          comma-separated projection
   * @return the query text
   */
  private static String nestedSetOperation(String columns) {
    return "SELECT " + columns + " FROM (SELECT " + columns + " FROM (SELECT * FROM wide) d1 UNION ALL SELECT "
        + columns + " FROM (SELECT * FROM wide) d2) u";
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
