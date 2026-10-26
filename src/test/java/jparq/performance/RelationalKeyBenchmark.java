package jparq.performance;

import java.nio.file.Files;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/** Manual warm benchmark for INT-column DISTINCT, UNION, and GROUP BY keys. */
public final class RelationalKeyBenchmark {
  /** Prevents utility class instantiation. */
  private RelationalKeyBenchmark() {
  }

  /**
   * Writes one million INT rows with 200,000 distinct values and measures three
   * warm samples after three unmeasured warmups per query.
   *
   * @param args
   *          unused command-line arguments
   * @throws Exception
   *           if fixture creation, JDBC execution, or cleanup fails
   */
  @SuppressWarnings("PMD.SystemPrintln")
  public static void main(String[] args) throws Exception {
    var directory = Files.createTempDirectory("jparq-key-benchmark");
    var schema = SchemaBuilder.record("row").fields().requiredInt("id").endRecord();
    try {
      var configuration = new Configuration(false);
      var file = new org.apache.hadoop.fs.Path(directory.resolve("big.parquet").toUri());
      try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
          .withSchema(schema).withConf(configuration).build()) {
        for (int id = 0; id < 1_000_000; id++) {
          var row = new GenericData.Record(schema);
          row.put("id", id % 200_000);
          writer.write(row);
        }
      }
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
        for (String sql : List.of("SELECT DISTINCT id FROM big", "SELECT id FROM big UNION SELECT id FROM big",
            "SELECT id, COUNT(*) FROM big GROUP BY id")) {
          List<Long> samples = new ArrayList<>();
          for (int iteration = 0; iteration < 6; iteration++) {
            long started = System.nanoTime();
            int rows = 0;
            long sum = 0;
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              while (result.next()) {
                sum += result.getInt(1);
                rows++;
              }
            }
            if (rows != 200_000 || sum != 19_999_900_000L) {
              throw new IllegalStateException("Unexpected query result: rows=" + rows + ", sum=" + sum);
            }
            if (iteration >= 3) {
              samples.add((System.nanoTime() - started) / 1_000_000);
            }
          }
          System.out.println(
              sql + ": samples(ms)=" + samples + ", best=" + samples.stream().min(Long::compare).orElseThrow());
        }
      }
    } finally {
      try (var paths = Files.walk(directory)) {
        for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(path);
        }
      }
    }
  }
}
