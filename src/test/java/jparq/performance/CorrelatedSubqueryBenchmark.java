package jparq.performance;

import java.nio.file.Files;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/** Manual warm benchmark for full iteration of the issue #192 reproducer. */
public final class CorrelatedSubqueryBenchmark {
  /** Prevents instantiation of the manual benchmark. */
  private CorrelatedSubqueryBenchmark() {
  }

  /**
   * Writes fixtures and measures each independent pushdown configuration.
   *
   * @param args
   *          unused command-line arguments
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @SuppressWarnings("PMD.SystemPrintln")
  public static void main(String[] args) throws Exception {
    var directory = Files.createTempDirectory("jparq-correlated-benchmark");
    var schema = SchemaBuilder.record("row").fields().requiredInt("id").requiredInt("g").endRecord();
    try {
      for (String name : List.of("big", "small")) {
        var configuration = new Configuration(false);
        var file = new org.apache.hadoop.fs.Path(directory.resolve(name + ".parquet").toUri());
        try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
            .withSchema(schema).withConf(configuration).build()) {
          for (int id = 0; id < ("big".equals(name) ? 500 : 5); id++) {
            var row = new GenericData.Record(schema);
            row.put("id", id);
            row.put("g", id % 5);
            writer.write(row);
          }
        }
      }
      for (boolean predicate : List.of(true, false)) {
        for (boolean projection : List.of(true, false)) {
          var samples = new ArrayList<Long>();
          try (var connection = DriverManager.getConnection(
              "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection)) {
            for (int iteration = 0; iteration < 4; iteration++) {
              long start = System.nanoTime();
              int rows = 0;
              try (
                  var statement = connection.prepareStatement(
                      "SELECT b.id FROM big b " + "WHERE EXISTS (SELECT 1 FROM small s WHERE s.g = b.g)");
                  var result = statement.executeQuery()) {
                while (result.next()) {
                  if (result.getInt(1) != rows++) {
                    throw new IllegalStateException("Unexpected identifier");
                  }
                }
              }
              if (rows != 500) {
                throw new IllegalStateException("Expected 500 rows, got " + rows);
              }
              if (iteration > 0) {
                samples.add((System.nanoTime() - start) / 1_000_000);
              }
            }
          }
          System.out.println("predicate=" + predicate + ", projection=" + projection + ", milliseconds=" + samples);
        }
      }
    } finally {
      try (var files = Files.list(directory)) {
        for (var file : files.toList()) {
          Files.deleteIfExists(file);
        }
      }
      Files.deleteIfExists(directory);
    }
  }
}
