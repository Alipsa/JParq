package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies that predicate pushdown cannot bind datetime keywords to stored
 * fields.
 */
class LocalDatetimePushdownTest {

  /**
   * Reads delimited physical fields with their stored values and NULLs intact
   * under every predicate and projection pushdown configuration.
   *
   * @param directory
   *          temporary database directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @Test
  void readsQuotedPhysicalColumns(@TempDir Path directory) throws Exception {
    writeFixture(directory.resolve("t.parquet"), true);
    for (boolean predicate : List.of(true, false)) {
      for (boolean projection : List.of(true, false)) {
        String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown="
            + projection;
        try (var connection = DriverManager.getConnection(url);
            var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT \"localtime\", \"localtimestamp\" FROM t ORDER BY id")) {
          for (int id = 1; id <= 4; id++) {
            assertTrue(result.next(), url);
            assertEquals(id <= 2 ? null : "stored time", result.getString(1), url);
            assertEquals(id <= 2 ? null : "stored timestamp", result.getString(2), url);
          }
          assertFalse(result.next(), url);
        }
      }
    }
  }

  /**
   * Preserves every matching row when physical fields share datetime keyword
   * names.
   *
   * @param sql
   *          query returning the matching row count
   * @param expected
   *          expected count with pushdown enabled or disabled
   * @param directory
   *          temporary database directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SELECT COUNT(*) FROM t WHERE NOT (LOCALTIME IS NULL)|4",
      "SELECT COUNT(*) FROM t WHERE NOT (LOCALTIMESTAMP IS NULL)|4",
      "SELECT COUNT(*) FROM t WHERE NOT (LOCALTIME IS NULL) AND id > 2|2",
      "SELECT COUNT(*) FROM t WHERE NOT (CAST(LOCALTIME AS VARCHAR(64)) LIKE 'never')|4",
      "SELECT COUNT(*) FROM t JOIN u ON t.id = u.id AND LOCALTIME IS NOT NULL|4",
      "SELECT COUNT(*) FROM t JOIN u ON t.id = u.id AND LOCALTIMESTAMP IS NOT NULL|4",
      "SELECT COUNT(*) FROM t JOIN u ON t.id = u.id WHERE LOCALTIME IS NOT NULL|4",
      "SELECT COUNT(*) FROM t JOIN u ON t.id = u.id WHERE LOCALTIMESTAMP IS NOT NULL|4",
      "SELECT COUNT(*) FROM t JOIN u ON t.id = u.id WHERE NOT (LOCALTIME IS NULL)|4",
      "SELECT COUNT(*) FROM t WHERE \"localtime\" IS NOT NULL|2",
      "SELECT COUNT(*) FROM t WHERE \"localtimestamp\" IS NOT NULL|2",
      "SELECT COUNT(*) FROM t JOIN u ON t.id = u.id WHERE t.\"localtime\" IS NOT NULL|2"
  })
  void preservesRowCounts(String sql, int expected, @TempDir Path directory) throws Exception {
    writeFixture(directory.resolve("t.parquet"), true);
    writeFixture(directory.resolve("u.parquet"), false);
    for (boolean predicate : List.of(true, false)) {
      for (boolean projection : List.of(true, false)) {
        String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown="
            + projection;
        try (var connection = DriverManager.getConnection(url);
            var statement = connection.createStatement();
            var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(expected, result.getInt(1), sql + ": " + url);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Writes four records, optionally including keyword-named physical columns with
   * two NULL values each.
   *
   * @param file
   *          destination Parquet file
   * @param keywordFields
   *          whether the table includes fields named after datetime keywords
   * @throws Exception
   *           if the fixture cannot be written
   */
  private static void writeFixture(Path file, boolean keywordFields) throws Exception {
    var fields = SchemaBuilder.record("KeywordFields").fields().requiredInt("id");
    if (keywordFields) {
      fields.optionalString("localtime").optionalString("localtimestamp");
    }
    Schema schema = fields.endRecord();
    Configuration configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(file.toUri()), configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withConf(configuration).withSchema(schema)
        .build()) {
      for (int id = 1; id <= 4; id++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", id);
        if (keywordFields) {
          record.put("localtime", id <= 2 ? null : "stored time");
          record.put("localtimestamp", id <= 2 ? null : "stored timestamp");
        }
        writer.write(record);
      }
    }
  }
}
