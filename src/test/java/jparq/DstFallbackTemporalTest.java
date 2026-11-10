package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Picking a UTC timestamp through CASE, COALESCE, grouping, set operations, or
 * a derived table must not change its instant during the DST fall-back overlap,
 * when two instants share one wall-clock time in the JVM zone.
 */
@Isolated("Changes the JVM default time zone")
class DstFallbackTemporalTest {
  @TempDir
  Path directory;

  /**
   * Retains both UTC instants that share one wall-clock time in the fall-back
   * hour, directly and through combinations.
   *
   * @param zone
   *          host time zone
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UTC", "Europe/Stockholm", "America/New_York"
  })
  void retainsUtcInstantsThroughCombinations(String zone) throws Exception {
    TimeZone original = TimeZone.getDefault();
    String previousProperty = System.getProperty("user.timezone");
    try {
      System.setProperty("user.timezone", zone);
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      writeFixture();
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
        assertInstants(connection, "SELECT o FROM t2 ORDER BY id", Instant.parse("2024-10-27T00:30:00Z"),
            Instant.parse("2024-10-27T01:30:00Z"));
        assertInstants(connection, "SELECT COALESCE(n, o) FROM t2 ORDER BY id", Instant.parse("2024-10-27T00:30:00Z"),
            Instant.parse("2024-10-27T01:30:00Z"));
        assertInstants(connection, "SELECT CASE WHEN TRUE THEN o ELSE l END FROM t2 ORDER BY id",
            Instant.parse("2024-10-27T00:30:00Z"), Instant.parse("2024-10-27T01:30:00Z"));
        assertInstants(connection, "SELECT c FROM (SELECT COALESCE(n, o) AS c FROM t2) d ORDER BY c",
            Instant.parse("2024-10-27T00:30:00Z"), Instant.parse("2024-10-27T01:30:00Z"));
        try (var statement = connection.createStatement();
            var result = statement
                .executeQuery("SELECT COUNT(*) FROM (SELECT COALESCE(n, o) AS c FROM t2 GROUP BY COALESCE(n, o)) g")) {
          assertTrue(result.next());
          assertEquals(2, result.getInt(1), "each overlapping instant forms its own group");
          assertFalse(result.next());
        }
        try (var statement = connection.createStatement();
            var result = statement.executeQuery(
                "SELECT x, COUNT(*) FROM (SELECT o AS x FROM t2 UNION ALL SELECT l FROM t2) d GROUP BY x")) {
          List<Instant> instants = new ArrayList<>();
          while (result.next()) {
            instants.add(assertInstanceOf(Timestamp.class, result.getObject(1)).toInstant());
          }
          assertTrue(instants.contains(Instant.parse("2024-10-27T00:30:00Z")),
              "the first overlapping UTC instant stays distinct: " + instants);
          assertTrue(instants.contains(Instant.parse("2024-10-27T01:30:00Z")),
              "the second overlapping UTC instant stays distinct: " + instants);
        }
      }
    } finally {
      TimeZone.setDefault(original);
      if (previousProperty == null) {
        System.clearProperty("user.timezone");
      } else {
        System.setProperty("user.timezone", previousProperty);
      }
    }
  }

  /**
   * Compares the instants of two single-column timestamp rows.
   *
   * @param connection
   *          open test connection
   * @param sql
   *          query returning two timestamp rows
   * @param expected
   *          expected instants in row order
   * @throws Exception
   *           if the query fails
   */
  private static void assertInstants(Connection connection, String sql, Instant... expected) throws Exception {
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      for (Instant instant : expected) {
        assertTrue(result.next(), sql);
        assertEquals(instant, assertInstanceOf(Timestamp.class, result.getObject(1), sql).toInstant(), sql);
      }
      assertFalse(result.next(), sql);
    }
  }

  /**
   * Writes a UTC column with two instants from the fall-back overlap, a local
   * column holding their shared wall-clock time, and a nullable local column.
   *
   * @throws Exception
   *           if fixture writing fails
   */
  private void writeFixture() throws Exception {
    Schema utc = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema local = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema = SchemaBuilder.record("dstFallback").fields().requiredInt("id").name("o").type(utc).noDefault()
        .name("l").type(local).noDefault().name("n").type(Schema.createUnion(Schema.create(Schema.Type.NULL), local))
        .noDefault().endRecord();
    long wall = LocalDateTime.parse("2024-10-27T02:30:00").toInstant(ZoneOffset.UTC).toEpochMilli();
    var configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("t2.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 1; id <= 2; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("o", Instant.parse(id == 1 ? "2024-10-27T00:30:00Z" : "2024-10-27T01:30:00Z").toEpochMilli());
        row.put("l", wall);
        row.put("n", null);
        writer.write(row);
      }
    }
  }
}
