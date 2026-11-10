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
import java.util.LinkedHashMap;
import java.util.Map;
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
import org.junit.jupiter.params.provider.CsvSource;

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
   * hour, directly and through combinations. Each fixture's instants fall in the
   * named zone's own fall-back overlap; only there do the local wall-clock rows
   * merge with the second instant during wall-clock grouping.
   *
   * @param zone
   *          host time zone
   * @param first
   *          first overlapping UTC instant
   * @param second
   *          second overlapping UTC instant
   * @param wall
   *          shared local wall-clock time stored in the local column
   * @param fallBackZone
   *          zone whose fall-back overlap the fixture instants fall into
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "UTC, 2024-10-27T00:30:00Z, 2024-10-27T01:30:00Z, 2024-10-27T02:30:00, Europe/Stockholm",
      "Europe/Stockholm, 2024-10-27T00:30:00Z, 2024-10-27T01:30:00Z, 2024-10-27T02:30:00, Europe/Stockholm",
      "America/New_York, 2024-11-03T05:30:00Z, 2024-11-03T06:30:00Z, 2024-11-03T01:30:00, America/New_York"
  })
  void retainsUtcInstantsThroughCombinations(String zone, String first, String second, String wall, String fallBackZone)
      throws Exception {
    Instant firstInstant = Instant.parse(first);
    Instant secondInstant = Instant.parse(second);
    boolean exercisesOverlap = zone.equals(fallBackZone);
    TimeZone original = TimeZone.getDefault();
    String previousProperty = System.getProperty("user.timezone");
    try {
      System.setProperty("user.timezone", zone);
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      writeFixture(firstInstant, secondInstant, wall);
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
        assertInstants(connection, "SELECT o FROM t2 ORDER BY id", firstInstant, secondInstant);
        assertInstants(connection, "SELECT COALESCE(n, o) FROM t2 ORDER BY id", firstInstant, secondInstant);
        assertInstants(connection, "SELECT CASE WHEN TRUE THEN o ELSE l END FROM t2 ORDER BY id", firstInstant,
            secondInstant);
        assertInstants(connection, "SELECT c FROM (SELECT COALESCE(n, o) AS c FROM t2) d ORDER BY c", firstInstant,
            secondInstant);
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
          Map<Instant, Integer> groups = new LinkedHashMap<>();
          while (result.next()) {
            Instant instant = assertInstanceOf(Timestamp.class, result.getObject(1)).toInstant();
            groups.put(instant, result.getInt(2));
          }
          assertTrue(groups.containsKey(firstInstant), "the first overlapping UTC instant stays distinct: " + groups);
          assertTrue(groups.containsKey(secondInstant), "the second overlapping UTC instant stays distinct: " + groups);
          assertEquals(1, groups.get(firstInstant), "the first instant forms its own group: " + groups);
          assertEquals(exercisesOverlap ? 3 : 1, groups.get(secondInstant),
              "the shared wall-clock merges only during the fall-back overlap: " + groups);
          assertEquals(exercisesOverlap ? 2 : 3, groups.size(), "unexpected grouping in zone " + zone + ": " + groups);
          assertEquals(4, groups.values().stream().mapToInt(Integer::intValue).sum(),
              "every UNION ALL row lands in exactly one group: " + groups);
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
   * @param first
   *          first overlapping UTC instant
   * @param second
   *          second overlapping UTC instant
   * @param wall
   *          shared local wall-clock time stored in the local column
   * @throws Exception
   *           if fixture writing fails
   */
  private void writeFixture(Instant first, Instant second, String wall) throws Exception {
    Schema utc = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema local = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema = SchemaBuilder.record("dstFallback").fields().requiredInt("id").name("o").type(utc).noDefault()
        .name("l").type(local).noDefault().name("n").type(Schema.createUnion(Schema.create(Schema.Type.NULL), local))
        .noDefault().endRecord();
    long wallMillis = LocalDateTime.parse(wall).toInstant(ZoneOffset.UTC).toEpochMilli();
    var configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("t2.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 1; id <= 2; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("o", (id == 1 ? first : second).toEpochMilli());
        row.put("l", wallMillis);
        row.put("n", null);
        writer.write(row);
      }
    }
  }
}
