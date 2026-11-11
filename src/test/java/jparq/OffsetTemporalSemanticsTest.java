package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.TimeZone;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.SqlComparisons;
import se.alipsa.jparq.helper.TemporalInterval;
import se.alipsa.jparq.helper.TemporalValueType;

/** Verifies offset datetime semantics independently of written clock fields. */
@Isolated("Changes the JVM default time zone")
class OffsetTemporalSemanticsTest {
  @TempDir
  Path directory;

  /**
   * Checks local Java and JDBC representations compare by the same clock fields.
   */
  @Test
  void comparesLocalRepresentations() {
    var local = java.time.LocalDateTime.parse("2024-07-01T10:00:00.123456789");
    assertEquals(0, SqlComparisons.typedCompare(local, Timestamp.valueOf(local)));
    assertEquals(0, SqlComparisons.typedCompare(Timestamp.valueOf(local), local));
    var time = java.sql.Time.valueOf("10:00:00");
    time.setTime(time.getTime() + 500);
    assertEquals(0, SqlComparisons.typedCompare(java.time.LocalTime.parse("10:00:00.5"), time));
    assertEquals(0, SqlComparisons.typedCompare(time, java.time.LocalTime.parse("10:00:00.5")));
  }

  /**
   * Preserves instants in predicates, keys, arithmetic, casts, and JDBC getters.
   *
   * @param zone
   *          JVM time zone
   * @throws Exception
   *           if fixture creation or querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UTC", "Europe/Stockholm", "America/New_York"
  })
  void usesUtcSemantics(String zone) throws Exception {
    TimeZone original = TimeZone.getDefault();
    String property = System.getProperty("user.timezone");
    try {
      System.setProperty("user.timezone", zone);
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      writeFixture();
      for (boolean pushdown : List.of(true, false)) {
        try (var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown)) {
          assertPredicates(connection);
          assertKeysAndOrder(connection);
          assertCastsAndGetters(connection);
        }
      }
    } finally {
      TimeZone.setDefault(original);
      if (property == null) {
        System.clearProperty("user.timezone");
      } else {
        System.setProperty("user.timezone", property);
      }
    }
  }

  /**
   * Checks comparison operators and subtraction use UTC values.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if a query fails
   */
  private static void assertPredicates(Connection connection) throws Exception {
    for (String predicate : List.of("CAST(o AS TIMESTAMP WITH TIME ZONE) = o",
        "o = CAST(o AS TIMESTAMP WITH TIME ZONE)", "CAST(o AS TIMESTAMP WITH TIME ZONE) IN (o)",
        "o IN (CAST(o AS TIMESTAMP WITH TIME ZONE))", "a = CAST(o AS TIMESTAMP WITH TIME ZONE)",
        "a IS NOT DISTINCT FROM CAST(o AS TIMESTAMP WITH TIME ZONE)",
        "CAST(o AS TIMESTAMP WITH TIME ZONE) IN (SELECT o FROM t)", "o IN (SELECT a FROM t)",
        "CASE o WHEN a THEN TRUE ELSE FALSE END", "NULLIF(o,a) IS NULL",
        "CAST(o AS TIMESTAMP WITH TIME ZONE) = ANY(SELECT o FROM t)", "o = ANY(SELECT a FROM t)",
        "o = ALL(ARRAY[a])")) {
      assertCount(connection, "SELECT COUNT(*) FROM t WHERE " + predicate, 2);
    }
    for (String predicate : List.of("a = b", "a IN (b)", "ta = tb", "ta IN (tb)", "a <= b")) {
      assertCount(connection, "SELECT COUNT(*) FROM t WHERE " + predicate, 1);
    }
    assertCount(connection, "SELECT COUNT(*) FROM t WHERE a < b", 0);
    try (var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT CAST(o AS TIMESTAMP WITH TIME ZONE) - o,a - b,ta - tb FROM t WHERE id = 1")) {
      assertTrue(result.next());
      for (int column = 1; column <= 3; column++) {
        assertEquals(java.time.Duration.ZERO, ((TemporalInterval) result.getObject(column)).duration());
      }
    }
    assertEquals(0, SqlComparisons.typedCompare(OffsetTime.parse("00:30:00+01:00"), OffsetTime.parse("23:30:00Z")));
  }

  /**
   * Checks relational hashing and sorting agree with comparison equality.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if a query fails
   */
  private static void assertKeysAndOrder(Connection connection) throws Exception {
    for (String[] pair : List.of(new String[]{
        "a", "b"
    }, new String[]{
        "ta", "tb"
    }, new String[]{
        "ARRAY[a]", "ARRAY[b]"
    }, new String[]{
        "ARRAY[ta]", "ARRAY[tb]"
    })) {
      String first = "SELECT " + pair[0] + " AS x FROM t WHERE id = 1";
      String second = "SELECT " + pair[1] + " AS x FROM t WHERE id = 1";
      assertCount(connection, "SELECT COUNT(*) FROM (" + first + " UNION " + second + ") q", 1);
      assertCount(connection,
          "SELECT COUNT(*) FROM (SELECT DISTINCT x FROM (" + first + " UNION ALL " + second + ") q) d", 1);
      try (var statement = connection.createStatement();
          var result = statement
              .executeQuery("SELECT COUNT(*) FROM (" + first + " UNION ALL " + second + ") q GROUP BY x")) {
        assertTrue(result.next());
        assertEquals(2, result.getInt(1));
        assertFalse(result.next());
      }
    }
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT x FROM (SELECT a AS x FROM t WHERE id = 1 "
            + "UNION ALL SELECT b AS x FROM t WHERE id = 2) q ORDER BY x")) {
      assertTrue(result.next());
      assertEquals(Instant.parse("2024-07-01T09:30:00Z"), result.getObject(1, OffsetDateTime.class).toInstant());
      assertTrue(result.next());
      assertEquals(Instant.parse("2024-07-01T10:00:00Z"), result.getObject(1, OffsetDateTime.class).toInstant());
      assertFalse(result.next());
    }
  }

  /**
   * Checks timestamp-to-time casts keep offsets and JDBC getters convert
   * instants.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if a query fails
   */
  private static void assertCastsAndGetters(Connection connection) throws Exception {
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CAST(o AS TIME WITH TIME ZONE),"
            + "CAST(CAST(o AS TIMESTAMP WITH TIME ZONE) AS TIME WITH TIME ZONE),"
            + "CAST(COALESCE(o,CURRENT_TIMESTAMP) AS TIME WITH TIME ZONE),"
            + "CAST(o AS TIMESTAMP WITH TIME ZONE),ta,edge FROM t WHERE id = 1")) {
      assertTrue(result.next());
      for (int column = 1; column <= 3; column++) {
        assertEquals(OffsetTime.parse("10:00:00Z"), result.getObject(column, OffsetTime.class));
      }
      Timestamp timestamp = Timestamp.from(Instant.parse("2024-07-01T10:00:00Z"));
      assertEquals(timestamp, result.getTimestamp(4));
      assertEquals(new java.sql.Date(timestamp.getTime()), result.getDate(4));
      assertEquals(se.alipsa.jparq.engine.function.DateTimeFunctions.toTime(timestamp), result.getTime(4));
      Timestamp epochTime = Timestamp.from(OffsetTime.parse("10:00:00Z").atDate(LocalDate.ofEpochDay(0)).toInstant());
      assertEquals(se.alipsa.jparq.engine.function.DateTimeFunctions.toTime(epochTime), result.getTime(5));
      Timestamp edge = Timestamp.from(result.getObject(6, OffsetDateTime.class).toInstant());
      assertEquals(new java.sql.Date(edge.getTime()), result.getDate(6));
      assertEquals(se.alipsa.jparq.engine.function.DateTimeFunctions.toTime(edge), result.getTime(6));
    }
  }

  /**
   * Checks a scalar count query.
   *
   * @param connection
   *          open connection
   * @param sql
   *          scalar count query
   * @param expected
   *          expected count
   * @throws Exception
   *           if querying fails
   */
  private static void assertCount(Connection connection, String sql, int expected) throws Exception {
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      assertTrue(result.next(), sql);
      assertEquals(expected, result.getInt(1), sql);
    }
  }

  /**
   * Writes UTC-adjusted storage and native offset columns with equal instants.
   *
   * @throws Exception
   *           if fixture creation fails
   */
  private void writeFixture() throws Exception {
    Schema schema = SchemaBuilder.record("offsets").fields().requiredInt("id").name("o")
        .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault().name("a")
        .type(TemporalValueType.OFFSET_TIMESTAMP.schema()).noDefault().name("b")
        .type(TemporalValueType.OFFSET_TIMESTAMP.schema()).noDefault().name("ta")
        .type(TemporalValueType.OFFSET_TIME.schema()).noDefault().name("tb")
        .type(TemporalValueType.OFFSET_TIME.schema()).noDefault().name("edge")
        .type(TemporalValueType.OFFSET_TIMESTAMP.schema()).noDefault().endRecord();
    Configuration configuration = new Configuration();
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("t.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericData.Record>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id : List.of(1, 2)) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("o", Instant.parse("2024-07-01T10:00:00Z").getEpochSecond() * 1_000_000L);
        row.put("a", "2024-07-01T10:00:00Z");
        row.put("b", id == 1 ? "2024-07-01T11:00:00+01:00" : "2024-07-01T10:30:00+01:00");
        row.put("edge", "2024-07-01T00:30:00+14:00");
        row.put("ta", "10:00:00Z");
        row.put("tb", id == 1 ? "11:00:00+01:00" : "10:30:00+01:00");
        writer.write(row);
      }
    }
  }
}
