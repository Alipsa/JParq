package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
import se.alipsa.jparq.engine.function.StatementTime;

/**
 * Verifies that timestamp promotion agrees with JDBC independently of the
 * execution date.
 */
@Isolated("Changes JVM time zone and mocks the execution clock")
class TimestampDateOffsetTest {
  @TempDir
  Path directory;

  /**
   * Uses runtime JVM zone changes even when user.timezone still names another
   * zone.
   *
   * @throws Exception
   *           if fixture creation or querying fails
   */
  @Test
  void runtimeDefaultOverridesStaleTimezoneProperty() throws Exception {
    TimeZone previous = TimeZone.getDefault();
    String property = System.getProperty("user.timezone");
    try {
      System.setProperty("user.timezone", "Europe/Stockholm");
      TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
      List<Instant> expected = writeFixture();
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
        assertComparisons(connection, expected);
        String literal = "TIMESTAMP '2024-07-01 06:00:00'";
        String query = "SELECT o = " + literal + ",CAST(o AS TIMESTAMP WITH TIME ZONE) = CAST(" + literal
            + " AS TIMESTAMP WITH TIME ZONE),CAST(" + literal + " AS TIMESTAMP WITH TIME ZONE) " + "FROM t WHERE lit = "
            + literal;
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
          int rows = 0;
          while (result.next()) {
            assertEquals(Boolean.TRUE, result.getObject(1));
            assertEquals(Boolean.TRUE, result.getObject(2));
            assertEquals(OffsetDateTime.parse("2024-07-01T06:00:00-04:00"), result.getObject(3));
            rows++;
          }
          assertEquals(2, rows);
        }
        try (var statement = connection.createStatement();
            var result = statement
                .executeQuery("SELECT CURRENT_TIMESTAMP,CURRENT_TIME,CAST(TIME '06:00:00' AS TIME WITH TIME ZONE)")) {
          assertTrue(result.next());
          OffsetDateTime timestamp = result.getObject(1, OffsetDateTime.class);
          ZoneOffset offset = ZoneId.of("America/New_York").getRules().getOffset(timestamp.toInstant());
          assertEquals(offset, timestamp.getOffset());
          assertEquals(offset, result.getObject(2, OffsetTime.class).getOffset());
          assertEquals(offset, result.getObject(3, OffsetTime.class).getOffset());
        }
      }
      assertEquals("Europe/Stockholm", System.getProperty("user.timezone"));
    } finally {
      TimeZone.setDefault(previous);
      if (property == null) {
        System.clearProperty("user.timezone");
      } else {
        System.setProperty("user.timezone", property);
      }
    }
  }

  /**
   * Compares direct, implicit, and materialized conversions under winter and
   * summer clocks.
   *
   * @param zoneName
   *          JVM zone, including northern and southern DST rules
   * @throws Exception
   *           if fixture creation or querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UTC", "Europe/Stockholm", "America/New_York", "Australia/Sydney"
  })
  void usesValueDateUnderOppositeSeasonClocks(String zoneName) throws Exception {
    TimeZone previous = TimeZone.getDefault();
    String property = System.getProperty("user.timezone");
    try {
      System.setProperty("user.timezone", zoneName);
      TimeZone.setDefault(TimeZone.getTimeZone(zoneName));
      List<Instant> expected = writeFixture();
      for (String clockText : List.of("2026-01-15T12:00:00", "2026-07-15T12:00:00")) {
        OffsetDateTime clock = LocalDateTime.parse(clockText).atZone(ZoneId.of(zoneName)).toOffsetDateTime();
        try (var time = mockStatic(StatementTime.class, CALLS_REAL_METHODS);
            var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
          time.when(StatementTime::current).thenReturn(clock);
          assertComparisons(connection, expected);
          String navigation = "LAG(o,1,lit) OVER (PARTITION BY grp ORDER BY id)";
          assertInstants(connection,
              "SELECT id,CAST(" + navigation + " AS TIMESTAMP WITH TIME ZONE) FROM t ORDER BY id", expected);
          assertInstants(connection, "SELECT id,CAST(x AS TIMESTAMP WITH TIME ZONE) FROM " + "(SELECT id," + navigation
              + " AS x FROM t) q ORDER BY id", expected);
        }
      }
    } finally {
      TimeZone.setDefault(previous);
      if (property == null) {
        System.clearProperty("user.timezone");
      } else {
        System.setProperty("user.timezone", property);
      }
    }
  }

  /**
   * Verifies the reported equality forms, including gap and overlap values.
   *
   * @param connection
   *          fixture connection
   * @param expected
   *          instants independently decoded by JDBC Timestamp
   * @throws Exception
   *           if querying fails
   */
  private static void assertComparisons(Connection connection, List<Instant> expected) throws Exception {
    String query = "SELECT id,o = lit,CAST(o AS TIMESTAMP WITH TIME ZONE) = CAST(lit AS TIMESTAMP WITH TIME ZONE),"
        + "CAST(o AS TIMESTAMP WITH TIME ZONE) = lit,CAST(lit AS TIMESTAMP WITH TIME ZONE) FROM t ORDER BY id";
    try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
      for (Instant instant : expected) {
        assertTrue(result.next());
        for (int column : List.of(2, 3, 4)) {
          assertEquals(Boolean.TRUE, result.getObject(column), query + " row " + result.getInt(1));
        }
        OffsetDateTime converted = result.getObject(5, OffsetDateTime.class);
        assertEquals(instant, converted.toInstant());
        assertEquals(Timestamp.from(instant).toLocalDateTime(), converted.toLocalDateTime());
      }
      assertFalse(result.next());
    }
  }

  /**
   * Checks navigation instants directly and after materialization.
   *
   * @param connection
   *          fixture connection
   * @param query
   *          SQL to evaluate
   * @param expected
   *          independently decoded instants
   * @throws Exception
   *           if querying fails
   */
  private static void assertInstants(Connection connection, String query, List<Instant> expected) throws Exception {
    try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
      for (Instant instant : expected) {
        assertTrue(result.next(), query);
        assertEquals(instant, result.getObject(2, OffsetDateTime.class).toInstant(), query);
      }
      assertFalse(result.next(), query);
    }
  }

  /**
   * Writes matching UTC and local columns around ordinary dates and regional
   * transitions.
   *
   * @return expected instants from the existing JDBC decoder
   * @throws Exception
   *           if writing fails
   */
  private List<Instant> writeFixture() throws Exception {
    Schema schema = SchemaBuilder.record("DatedTimestamps").fields().requiredInt("id").requiredInt("grp").name("o")
        .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault().name("lit")
        .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault().endRecord();
    var configuration = new Configuration();
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("t.parquet").toUri()),
        configuration);
    List<Instant> instants = new ArrayList<>();
    try (var writer = AvroParquetWriter.<GenericData.Record>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      int group = 0;
      for (String text : List.of("2024-01-15T12:00:00", "2024-07-01T12:00:00", "2024-03-31T02:30:00",
          "2024-10-27T02:30:00", "2024-03-10T02:30:00", "2024-11-03T01:30:00", "2024-10-06T02:30:00",
          "2024-04-07T02:30:00", "2024-07-01T06:00:00")) {
        LocalDateTime local = LocalDateTime.parse(text);
        Instant instant = Timestamp.valueOf(local).toInstant();
        for (int copy = 0; copy < 2; copy++) {
          var row = new GenericData.Record(schema);
          row.put("id", instants.size() + 1);
          row.put("grp", group);
          row.put("o", instant.getEpochSecond() * 1_000_000L);
          row.put("lit", local.toEpochSecond(ZoneOffset.UTC) * 1_000_000L);
          writer.write(row);
          instants.add(instant);
        }
        group++;
      }
    }
    return instants;
  }
}
