package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.helper.TemporalInterval;

/** Checks stored TIME precision, relational operations, and data errors. */
class TimePrecisionTest {
  @TempDir
  Path directory;

  /**
   * Writes both TIME logical types with deliberately unordered fractions.
   *
   * @throws Exception
   *           if a fixture cannot be written
   */
  @BeforeEach
  void createFixture() throws Exception {
    write("clocks",
        SchemaBuilder.record("clocks").fields().requiredInt("id").name("t")
            .type(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))).noDefault().name("u")
            .type(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault().endRecord(),
        List.of(List.of(1, 36_000_000, 36_000_000_999L), List.of(2, 36_000_500, 36_000_500_999L),
            List.of(3, 36_000_250, 36_000_250_999L)));
    Schema timestamps = SchemaBuilder.record("dated").fields().requiredInt("id").requiredString("s").name("ts")
        .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault().endRecord();
    List<List<?>> rows = new ArrayList<>();
    int id = 1;
    for (String date : List.of("2024-01-01", "2024-06-01", "2025-01-01")) {
      Timestamp timestamp = Timestamp.valueOf(date + " 10:00:00.500999");
      String text = id == 1 ? "2024-01-02 00:00:00.123456" : "2024-01-02 00:00:00.123456789";
      rows.add(List.of(id++, text, timestamp.toInstant().getEpochSecond() * 1_000_000 + timestamp.getNanos() / 1000));
    }
    write("dated", timestamps, rows);
  }

  /**
   * Applies timestamp precision consistently across supported target spellings.
   *
   * @param target
   *          timestamp cast target
   * @param precision
   *          expected fractional-second precision
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "TIMESTAMP,6", "TIMESTAMP WITHOUT TIME ZONE,6", "TIMESTAMP WITH TIME ZONE,6", "DATETIME,6",
      "TIMESTAMP(0) WITHOUT TIME ZONE,0", "TIMESTAMP(3) WITHOUT TIME ZONE,3", "TIMESTAMP(9) WITHOUT TIME ZONE,9"
  })
  void timestampTargetSpellingsKeepFractionalPrecision(String target, int precision) throws Exception {
    try (var connection = connection(true, true); var statement = connection.createStatement()) {
      for (int id : List.of(1, 2)) {
        int sourceNanos = id == 1 ? 123456000 : 123456789;
        int unit = (int) Math.pow(10, 9 - precision);
        int expectedNanos = sourceNanos / unit * unit;
        String query = "SELECT CAST(s AS " + target + ") AS x FROM dated WHERE id = " + id;
        try (var result = statement.executeQuery(query)) {
          assertTrue(result.next(), query);
          assertEquals(expectedNanos, result.getTimestamp(1).getNanos(), query);
          assertEquals(expectedNanos, assertInstanceOf(Timestamp.class, result.getObject(1)).getNanos(), query);
          assertFalse(result.next(), query);
        }
        String difference = "SELECT CAST(s AS " + target
            + ") - TIMESTAMP '2024-01-01 00:00:00' AS x FROM dated WHERE id = " + id;
        for (String sql : List.of(difference, "SELECT x FROM (" + difference + ") q")) {
          try (var result = statement.executeQuery(sql)) {
            assertEquals("INTERVAL", result.getMetaData().getColumnTypeName(1), difference);
            assertTrue(result.next(), difference);
            TemporalInterval interval = assertInstanceOf(TemporalInterval.class, result.getObject(1));
            assertEquals(expectedNanos, interval.duration().getNano(), difference);
            assertEquals(precision, interval.fractionalPrecision(), difference);
            assertEquals(interval.toString(), result.getString(1), difference);
          }
        }
      }
      try (var result = statement.executeQuery(
          "SELECT CAST(s AS " + target + ") - TIMESTAMP '2024-01-01 00:00:00' AS x FROM dated WHERE id > 5")) {
        assertEquals("INTERVAL", result.getMetaData().getColumnTypeName(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Normalizes timestamp casts before comparison, ordering, equality, and
   * hashing.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void timestampCastsUseOnlyTheClock(boolean predicate, boolean projection) throws Exception {
    try (var connection = connection(predicate, projection)) {
      for (String target : List.of("TIME", "TIME(3)")) {
        String cast = "CAST(ts AS " + target + ")";
        int millis = "TIME".equals(target) ? 0 : 500;
        String literal = millis == 0 ? "TIME '10:00:00'" : "TIME '10:00:00.5'";
        assertEquals(List.of(1, 2, 3),
            ids(connection, "SELECT id FROM dated WHERE " + cast + " = " + literal + " ORDER BY id"));
        try (var statement = connection.createStatement();
            var result = statement.executeQuery(
                "SELECT ts, " + cast + ", DENSE_RANK() OVER (ORDER BY " + cast + ") AS r FROM dated ORDER BY id")) {
          for (int id = 1; id <= 3; id++) {
            assertTrue(result.next());
            assertEquals(time(500), result.getTime(1));
            assertEquals(time(millis), assertInstanceOf(Time.class, result.getObject(2)));
            assertEquals(1, result.getInt(3));
          }
          assertFalse(result.next());
        }
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT DISTINCT " + cast + " FROM dated")) {
          assertTrue(result.next());
          assertEquals(time(millis), result.getObject(1));
          assertFalse(result.next());
        }
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT " + cast + ", COUNT(*) FROM dated GROUP BY " + cast)) {
          assertTrue(result.next());
          assertEquals(time(millis), result.getObject(1));
          assertEquals(3, result.getInt(2));
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Retains milliseconds through interval arithmetic and midnight wraparound.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void intervalsRetainMilliseconds(boolean predicate, boolean projection) throws Exception {
    try (var connection = connection(predicate, projection)) {
      for (String column : List.of("t", "u")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT " + column + " + INTERVAL '1' HOUR FROM clocks ORDER BY id")) {
          for (int fraction : List.of(0, 500, 250)) {
            assertTrue(result.next());
            Time expected = Time.valueOf("11:00:00");
            expected.setTime(expected.getTime() + fraction);
            assertEquals(expected, assertInstanceOf(Time.class, result.getObject(1)));
          }
          assertFalse(result.next());
        }
        assertEquals(List.of(2),
            ids(connection, "SELECT id FROM clocks WHERE " + column + " + INTERVAL '1' HOUR = TIME '11:00:00.5'"));
        assertEquals(List.of(2),
            ids(connection, "SELECT id FROM clocks WHERE " + column + " - INTERVAL '1' HOUR = TIME '09:00:00.5'"));
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT TIME '23:59:59.75' + INTERVAL '0.5' SECOND")) {
        assertTrue(result.next());
        Time expected = Time.valueOf("00:00:00");
        expected.setTime(expected.getTime() + 250);
        assertEquals(expected, assertInstanceOf(Time.class, result.getObject(1)));
      }
    }
  }

  /**
   * Exercises getter conversions directly, including LocalTime values currently
   * difficult to produce through SQL because of issue 274.
   *
   * @throws Exception
   *           if reading the result fails
   */
  @Test
  void gettersNormalizeAllSupportedTimeInputs() throws Exception {
    Timestamp timestamp = Timestamp.valueOf("2024-06-01 10:00:00.500999");
    Time dated = new Time(timestamp.getTime());
    List<List<Object>> rows = new ArrayList<>();
    for (Object value : List.of(timestamp, dated, LocalTime.of(10, 0, 0, 500_999_999), "10:00:00.500",
        timestamp.getTime(), (double) timestamp.getTime())) {
      rows.add(List.of(value));
    }
    rows.add(java.util.Collections.singletonList(null));
    try (var result = JParqResultSet.materializedResult("clock", List.of("t"), List.of(Types.TIME), rows)) {
      for (int row = 0; row < rows.size() - 1; row++) {
        assertTrue(result.next());
        assertEquals(time(500), result.getTime(1));
        assertEquals(time(500), result.getTime("t"));
      }
      assertTrue(result.next());
      assertNull(result.getTime(1));
      assertTrue(result.wasNull());
      assertFalse(result.next());
    }
    assertEquals(timestamp.getTime(), dated.getTime(), "Normalization must not mutate an input Time");
  }

  /**
   * Preserves fractions for getters, comparisons, grouping, sets, aggregates,
   * materialized inputs, and window ordering.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void preservesMilliseconds(boolean predicate, boolean projection) throws Exception {
    try (var connection = connection(predicate, projection)) {
      for (String column : List.of("t", "u")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT " + column + " FROM clocks ORDER BY id")) {
          for (int fraction : List.of(0, 500, 250)) {
            assertTrue(result.next());
            assertEquals(time(fraction), assertInstanceOf(Time.class, result.getObject(1)));
            assertEquals(time(fraction), result.getTime(1));
          }
          assertFalse(result.next());
        }
        assertEquals(List.of(2, 3, 1), ids(connection, "SELECT id FROM clocks ORDER BY " + column + " DESC"));
        for (String condition : List.of(" = TIME '10:00:00'", " = {t '10:00:00'}", " < TIME '10:00:00.25'")) {
          assertEquals(List.of(1), ids(connection, "SELECT id FROM clocks WHERE " + column + condition));
        }
        assertEquals(List.of(), ids(connection, "SELECT id FROM clocks WHERE " + column + " > TIME '12:00:00'"));
        assertEquals(List.of(2), ids(connection, "SELECT id FROM clocks WHERE " + column + " = TIME '10:00:00.5'"));
        try (var statement = connection.prepareStatement("SELECT id FROM clocks WHERE " + column + " = ?")) {
          for (Object parameter : List.of(time(500), java.time.LocalTime.of(10, 0, 0, 500_999_999))) {
            if (parameter instanceof Time time) {
              statement.setTime(1, time);
            } else {
              statement.setObject(1, parameter);
            }
            try (var result = statement.executeQuery()) {
              assertTrue(result.next());
              assertEquals(2, result.getInt(1));
              assertFalse(result.next());
            }
          }
        }
        for (String sql : List.of("SELECT DISTINCT " + column + " FROM clocks",
            "SELECT " + column + " FROM clocks GROUP BY " + column,
            "SELECT " + column + " FROM clocks UNION SELECT " + column + " FROM clocks",
            "SELECT DISTINCT x FROM (SELECT " + column + " AS x FROM clocks) d",
            "WITH c AS (SELECT " + column + " AS x FROM clocks) SELECT DISTINCT x FROM c")) {
          List<Time> values = new ArrayList<>();
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            while (result.next()) {
              values.add(assertInstanceOf(Time.class, result.getObject(1)));
            }
          }
          assertEquals(3, values.size(), sql);
          assertTrue(values.containsAll(List.of(time(0), time(250), time(500))), sql);
        }
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT MIN(" + column + "), MAX(" + column + ") FROM clocks")) {
          assertTrue(result.next());
          assertEquals(time(0), result.getTime(1));
          assertEquals(time(500), result.getTime(2));
        }
        assertEquals(List.of(2, 3, 1), ids(connection,
            "SELECT id, ROW_NUMBER() OVER (ORDER BY " + column + " DESC) AS rn FROM clocks ORDER BY rn"));
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT CAST(t AS VARCHAR) FROM clocks WHERE id = 1")) {
        assertTrue(result.next());
        assertEquals("10:00:00", result.getString(1));
      }
    }
  }

  /**
   * Rejects invalid stored clock fields through direct and residual projections.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if writing a fixture or opening a connection fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void classifiesInvalidStoredTimes(boolean predicate, boolean projection) throws Exception {
    int index = 0;
    for (boolean micros : List.of(false, true)) {
      for (long invalid : micros ? List.of(-1L, 86_400_000_000L, Long.MAX_VALUE) : List.of(-1L, 86_400_000L)) {
        String table = "bad" + index++;
        Schema type = micros
            ? LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))
            : LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        Object stored = micros ? Long.valueOf(invalid) : (Object) Integer.valueOf((int) invalid);
        write(table, SchemaBuilder.record(table).fields().name("t").type(type).noDefault().endRecord(),
            List.of(List.of(stored)));
        try (var connection = connection(predicate, projection)) {
          for (String suffix : List.of("", " WHERE t = TIME '10:00:00'")) {
            for (String getter : List.of("object", "time", "string")) {
              var failure = assertThrows(SQLDataException.class, () -> {
                try (var statement = connection.createStatement();
                    var result = statement.executeQuery("SELECT t FROM " + table + suffix)) {
                  assertTrue(result.next());
                  switch (getter) {
                    case "object" -> result.getObject(1);
                    case "time" -> result.getTime(1);
                    case "string" -> result.getString(1);
                    default -> fail("Unknown getter");
                  }
                }
              });
              assertEquals("22008", failure.getSQLState());
              assertTrue(failure.getMessage().contains("TIME"), failure::getMessage);
            }
          }
        }
      }
    }
  }

  /**
   * Creates an expected JDBC time with a fraction independent of the converter.
   *
   * @param millis
   *          milliseconds after 10:00:00
   * @return the expected time
   */
  private static Time time(int millis) {
    Time result = Time.valueOf("10:00:00");
    result.setTime(result.getTime() + millis);
    return result;
  }

  /**
   * Collects query IDs in their returned order.
   *
   * @param connection
   *          test connection
   * @param sql
   *          query to execute
   * @return returned IDs
   * @throws Exception
   *           if the query fails
   */
  private List<Integer> ids(Connection connection, String sql) throws Exception {
    List<Integer> ids = new ArrayList<>();
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        ids.add(result.getInt(1));
      }
    }
    return ids;
  }

  /**
   * Writes fixture values without logical conversions.
   *
   * @param table
   *          fixture table
   * @param schema
   *          declared Avro schema
   * @param rows
   *          stored field values
   * @throws Exception
   *           if writing fails
   */
  private void write(String table, Schema schema, List<? extends List<?>> rows) throws Exception {
    var path = new org.apache.hadoop.fs.Path(directory.resolve(table + ".parquet").toUri());
    try (
        var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(path, new Configuration(false)))
            .withSchema(schema).withConf(new Configuration(false)).build()) {
      for (List<?> values : rows) {
        var row = new GenericData.Record(schema);
        for (int i = 0; i < values.size(); i++) {
          row.put(i, values.get(i));
        }
        writer.write(row);
      }
    }
  }

  /**
   * Opens a test connection with explicit pushdown settings.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @return the test connection
   * @throws Exception
   *           if opening fails
   */
  private Connection connection(boolean predicate, boolean projection) throws Exception {
    return DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
  }
}
