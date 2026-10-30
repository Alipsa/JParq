package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.Time;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
