package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Verifies value-based types through derived projections and aggregation. */
class DerivedNumericTypeTest {

  @TempDir
  Path directory;

  /**
   * Writes integers and longs beyond the exact integer range of DOUBLE.
   *
   * @throws Exception
   *           if the fixture cannot be written
   */
  @BeforeEach
  void createFixture() throws Exception {
    var schema = SchemaBuilder.record("small").fields().requiredInt("id").requiredLong("large").requiredString("s")
        .endRecord();
    var configuration = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(directory.resolve("small.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      for (int id = 0; id < 5; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("large", 9007199254740993L + id);
        row.put("s", "2024-01-02");
        writer.write(row);
      }
    }
  }

  /**
   * Checks metadata, object types, and exact values with independently configured
   * predicate and projection pushdown.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void derivedArithmeticRetainsExactAggregateType(boolean predicate, boolean projection) throws Exception {
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      try (var statement = connection.createStatement();
          var result = statement.executeQuery(
              "SELECT x FROM (SELECT id, large * 1 AS x FROM small WHERE id >= 1) t WHERE id < 4 ORDER BY id")) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertEquals(BigDecimal.class.getName(), result.getMetaData().getColumnClassName(1));
        for (long expected = 9007199254740994L; expected <= 9007199254740996L; expected++) {
          assertTrue(result.next());
          assertEquals(BigDecimal.valueOf(expected), result.getObject(1));
        }
        assertFalse(result.next());
      }
      var sums = Map.of("id * 1", "10", "large * 1", "45035996273704975", "large + 1", "45035996273704980", "large - 1",
          "45035996273704970", "large / 1", "45035996273704975", "large % 10", "25");
      for (var entry : sums.entrySet()) {
        String expression = entry.getKey();
        BigDecimal expected = new BigDecimal(entry.getValue());
        String body = "SELECT " + expression + " AS x FROM small WHERE id >= 0";
        for (String sql : List.of("SELECT SUM(" + expression + ") FROM small WHERE id >= 0",
            "SELECT SUM(x) FROM (" + body + ") t", "WITH t AS (" + body + ") SELECT SUM(x) FROM t",
            "SELECT SUM(t.x) FROM (" + body + ") t JOIN small s ON s.id = 0",
            "SELECT SUM(t.x) FROM small s JOIN (" + body + ") t ON s.id = 0")) {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            var metadata = result.getMetaData();
            assertEquals(Types.DECIMAL, metadata.getColumnType(1), sql);
            assertEquals("DECIMAL", metadata.getColumnTypeName(1), sql);
            assertEquals(BigDecimal.class.getName(), metadata.getColumnClassName(1), sql);
            assertTrue(result.next(), sql);
            var value = assertInstanceOf(BigDecimal.class, result.getObject(1), sql);
            assertEquals(0, expected.compareTo(value), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Supplies computed dates, window values, and mixed numeric values.
   *
   * @return expressions paired with JDBC types and expected objects
   */
  static Stream<Arguments> computedValues() {
    return Stream.of(Arguments.of("CAST(s AS DATE)", Types.DATE, Collections.nCopies(5, Date.valueOf("2024-01-02"))),
        Arguments.of("SUM(id) OVER (ORDER BY id)", Types.BIGINT, List.of(0L, 1L, 3L, 6L, 10L)),
        Arguments.of("LAG(id * 2) OVER (ORDER BY id)", Types.DECIMAL,
            Arrays.asList(null, new BigDecimal("0"), new BigDecimal("2"), new BigDecimal("4"), new BigDecimal("6"))),
        Arguments.of("CASE WHEN id = 1 THEN 1 ELSE 2.5 END", Types.DECIMAL,
            List.of(new BigDecimal("2.5"), new BigDecimal("1.0"), new BigDecimal("2.5"), new BigDecimal("2.5"),
                new BigDecimal("2.5"))),
        Arguments.of("CASE WHEN id = 1 THEN 1 ELSE 'a' END", Types.VARCHAR, List.of("a", "1", "a", "a", "a")));
  }

  /**
   * Checks inferred metadata and objects across materialization boundaries.
   *
   * @param expression
   *          the computed projection
   * @param jdbcType
   *          the expected JDBC type
   * @param expected
   *          the expected values in id order
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @MethodSource("computedValues")
  void computedValuesRetainInferredTypes(String expression, int jdbcType, List<Object> expected) throws Exception {
    String body = "SELECT id, " + expression + " AS x FROM small";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String source : List.of("(" + body + ") t", "(" + body + ") t JOIN small s ON s.id = 0",
          "small s JOIN (" + body + ") t ON s.id = 0")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT t.x FROM " + source + " ORDER BY t.id")) {
          assertEquals(jdbcType, result.getMetaData().getColumnType(1), expression);
          Object nonNull = expected.stream().filter(Objects::nonNull).findFirst().orElseThrow();
          assertEquals(nonNull.getClass().getName(), result.getMetaData().getColumnClassName(1), expression);
          for (Object value : expected) {
            assertTrue(result.next(), expression);
            assertEquals(value, result.getObject(1), expression);
          }
          assertFalse(result.next(), expression);
        }
      }
    }
  }

  /**
   * Pins the VARCHAR fallback when unknown expressions have no non-null values.
   *
   * @param body
   *          the empty or all-null projection
   * @param expectedRows
   *          the expected row count
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SELECT id * 1 AS x FROM small WHERE id > 100 | 0",
      "SELECT CASE WHEN id > 100 THEN id * 1 END AS x FROM small | 5"
  })
  void unknownTypesWithoutValuesFallBackToVarchar(String body, int expectedRows) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String sql : List.of("SELECT t.x FROM (" + body + ") t",
          "SELECT t.x FROM (" + body + ") t JOIN small s ON s.id = 0",
          "SELECT t.x FROM small s JOIN (" + body + ") t ON s.id = 0", "WITH t AS (" + body + ") SELECT x FROM t")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(Types.VARCHAR, result.getMetaData().getColumnType(1), sql);
          assertEquals(String.class.getName(), result.getMetaData().getColumnClassName(1), sql);
          int count = 0;
          while (result.next()) {
            assertNull(result.getObject(1), sql);
            count++;
          }
          assertEquals(expectedRows, count, sql);
        }
      }
    }
  }
}
