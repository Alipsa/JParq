package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.List;
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

/** Verifies exact numeric types through derived projections and aggregation. */
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
    var schema = SchemaBuilder.record("small").fields().requiredInt("id").requiredLong("large").requiredString("unused")
        .endRecord();
    var configuration = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(directory.resolve("small.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      for (int id = 0; id < 5; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("large", 9007199254740993L + id);
        row.put("unused", "unused");
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
      for (String expression : List.of("id * 1", "large * 1", "large + 1", "large - 1", "large / 1", "large % 10")) {
        BigDecimal expected = switch (expression) {
          case "id * 1" -> new BigDecimal("10");
          case "large + 1" -> new BigDecimal("45035996273704980");
          case "large - 1" -> new BigDecimal("45035996273704970");
          case "large % 10" -> new BigDecimal("25");
          default -> new BigDecimal("45035996273704975");
        };
        String body = "SELECT " + expression + " AS x FROM small WHERE id >= 0";
        for (String sql : List.of("SELECT SUM(" + expression + ") FROM small WHERE id >= 0",
            "SELECT SUM(x) FROM (" + body + ") t", "WITH t AS (" + body + ") SELECT SUM(x) FROM t",
            "SELECT SUM(t.x) FROM (" + body + ") t JOIN small s ON s.id = 0")) {
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
}
