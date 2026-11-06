package jparq.aggregate;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
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
import org.junit.jupiter.params.provider.ValueSource;

/** Regression tests for REAL SUM value types and JDBC metadata. */
class RealSumTest {
  @TempDir
  Path directory;

  /**
   * Writes two identical REAL values and an all-NULL REAL column.
   *
   * @throws Exception
   *           if writing the Parquet fixture fails
   */
  @BeforeEach
  void writeFixture() throws Exception {
    var schema = SchemaBuilder.record("p").fields().requiredFloat("f").optionalFloat("n").endRecord();
    var file = new org.apache.hadoop.fs.Path(directory.resolve("p.parquet").toUri());
    var configuration = new Configuration(false);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      for (int i = 0; i < 2; i++) {
        var record = new GenericData.Record(schema);
        record.put("f", 0.1f);
        writer.write(record);
      }
    }
  }

  /**
   * Verifies direct, grouped, and materialized sums return the advertised Double.
   *
   * @param sql
   *          the aggregate query
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT SUM(f) AS x FROM p", "SELECT SUM(f) AS x FROM p GROUP BY f",
      "SELECT x FROM (SELECT SUM(f) AS x FROM p) q", "WITH q AS (SELECT SUM(f) AS x FROM p) SELECT x FROM q"
  })
  void returnsDoubleWithMatchingMetadata(String sql) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1));
      assertEquals(Double.class.getName(), result.getMetaData().getColumnClassName(1));
      assertTrue(result.next());
      assertEquals(2 * (double) 0.1f, assertInstanceOf(Double.class, result.getObject(1)));
      assertFalse(result.next());
    }
  }

  /**
   * Verifies NULL sums retain DOUBLE metadata for all-NULL and empty inputs.
   *
   * @param sql
   *          the query producing a NULL sum
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT SUM(n) AS x FROM p", "SELECT SUM(f) AS x FROM p WHERE f > 1"
  })
  void nullSumsRetainDoubleMetadata(String sql) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1));
      assertEquals(Double.class.getName(), result.getMetaData().getColumnClassName(1));
      assertTrue(result.next());
      assertNull(result.getObject(1));
      assertTrue(result.wasNull());
      assertFalse(result.next());
    }
  }

  /**
   * Verifies set operations compare SUM and REAL values after common conversion.
   *
   * @param operation
   *          the SQL set operator
   * @param expectedRows
   *          the expected result count
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @CsvSource({
      "UNION, 1", "UNION ALL, 3", "INTERSECT, 1", "EXCEPT, 0"
  })
  void setOperationsUseConsistentDoubleValues(String operation, int expectedRows) throws Exception {
    String aggregate = "SELECT SUM(f) AS x FROM (SELECT f FROM p LIMIT 1) q";
    String scalar = "SELECT f FROM p";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : new String[]{
          aggregate + " " + operation + " " + scalar, scalar + " " + operation + " " + aggregate
      }) {
        try (var result = statement.executeQuery(sql)) {
          assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1), sql);
          assertEquals(Double.class.getName(), result.getMetaData().getColumnClassName(1), sql);
          int rows = 0;
          while (result.next()) {
            assertEquals((double) 0.1f, assertInstanceOf(Double.class, result.getObject(1)), sql);
            rows++;
          }
          assertEquals(expectedRows, rows, sql);
        }
      }
    }
  }
}
