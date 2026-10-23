package jparq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * JDBC coverage for array columns stored with the standard 3-level Parquet LIST
 * layout, the layout written by PyArrow, Spark, and parquet-avro with
 * {@code parquet.avro.write-old-list-structure=false}. Regression tests for
 * issue #217, where projection pushdown silently returned null for these
 * columns.
 */
class ThreeLevelListLayoutTest {

  /**
   * Reads 3-level arrays with projection pushdown on and off.
   *
   * @param pushdown
   *          projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void projectsThreeLevelArrays(boolean pushdown, @TempDir Path directory) throws Exception {
    writeThreeLevel(directory);
    try (var connection = connect(directory, pushdown)) {
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT id, nums FROM p ORDER BY id")) {
        int[][] expected = new int[][]{
            {
                1, 2
            }, {
                3
            }, {}
        };
        for (int row = 0; row < expected.length; row++) {
          assertTrue(result.next());
          assertEquals(row + 1, result.getInt(1));
          assertArrayEquals(expected[row], ints(result.getObject(2)), "row " + (row + 1));
        }
        assertFalse(result.next());
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT nums FROM p WHERE id = 2")) {
        assertTrue(result.next());
        assertArrayEquals(new int[]{
            3
        }, ints(result.getObject(1)));
        assertFalse(result.next());
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT nums FROM p ORDER BY id")) {
        int[][] expected = new int[][]{
            {
                1, 2
            }, {
                3
            }, {}
        };
        for (int row = 0; row < expected.length; row++) {
          assertTrue(result.next());
          assertArrayEquals(expected[row], ints(result.getObject(1)), "row " + (row + 1));
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Evaluates quantified predicates over 3-level arrays with predicate and
   * projection pushdown combined.
   *
   * @param pushdown
   *          predicate and projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void quantifiedPredicatesOverThreeLevelArrays(boolean pushdown, @TempDir Path directory) throws Exception {
    writeThreeLevel(directory);
    try (var connection = connect(directory, pushdown)) {
      assertEquals(List.of(1), ids(connection, "SELECT id FROM p WHERE 2 = ANY(nums) ORDER BY id"));
      assertEquals(List.of(3), ids(connection, "SELECT id FROM p WHERE 2 = ALL(nums) ORDER BY id"));
      assertEquals(List.of(2), ids(connection, "SELECT id FROM p WHERE 3 = ANY(nums) ORDER BY id"));
      assertEquals(List.of(2, 3), ids(connection, "SELECT id FROM p WHERE id = 2 OR NOT (2 = ANY(nums)) ORDER BY id"));
      assertEquals(List.of(), ids(connection, "SELECT id FROM p WHERE id = 1 AND 3 = ANY(nums)"));
    }
  }

  /**
   * Reads optional 3-level arrays containing null arrays and null elements,
   * matching the PyArrow element nullability convention.
   *
   * @param pushdown
   *          projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void projectsNullableThreeLevelArrays(boolean pushdown, @TempDir Path directory) throws Exception {
    Schema nullableInt = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    Schema schema = SchemaBuilder.record("nullable_arrays").fields().requiredInt("id").requiredString("extra")
        .name("nums").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createArray(nullableInt)))
        .noDefault().endRecord();
    writeRecords(directory, "nullable.parquet", schema, List.of(//
        record(schema, 1, "a", Arrays.asList(1, null, 3)), //
        record(schema, 2, "b", null), //
        record(schema, 3, "c", List.of())));
    try (var connection = connect(directory, pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT id, nums FROM nullable ORDER BY id")) {
      assertTrue(result.next());
      assertEquals(1, result.getInt(1));
      assertArrayEquals(new Object[]{
          1, null, 3
      }, (Object[]) result.getObject(2));
      assertTrue(result.next());
      assertEquals(2, result.getInt(1));
      assertNull(result.getObject(2));
      assertTrue(result.next());
      assertEquals(3, result.getInt(1));
      assertArrayEquals(new Object[]{}, (Object[]) result.getObject(2));
      assertFalse(result.next());
    }
  }

  /**
   * Writes the issue #217 fixture: {@code {id: int, nums: array<int>}} in the
   * standard 3-level LIST layout, plus a string column no query selects so every
   * projection prunes at least one field.
   *
   * @param directory
   *          dataset directory
   * @throws IOException
   *           if writing fails
   */
  private static void writeThreeLevel(Path directory) throws IOException {
    Schema schema = SchemaBuilder.record("three_level").fields().requiredInt("id").requiredString("extra").name("nums")
        .type(Schema.createArray(Schema.create(Schema.Type.INT))).noDefault().endRecord();
    writeRecords(directory, "p.parquet", schema, List.of(//
        record(schema, 1, "a", List.of(1, 2)), //
        record(schema, 2, "b", List.of(3)), //
        record(schema, 3, "c", List.of())));
  }

  /**
   * Writes records using the standard 3-level LIST layout.
   *
   * @param directory
   *          dataset directory
   * @param fileName
   *          parquet file name
   * @param schema
   *          record schema
   * @param records
   *          rows to write
   * @throws IOException
   *           if writing fails
   */
  private static void writeRecords(Path directory, String fileName, Schema schema, List<GenericRecord> records)
      throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve(fileName).toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
  }

  /**
   * Creates one row, storing the array through its declared (possibly union)
   * field schema.
   *
   * @param schema
   *          record schema
   * @param id
   *          row id
   * @param extra
   *          unselected string column value
   * @param nums
   *          array elements, or null for a null array
   * @return the populated record
   */
  private static GenericRecord record(Schema schema, int id, String extra, List<Integer> nums) {
    var record = new GenericData.Record(schema);
    record.put("id", id);
    record.put("extra", extra);
    Schema fieldSchema = schema.getField("nums").schema();
    if (fieldSchema.getType() == Schema.Type.UNION) {
      fieldSchema = fieldSchema.getTypes().stream().filter(type -> type.getType() == Schema.Type.ARRAY).findFirst()
          .orElseThrow();
    }
    Object value = nums == null ? null : new GenericData.Array<>(fieldSchema, new ArrayList<>(nums));
    record.put("nums", value);
    return record;
  }

  /**
   * Opens a connection with both pushdown modes set to the supplied value.
   *
   * @param directory
   *          dataset directory
   * @param pushdown
   *          predicate and projection pushdown setting
   * @return open connection
   * @throws SQLException
   *           if the connection cannot be opened
   */
  private static Connection connect(Path directory, boolean pushdown) throws SQLException {
    return DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
  }

  /**
   * Converts a JDBC array value to primitive ints.
   *
   * @param value
   *          array value returned by {@code getObject}
   * @return primitive int copy of the array
   */
  private static int[] ints(Object value) {
    Object[] array = (Object[]) value;
    int[] result = new int[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = ((Number) array[i]).intValue();
    }
    return result;
  }

  /**
   * Reads IDs in query order.
   *
   * @param connection
   *          open connection
   * @param sql
   *          query to execute
   * @return returned IDs
   * @throws SQLException
   *           if execution fails
   */
  private static List<Integer> ids(Connection connection, String sql) throws SQLException {
    List<Integer> ids = new ArrayList<>();
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        ids.add(result.getInt(1));
      }
    }
    return ids;
  }
}
