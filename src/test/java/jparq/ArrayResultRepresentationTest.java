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
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that every ARRAY value exposed through {@code getObject()} uses the
 * uniform {@code Object[]} representation, regardless of whether the array
 * comes from a physical column, a computed constructor, or a mixture of both in
 * one result. Regression coverage for issue 234.
 */
class ArrayResultRepresentationTest {

  @Test
  void computedArrayReturnsObjectArray(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[4,6]")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          4, 6
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void physicalArrayColumnReturnsObjectArray(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ints FROM s WHERE id = 1")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          1, 2
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void unionAllMixingPhysicalAndComputedArraysReturnsOneRepresentation(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ints FROM s WHERE id = 1 UNION ALL SELECT ARRAY['x']")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          1, 2
      }, (Object[]) result.getObject(1));
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          "x"
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void nestedComputedArraysUseObjectArrays(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[ARRAY[1,2],ARRAY[3]]")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          new Object[]{
              1, 2
          }, new Object[]{
              3
          }
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void derivedTableAndCteArraysUseObjectArrays(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT a FROM (SELECT ARRAY[4,6] AS a) t")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          4, 6
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("WITH t AS (SELECT ARRAY[4,6] AS a) SELECT a FROM t")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          4, 6
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void columnClassNameReportsObjectArrayForEveryArrayColumn(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[4,6], ints FROM s WHERE id = 1")) {
      ResultSetMetaData metaData = result.getMetaData();
      assertEquals(Types.ARRAY, metaData.getColumnType(1));
      assertEquals(Object[].class.getName(), metaData.getColumnClassName(1));
      assertEquals(Types.ARRAY, metaData.getColumnType(2));
      assertEquals(Object[].class.getName(), metaData.getColumnClassName(2));
    }
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ints FROM s WHERE id = 1 UNION ALL SELECT ARRAY['x']")) {
      ResultSetMetaData metaData = result.getMetaData();
      assertEquals(Types.ARRAY, metaData.getColumnType(1));
      assertEquals(Object[].class.getName(), metaData.getColumnClassName(1));
    }
  }

  @Test
  void getStringRendersArraysReadably(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[4,6], ARRAY[[1,2],[3]]")) {
      assertTrue(result.next());
      assertEquals("[4, 6]", result.getString(1));
      assertEquals("[[1, 2], [3]]", result.getString(2));
      assertFalse(result.next());
    }
  }

  @Test
  void nullArrayValuesRemainNull(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT NULL, CAST(NULL AS ARRAY<INT>)")) {
      assertTrue(result.next());
      assertNull(result.getObject(1));
      Object value = result.getObject(2);
      assertTrue(value == null || value instanceof Object[], "expected null or Object[] but got " + value);
      assertFalse(result.next());
    }
  }

  @Test
  void arraysCompareByContentWithScalarSubqueryArrays(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] = (SELECT ARRAY[1,2]) ORDER BY id", 1, 2);
      assertIds(statement, "SELECT id FROM s WHERE (SELECT ARRAY[1,2]) = ARRAY[1,2] ORDER BY id", 1, 2);
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] IN (SELECT ARRAY[1,2]) ORDER BY id", 1, 2);
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[s.id] IN (SELECT ARRAY[i.id] FROM s i) ORDER BY id", 1, 2);
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] <> (SELECT ARRAY[1,2]) ORDER BY id");
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] NOT IN (SELECT ARRAY[1,2]) ORDER BY id");
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] = (SELECT ARRAY[1,3]) ORDER BY id");
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] < (SELECT ARRAY[1,3]) ORDER BY id", 1, 2);
    }
  }

  @Test
  void storedArraysCompareByContentWithComputedArrays(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      assertIds(statement, "SELECT id FROM s WHERE ints = ARRAY[1,2] ORDER BY id", 1);
      assertIds(statement, "SELECT id FROM s WHERE ints = (SELECT ints FROM s WHERE id = 1) ORDER BY id", 1);
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] <> ints ORDER BY id");
    }
  }

  @Test
  void scalarSubqueryArrayReportsArrayTypeAndClass(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT (SELECT ARRAY[1,2]) FROM s WHERE id = 1")) {
      var metaData = result.getMetaData();
      assertEquals(Types.ARRAY, metaData.getColumnType(1));
      assertEquals(Object[].class.getName(), metaData.getColumnClassName(1));
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          1, 2
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  private static void assertIds(java.sql.Statement statement, String sql, int... expected)
      throws java.sql.SQLException {
    List<Integer> ids = new java.util.ArrayList<>();
    try (var result = statement.executeQuery(sql)) {
      while (result.next()) {
        ids.add(result.getInt(1));
      }
    }
    assertEquals(java.util.Arrays.stream(expected).boxed().toList(), ids, sql);
  }

  private static Connection connect(Path directory) throws Exception {
    Class.forName("se.alipsa.jparq.JParqDriver");
    return DriverManager.getConnection("jdbc:jparq:" + directory.toAbsolutePath());
  }

  /**
   * Writes a minimal dataset with one physical array column.
   *
   * @param directory
   *          dataset directory
   * @throws IOException
   *           if writing fails
   */
  private static void writeDataset(Path directory) throws IOException {
    Schema schema = SchemaBuilder.record("array_row").fields().requiredInt("id").name("ints")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createArray(Schema.create(Schema.Type.INT))))
        .noDefault().endRecord();
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("s.parquet").toUri()),
        new org.apache.hadoop.conf.Configuration());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).build()) {
      for (int id = 1; id <= 2; id++) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("ints",
            id == 1
                ? new GenericData.Array<>(schema.getField("ints").schema().getTypes().get(1), List.of(1, 2))
                : null);
        writer.write(record);
      }
    }
  }
}
