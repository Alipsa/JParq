package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.function.ArrayFunctions;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/**
 * Regression coverage for recursive MAP and STRUCT ARRAY typing in issue 278.
 */
class ComplexArrayElementsTest {

  /** Validates map contents at every depth, in either element order. */
  @Test
  void rejectsIncompatibleMapContents() {
    for (List<Object> pair : List.of(List.<Object>of(Map.of("k", 1), Map.of("k", "v")),
        List.<Object>of(Map.of(1, "v"), Map.of("k", "v")),
        List.<Object>of(Map.of("k", List.of(1)), Map.of("k", List.of("v"))),
        List.<Object>of(Map.of("k", Map.of("x", 1)), Map.of("k", Map.of("x", true))),
        List.<Object>of(Map.of("k", 1), Map.of("k", List.of(1))),
        List.<Object>of(Map.of("k", record("a", "x", Schema.create(Schema.Type.INT), null)),
            Map.of("k", record("b", "y", Schema.create(Schema.Type.INT), null))))) {
      var values = new ArrayList<>(pair);
      for (int order = 0; order < 2; order++) {
        assertIncompatible(values);
        assertIncompatible(List.of(List.of(values.get(0)), List.of(values.get(1))));
        Collections.reverse(values);
      }
    }
    Map<String, Object> heterogeneous = new LinkedHashMap<>();
    heterogeneous.put("a", 1);
    heterogeneous.put("b", "v");
    assertIncompatible(List.of(heterogeneous));
  }

  /**
   * Promotes map keys and nested values while preserving NULLs and empty maps.
   */
  @Test
  void promotesMapContents() {
    Map<Object, Object> first = new LinkedHashMap<>();
    first.put(1, Arrays.asList(null, 2));
    first.put(null, null);
    var second = Map.of(3L, List.of(4L));
    var values = Arrays.<Object>asList(null, Map.of(), first, second, null);
    var result = ArrayFunctions.homogenizeArrayValues(values);
    Map<Object, Object> expected = new LinkedHashMap<>();
    expected.put(1L, Arrays.asList(null, 2L));
    expected.put(null, null);
    assertEquals(Arrays.asList(null, Map.of(), expected, second, null), result);
    assertInstanceOf(Long.class, ((Map<?, ?>) result.get(2)).keySet().iterator().next());
    assertThrows(UnsupportedOperationException.class, () -> ((Map<?, ?>) result.get(2)).clear());
    assertEquals(Arrays.asList(null, 2), first.get(1));
    assertEquals(List.of(Map.of("k", Map.of("v", 1L)), Map.of("k", Map.of("v", 2L))),
        ArrayFunctions.homogenizeArrayValues(List.of(Map.of("k", Map.of("v", 1)), Map.of("k", Map.of("v", 2L)))));
    var nullMap = Collections.singletonMap("k", null);
    assertEquals(List.of(nullMap, Map.of("k", 1)),
        ArrayFunctions.homogenizeArrayValues(List.of(nullMap, Map.of("k", 1))));
  }

  /** Uses the session offset when temporal map values require promotion. */
  @Test
  void promotesTemporalMapValues() {
    var local = LocalDateTime.parse("2026-01-01T12:00:00.123456789");
    var zoned = OffsetDateTime.parse("2026-01-02T12:00:00+03:00");
    assertEquals(List.of(Map.of("k", local.atOffset(DateTimeFunctions.sessionOffset())), Map.of("k", zoned)),
        ArrayFunctions.homogenizeArrayValues(List.of(Map.of("k", local), Map.of("k", zoned))));
  }

  /** Rejects a key conversion that would silently discard a map entry. */
  @Test
  void rejectsPromotedKeyCollisions() {
    Map<Object, Object> map = new LinkedHashMap<>();
    map.put(1, "a");
    map.put(1L, "b");
    assertIncompatible(List.of(map));
  }

  /** Checks declared STRUCT types even when every field value is NULL. */
  @Test
  void validatesStructSchemas() {
    Schema integer = Schema.create(Schema.Type.INT);
    Schema string = Schema.create(Schema.Type.STRING);
    var first = record("first", "x", integer, null);
    var compatible = record("other_name", "x", integer, null);
    var result = ArrayFunctions.homogenizeArrayValues(Arrays.asList(null, first, compatible));
    assertSame(first, result.get(1));
    assertSame(compatible, result.get(2));
    for (var other : List.of(record("renamed", "y", integer, null), record("retyped", "x", string, null),
        record("nested", "x", Schema.createArray(integer), null),
        record("nonarray", "x", Schema.create(Schema.Type.BOOLEAN), null))) {
      assertIncompatible(Arrays.asList(null, first, other));
      assertIncompatible(List.of(other, first));
    }
    assertIncompatible(List.of(record("arrays1", "x", Schema.createArray(integer), null),
        record("arrays2", "x", Schema.createArray(string), null)));
    assertIncompatible(List.of(record("maps1", "x", Schema.createMap(integer), null),
        record("maps2", "x", Schema.createMap(string), null)));
    assertIncompatible(
        List.of(first, record("date", "x", LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)), null)));
    var inner1 = record("inner1", "v", integer, null);
    var inner2 = record("inner2", "v", string, null);
    assertIncompatible(
        List.of(record("outer1", "x", inner1.getSchema(), inner1), record("outer2", "x", inner2.getSchema(), inner2)));
    var twoFields = SchemaBuilder.record("two").fields().requiredInt("x").requiredInt("y").endRecord();
    var reversed = SchemaBuilder.record("reversed").fields().requiredInt("y").requiredInt("x").endRecord();
    assertIncompatible(List.of(new GenericData.Record(twoFields), first));
    assertIncompatible(List.of(new GenericData.Record(twoFields), new GenericData.Record(reversed)));
  }

  /**
   * Exercises incompatible stored columns at JDBC execution and getter
   * boundaries.
   *
   * @param expression
   *          incompatible ARRAY expression
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "ARRAY[mi,ms]", "ARRAY[ms,mi]", "ARRAY[s1,s2]", "ARRAY[s1,s3]", "ARRAY[mi,s1]", "ARRAY[ARRAY[mi],ARRAY[ms]]",
      "ARRAY[(SELECT mi FROM t),(SELECT ms FROM t)]"
  })
  void rejectsIncompatibleStoredColumns(String expression, @TempDir Path directory) throws Exception {
    writeFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String sql : List.of("SELECT " + expression + " FROM t",
          "SELECT a FROM (SELECT " + expression + " AS a FROM t) d",
          "WITH c AS (SELECT " + expression + " AS a FROM t) SELECT a FROM c")) {
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("ARRAY elements must share a common type"), failure.getMessage());
      }
    }
  }

  /**
   * Accepts compatible stored MAP and STRUCT elements and promotes map values.
   *
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if writing or querying the fixture fails
   */
  @Test
  void acceptsCompatibleStoredColumns(@TempDir Path directory) throws Exception {
    writeFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[mi,ml], ARRAY[s1,s1], ARRAY(SELECT mi FROM t) FROM t")) {
      assertTrue(result.next());
      Object[] maps = (Object[]) result.getObject(1);
      assertEquals(Map.of("k", 1L), maps[0]);
      assertEquals(Map.of("k", 2L), maps[1]);
      Object[] structs = (Object[]) result.getObject(2);
      assertEquals(structs[0], structs[1]);
      assertEquals(Map.of("k", 1), ((Object[]) result.getObject(3))[0]);
    }
  }

  /**
   * Asserts the public normalizer reports the required SQL error cause.
   *
   * @param values
   *          incompatible array values
   */
  private static void assertIncompatible(List<Object> values) {
    var failure = assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.homogenizeArrayValues(values));
    assertEquals("42000", assertInstanceOf(SQLSyntaxErrorException.class, failure.getCause()).getSQLState());
  }

  /**
   * Creates a one-field STRUCT value.
   *
   * @param name
   *          Avro record name
   * @param field
   *          SQL field name
   * @param type
   *          field declaration
   * @param value
   *          field value
   * @return record with the given declaration and value
   */
  private static GenericData.Record record(String name, String field, Schema type, Object value) {
    Schema schema = SchemaBuilder.record(name).fields().name(field).type(type).noDefault().endRecord();
    var record = new GenericData.Record(schema);
    record.put(field, value);
    return record;
  }

  /**
   * Writes the MAP and STRUCT columns from the reported reproduction.
   *
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if the Parquet file cannot be written
   */
  private static void writeFixture(Path directory) throws Exception {
    var s1 = record("struct1", "x", Schema.create(Schema.Type.INT), 1);
    var s2 = record("struct2", "y", Schema.create(Schema.Type.STRING), "b");
    var s3 = record("struct3", "x", Schema.create(Schema.Type.STRING), "b");
    Schema schema = SchemaBuilder.record("complex_arrays").fields().name("mi").type().map().values().intType()
        .noDefault().name("ms").type().map().values().stringType().noDefault().name("ml").type().map().values()
        .longType().noDefault().name("s1").type(s1.getSchema()).noDefault().name("s2").type(s2.getSchema()).noDefault()
        .name("s3").type(s3.getSchema()).noDefault().endRecord();
    var row = new GenericData.Record(schema);
    row.put("mi", Map.of("k", 1));
    row.put("ms", Map.of("k", "v"));
    row.put("ml", Map.of("k", 2L));
    row.put("s1", s1);
    row.put("s2", s2);
    row.put("s3", s3);
    var configuration = new org.apache.hadoop.conf.Configuration();
    var path = new org.apache.hadoop.fs.Path(directory.resolve("t.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericData.Record>builder(HadoopOutputFile.fromPath(path, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      writer.write(row);
    }
  }
}
