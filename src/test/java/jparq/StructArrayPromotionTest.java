package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.function.ArrayFunctions;

/**
 * Verifies STRUCT SQL type combinations independently of Avro storage details.
 */
class StructArrayPromotionTest {
  /**
   * Ignores optional constraints and union order, including NULL field values.
   */
  @Test
  void acceptsOptionalFields() {
    Schema integer = Schema.create(Schema.Type.INT);
    for (Schema optional : List.of(Schema.createUnion(Schema.create(Schema.Type.NULL), integer),
        Schema.createUnion(integer, Schema.create(Schema.Type.NULL)))) {
      for (Object value : Arrays.asList(null, 2)) {
        var first = record("required", integer, 1);
        var second = record("optional", optional, value);
        assertFieldValues(first, second, 1, value);
        assertFieldValues(second, first, value, 1);
      }
    }
  }

  /** Widens numeric field declarations and values, including all-NULL inputs. */
  @Test
  void promotesNumericFields() {
    var integer = record("ints", Schema.create(Schema.Type.INT), 1);
    var wide = record("longs", Schema.create(Schema.Type.LONG), 2L);
    assertFieldValues(integer, wide, 1L, 2L);
    assertFieldValues(wide, integer, 2L, 1L);
    var result = ArrayFunctions.homogenizeArrayValues(List.of(record("nullint", Schema.create(Schema.Type.INT), null),
        record("nulllong", Schema.create(Schema.Type.LONG), null)));
    var first = (IndexedRecord) result.getFirst();
    assertEquals(Schema.Type.LONG, first.getSchema().getField("x").schema().getType());
    var a = record("nestedA", Schema.createArray(Schema.create(Schema.Type.INT)), List.of(1));
    var b = record("nestedB", Schema.createArray(Schema.create(Schema.Type.LONG)), List.of(2L));
    assertFieldValues(a, b, new Object[]{
        1L
    }, new Object[]{
        2L
    });
    assertFieldValues(record("mapA", Schema.createMap(Schema.create(Schema.Type.INT)), Map.of("k", 1)),
        record("mapB", Schema.createMap(Schema.create(Schema.Type.LONG)), Map.of("k", 2L)), Map.of("k", 1L),
        Map.of("k", 2L));
  }

  /** Combines timestamp storage precision without changing timestamp values. */
  @Test
  void acceptsTimestampPrecisionDifferences() {
    var millis = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    var micros = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    var first = record("millis", millis, 1234L);
    var second = record("micros", micros, 2345678L);
    assertFieldValues(first, second, Timestamp.from(java.time.Instant.ofEpochMilli(1234)),
        Timestamp.from(java.time.Instant.ofEpochSecond(2, 345678000)));
    assertFieldValues(second, first, Timestamp.from(java.time.Instant.ofEpochSecond(2, 345678000)),
        Timestamp.from(java.time.Instant.ofEpochMilli(1234)));
  }

  /**
   * Keeps UTC instants and local gap fields when STRUCT arrays acquire offsets.
   */
  @Test
  void keepsMixedTimestampFieldsWithoutTimeZone() {
    Schema utc = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema local = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Instant instant = Instant.parse("2024-07-01T12:00:00Z");
    LocalDateTime gap = LocalDateTime.parse("2024-03-31T02:30:00");
    var first = record("utcField", utc, instant.toEpochMilli());
    var second = record("localField", local, gap.toInstant(ZoneOffset.UTC).toEpochMilli());
    assertFieldValues(first, second, Timestamp.from(instant), Timestamp.valueOf(gap));
    assertFieldValues(second, first, Timestamp.valueOf(gap), Timestamp.from(instant));
  }

  /**
   * Selects microsecond TIME storage in either order without discarding stored
   * fractions.
   */
  @Test
  void preservesTimeStoragePrecision() {
    Schema millis = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema micros = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    var first = record("timeMillis", millis, 1234);
    var second = record("timeMicros", micros, 2_123_456L);
    for (var pair : List.of(List.of(first, second), List.of(second, first))) {
      var values = ArrayFunctions.homogenizeArrayValues(new java.util.ArrayList<>(pair));
      for (int index = 0; index < pair.size(); index++) {
        var value = (IndexedRecord) values.get(index);
        Schema field = value.getSchema().getFields().getFirst().schema();
        assertEquals("time-micros", field.getLogicalType().getName());
        assertEquals(pair.get(index) == first ? 1_234_000L : 2_123_456L, value.get(0));
      }
    }
    Schema annotated = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    annotated.addProp("storage", "other writer");
    var result = ArrayFunctions.homogenizeArrayValues(List.of(second, record("annotatedTime", annotated, 3_456_789L)));
    assertEquals(2_123_456L, ((IndexedRecord) result.getFirst()).get(0));
  }

  /** Reports timestamp promotion overflow as a datetime data exception. */
  @Test
  void reportsTimestampStorageOverflow() {
    Schema millis = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema micros = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    for (long raw : new long[]{
        9_300_000_000_000_000L, -9_300_000_000_000_000L
    }) {
      var first = record("overflowMillis", millis, raw);
      var second = record("overflowMicros", micros, 0L);
      for (var pair : List.of(List.of(first, second), List.of(second, first))) {
        var failure = assertThrows(IllegalArgumentException.class,
            () -> ArrayFunctions.homogenizeArrayValues(new java.util.ArrayList<>(pair)));
        var sql = assertInstanceOf(java.sql.SQLDataException.class, failure.getCause());
        assertEquals("22008", sql.getSQLState());
        assertTrue(sql.getMessage().endsWith(java.time.Instant.ofEpochMilli(raw).toString()), sql.getMessage());
        assertInstanceOf(ArithmeticException.class, sql.getCause());
      }
    }
  }

  /**
   * Preserves SQLState when timestamp promotion fails through a JDBC getter.
   *
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or connection fails
   */
  @Test
  void reportsStoredTimestampOverflow(@TempDir Path directory) throws Exception {
    var millis = record("storedMillis", LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
        9_300_000_000_000_000L);
    var micros = record("storedMicros", LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
        0L);
    Schema schema = SchemaBuilder.record("overflow").fields().name("a").type(millis.getSchema()).noDefault().name("b")
        .type(micros.getSchema()).noDefault().endRecord();
    var row = new GenericData.Record(schema);
    row.put("a", millis);
    row.put("b", micros);
    write(directory, "overflow", row);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      var failure = assertThrows(java.sql.SQLDataException.class, () -> {
        try (var result = statement.executeQuery("SELECT ARRAY[a,b] FROM overflow")) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      });
      assertEquals("22008", failure.getSQLState());
    }
  }

  /**
   * Reuses source schemas while still allowing later widening and independent
   * executions.
   */
  @Test
  void resolvesRepeatedSchemasAndLatePromotion() {
    var required = record("repeatedRequired", Schema.create(Schema.Type.INT), 1);
    var optional = record("repeatedOptional",
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)), 2);
    final var wide = record("lateWide", Schema.create(Schema.Type.LONG), 3L);
    var input = new java.util.ArrayList<Object>();
    for (int i = 0; i < 1000; i++) {
      input.add(i % 2 == 0 ? required : optional);
    }
    input.add(wide);
    var result = ArrayFunctions.homogenizeArrayValues(input);
    Schema common = ((IndexedRecord) result.getFirst()).getSchema();
    for (int i = 0; i < result.size(); i++) {
      var value = (IndexedRecord) result.get(i);
      assertSame(common, value.getSchema());
      assertEquals(i == 1000 ? 3L : i % 2 == 0 ? 1L : 2L, field(value));
    }
    assertFieldValues(required, optional, 1, 2);
  }

  /**
   * Describes differing degrees and union alternatives without empty paths or
   * redundant labels.
   */
  @Test
  void describesStructDegreeAndUnionMismatch() {
    Schema two = SchemaBuilder.record("twoFields").fields().requiredInt("x").requiredInt("y").endRecord();
    var one = record("oneField", Schema.create(Schema.Type.INT), 1);
    var count = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(List.of(new GenericData.Record(two), one)));
    assertEquals("ARRAY elements must share a common type; found STRUCT with 2 fields and STRUCT with 1 field",
        count.getMessage());
    Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.STRING));
    var types = assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.homogenizeArrayValues(
        List.of(record("unionField", union, 1), record("booleanField", Schema.create(Schema.Type.BOOLEAN), true))));
    assertTrue(types.getMessage().contains("STRUCT field 'x': UNION(NULL, INTEGER, VARCHAR) and BOOLEAN"),
        types.getMessage());
  }

  /** Combines decimal bounds and scales and retains exact values. */
  @Test
  void promotesDecimalFields() {
    Schema a = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema b = LogicalTypes.decimal(12, 3).addToSchema(Schema.create(Schema.Type.BYTES));
    var conversion = new Conversions.DecimalConversion();
    var first = record("decimalA", a, conversion.toBytes(new BigDecimal("123.45"), a, a.getLogicalType()));
    var second = record("decimalB", b, conversion.toBytes(new BigDecimal("234.567"), b, b.getLogicalType()));
    assertFieldValues(first, second, new BigDecimal("123.450"), new BigDecimal("234.567"));
  }

  /** Ignores string properties and nested fixed/enum type names. */
  @Test
  void acceptsStorageNamesAndProperties() {
    Schema plain = Schema.create(Schema.Type.STRING);
    Schema annotated = Schema.create(Schema.Type.STRING);
    annotated.addProp("avro.java.string", "String");
    assertFieldValues(record("plain", plain, "a"), record("annotated", annotated, "b"), "a", "b");
    Schema f1 = Schema.createFixed("f1", null, null, 4);
    Schema f2 = Schema.createFixed("f2", null, null, 4);
    byte[] bytes = {
        0, 1, 2, 3
    };
    var fixed = ArrayFunctions.homogenizeArrayValues(List.of(record("fixedA", f1, new GenericData.Fixed(f1, bytes)),
        record("fixedB", f2, new GenericData.Fixed(f2, bytes))));
    for (Object value : fixed) {
      org.junit.jupiter.api.Assertions.assertArrayEquals(bytes, (byte[]) field((IndexedRecord) value));
    }
    Schema e1 = Schema.createEnum("e1", null, null, List.of("A", "B"));
    Schema e2 = Schema.createEnum("e2", null, null, List.of("A", "B"));
    assertFieldValues(record("enumA", e1, new GenericData.EnumSymbol(e1, "A")),
        record("enumB", e2, new GenericData.EnumSymbol(e2, "B")), "A", "B");
  }

  /** Names the first differing field without dumping a large Avro schema. */
  @Test
  void reportsFirstDifferingField() {
    var builder = SchemaBuilder.record("wide").fields();
    for (int i = 0; i < 200; i++) {
      builder.requiredInt("field" + i);
    }
    Schema first = builder.endRecord();
    var other = SchemaBuilder.record("wideOther").fields().requiredString("field0");
    for (int i = 1; i < 200; i++) {
      other.requiredInt("field" + i);
    }
    var failure = assertThrows(IllegalArgumentException.class, () -> ArrayFunctions
        .homogenizeArrayValues(List.of(new GenericData.Record(first), new GenericData.Record(other.endRecord()))));
    assertTrue(failure.getMessage().contains("STRUCT field 'field0': INTEGER and VARCHAR"), failure.getMessage());
    assertTrue(failure.getMessage().length() < 150, failure.getMessage());
  }

  /**
   * Uses entry iteration exclusively and retains unchanged maps and boxed
   * numbers.
   */
  @Test
  void retainsUnchangedMapsWithoutUsingSeparateViews() {
    var map = new EntryOnlyMap();
    map.put("a", 1000);
    map.put("b", 2000);
    assertSame(map, ArrayFunctions.homogenizeArrayValues(List.of(map)).getFirst());
    var result = ArrayFunctions.homogenizeArrayValues(List.of(map, Map.of("c", 3000L)));
    assertEquals(Map.of("a", 1000L, "b", 2000L), result.getFirst());
    var lateChange = new LinkedHashMap<Object, Object>();
    lateChange.put("a", 1000L);
    lateChange.put("b", 2000);
    assertEquals(Map.of("a", 1000L, "b", 2000L), ArrayFunctions.homogenizeArrayValues(List.of(lateChange)).getFirst());
  }

  /**
   * Combines required and optional stored fields, both within one file and across
   * a join.
   *
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @Test
  void acceptsStoredOptionalFieldsAndJoinedSchemas(@TempDir Path directory) throws Exception {
    Schema integer = Schema.create(Schema.Type.INT);
    Schema optional = Schema.createUnion(Schema.create(Schema.Type.NULL), integer);
    var required = record("required", integer, 1);
    var nullable = record("optional", optional, null);
    Schema rowSchema = SchemaBuilder.record("row").fields().name("sr").type(required.getSchema()).noDefault().name("so")
        .type(nullable.getSchema()).noDefault().endRecord();
    var row = new GenericData.Record(rowSchema);
    row.put("sr", required);
    row.put("so", nullable);
    write(directory, "t", row);
    var wide = record("wideValue", Schema.create(Schema.Type.LONG), 2L);
    Schema joinSchema = SchemaBuilder.record("joined").fields().name("s").type(wide.getSchema()).noDefault()
        .endRecord();
    var joined = new GenericData.Record(joinSchema);
    joined.put("s", wide);
    write(directory, "u", joined);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String sql : List.of("SELECT ARRAY[sr,so] FROM t", "SELECT ARRAY[t.sr,t.so,u.s] FROM t CROSS JOIN u")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next());
          Object[] records = (Object[]) result.getObject(1);
          if (sql.contains("JOIN")) {
            assertEquals(1L, field((IndexedRecord) records[0]));
          } else {
            assertEquals(1, field((IndexedRecord) records[0]));
          }
          assertEquals(null, field((IndexedRecord) records[1]));
          if (sql.contains("JOIN")) {
            assertEquals(2L, field((IndexedRecord) records[2]));
          }
        }
      }
    }
  }

  /**
   * Reads a STRUCT field using its retained Avro declaration.
   *
   * @param record
   *          normalized STRUCT
   * @return decoded first field
   */
  private static Object field(IndexedRecord record) {
    return AvroCoercions.unwrap(record.get(0), record.getSchema().getFields().getFirst().schema());
  }

  /**
   * Verifies field values in a pair of combined STRUCT elements.
   *
   * @param first
   *          first STRUCT
   * @param second
   *          second STRUCT
   * @param expectedFirst
   *          expected first field value
   * @param expectedSecond
   *          expected second field value
   */
  private static void assertFieldValues(IndexedRecord first, IndexedRecord second, Object expectedFirst,
      Object expectedSecond) {
    var values = ArrayFunctions.homogenizeArrayValues(List.of(first, second));
    assertTrue(
        java.util.Objects.deepEquals(expectedFirst, field(assertInstanceOf(IndexedRecord.class, values.get(0)))));
    assertTrue(
        java.util.Objects.deepEquals(expectedSecond, field(assertInstanceOf(IndexedRecord.class, values.get(1)))));
  }

  /**
   * Creates a one-field STRUCT.
   *
   * @param name
   *          Avro name
   * @param fieldSchema
   *          field declaration
   * @param value
   *          field value
   * @return STRUCT value
   */
  private static GenericData.Record record(String name, Schema fieldSchema, Object value) {
    var schema = SchemaBuilder.record(name).fields().name("x").type(fieldSchema).noDefault().endRecord();
    var record = new GenericData.Record(schema);
    record.put(0, value);
    return record;
  }

  /**
   * Writes a fixture record.
   *
   * @param directory
   *          database directory
   * @param table
   *          table name
   * @param row
   *          fixture row
   * @throws Exception
   *           if writing fails
   */
  private static void write(Path directory, String table, GenericData.Record row) throws Exception {
    var config = new org.apache.hadoop.conf.Configuration();
    var path = new org.apache.hadoop.fs.Path(directory.resolve(table + ".parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericData.Record>builder(HadoopOutputFile.fromPath(path, config))
        .withSchema(row.getSchema()).withConf(config).build()) {
      writer.write(row);
    }
  }

  /** A map whose independent views must not be used to pair entries. */
  private static final class EntryOnlyMap extends LinkedHashMap<String, Integer> {
    @Override
    public Set<String> keySet() {
      throw new AssertionError("Use entrySet to keep keys and values paired");
    }

    @Override
    public Collection<Integer> values() {
      throw new AssertionError("Use entrySet to keep keys and values paired");
    }
  }
}
