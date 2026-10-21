package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Regression coverage for structured values crossing materialization
 * boundaries.
 */
class DerivedComplexTypeTest {

  /**
   * Preserves source schemas, nested values, aliases, NULLs, and empty arrays.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void retainsPhysicalTypes(boolean predicate, boolean projection, @TempDir Path directory) throws Exception {
    writeFixture(directory);
    try (var connection = DriverManager.getConnection(url(directory, predicate, projection))) {
      for (String query : List.of("SELECT id, tags, details, dates, decs FROM small",
          "SELECT id, tags, details, dates, decs FROM (SELECT * FROM small) t",
          "WITH t AS (SELECT * FROM small) SELECT id, tags, details, dates, decs FROM t",
          "WITH t(i, a, s, d, n) AS (SELECT id, tags, details, dates, decs FROM small) "
              + "SELECT i, a, s, d, n FROM (SELECT * FROM t) u",
          "SELECT t.id, t.tags, t.details, t.dates, t.decs FROM (SELECT * FROM small) t "
              + "JOIN small q ON q.id = t.id",
          "SELECT t.id, t.tags, t.details, t.dates, t.decs FROM small q "
              + "CROSS JOIN LATERAL (SELECT * FROM small p WHERE p.id = q.id) t")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query + " ORDER BY 1")) {
          assertEquals(Types.ARRAY, result.getMetaData().getColumnType(2), query);
          assertEquals(Types.STRUCT, result.getMetaData().getColumnType(3), query);
          assertTrue(result.next(), query);
          assertArrayEquals(new Object[]{
              "Sport", "Family"
          }, assertInstanceOf(Object[].class, result.getObject(2)), query);
          GenericRecord details = assertInstanceOf(GenericRecord.class, result.getObject(3), query);
          assertEquals("m1", details.get("m").toString(), query);
          assertEquals(GenericRecord.class.getName(), result.getMetaData().getColumnClassName(3), query);
          assertEquals(details.toString(), result.getString(3), query);
          assertEquals(7, details.get("w"), query);
          assertArrayEquals(new Object[]{
              Date.valueOf("2024-01-01")
          }, assertInstanceOf(Object[].class, result.getObject(4)), query);
          assertArrayEquals(new Object[]{
              new BigDecimal("1.50")
          }, assertInstanceOf(Object[].class, result.getObject(5)), query);
          assertTrue(result.next(), query);
          assertArrayEquals(new Object[0], assertInstanceOf(Object[].class, result.getObject(2)), query);
          assertNull(result.getObject(3), query);
          assertTrue(result.next(), query);
          assertNull(result.getObject(2), query);
          assertFalse(result.next(), query);
        }
      }
    }
  }

  /**
   * Keeps array elements usable after computed projections and set operations.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void retainsComputedAndSetTypes(boolean predicate, boolean projection, @TempDir Path directory) throws Exception {
    writeFixture(directory);
    try (var connection = DriverManager.getConnection(url(directory, predicate, projection))) {
      for (String input : List.of("(SELECT ARRAY[4,6] AS a) t",
          "(SELECT ARRAY(SELECT id FROM small WHERE id < 3 ORDER BY id) AS a) t",
          "(SELECT ARRAY[4,6] AS a UNION ALL SELECT ARRAY[8] AS a) t")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT a FROM " + input)) {
          assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1), input);
          assertTrue(result.next(), input);
          Object[] expected = input.contains("ARRAY(SELECT") ? new Object[]{
              1, 2
          } : new Object[]{
              4L, 6L
          };
          assertArrayEquals(expected, assertInstanceOf(Object[].class, result.getObject(1)), input);
        }
      }
      for (String prefix : List.of(
          "WITH t AS (SELECT tags, details, dates, decs FROM small UNION ALL "
              + "SELECT tags, details, dates, decs FROM small WHERE id < 0) SELECT * FROM t",
          "SELECT * FROM (SELECT tags, details, dates, decs FROM small UNION ALL "
              + "SELECT tags, details, dates, decs FROM small WHERE id < 0) t")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(prefix)) {
          assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1), prefix);
          assertEquals(Types.STRUCT, result.getMetaData().getColumnType(2), prefix);
          assertTrue(result.next(), prefix);
          assertArrayEquals(new Object[]{
              "Sport", "Family"
          }, assertInstanceOf(Object[].class, result.getObject(1)));
          assertInstanceOf(GenericRecord.class, result.getObject(2));
          assertArrayEquals(new Object[]{
              Date.valueOf("2024-01-01")
          }, assertInstanceOf(Object[].class, result.getObject(3)));
          assertArrayEquals(new Object[]{
              new BigDecimal("1.50")
          }, assertInstanceOf(Object[].class, result.getObject(4)));
        }
      }
      for (String sql : List.of("SELECT * FROM (SELECT ARRAY[4,6] AS a) t WHERE 4 = ANY(a)",
          "WITH t AS (SELECT ARRAY[4,6] AS a) SELECT * FROM t WHERE 4 = ANY(a)",
          "WITH t AS (SELECT tags FROM small UNION ALL SELECT tags FROM small) "
              + "SELECT * FROM t WHERE 'Sport' = ANY(tags)")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1), sql);
        }
      }
      for (String sql : List.of("SELECT tags, details FROM (SELECT tags, details FROM small WHERE id < 0) t",
          "WITH t AS (SELECT tags, details FROM small WHERE id < 0 UNION ALL "
              + "SELECT tags, details FROM small WHERE id < 0) SELECT * FROM t")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1), sql);
          assertEquals(Types.STRUCT, result.getMetaData().getColumnType(2), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Infers nested, empty, nullable, and logical computed array elements.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void infersArrayElements(boolean predicate, boolean projection, @TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection(url(directory, predicate, projection))) {
      for (String expression : List.of("ARRAY[]", "ARRAY[NULL]", "ARRAY[DATE '2024-01-01', NULL]", "ARRAY[1.50, 2.75]",
          "ARRAY[ARRAY[1,2], ARRAY[3,4]]")) {
        String sql = "WITH t AS (SELECT " + expression + " AS a) SELECT a FROM (SELECT * FROM t) u";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1), sql);
          assertTrue(result.next(), sql);
          Object[] values = assertInstanceOf(Object[].class, result.getObject(1), sql);
          if (expression.equals("ARRAY[]")) {
            assertEquals(0, values.length);
          } else if (expression.equals("ARRAY[NULL]")) {
            assertArrayEquals(new Object[]{
                null
            }, values);
          } else if (expression.contains("DATE")) {
            assertArrayEquals(new Object[]{
                Date.valueOf("2024-01-01"), null
            }, values);
          } else if (expression.contains("1.50")) {
            assertArrayEquals(new Object[]{
                new BigDecimal("1.50"), new BigDecimal("2.75")
            }, values);
          } else {
            assertArrayEquals(new Object[]{
                1L, 2L
            }, assertInstanceOf(Object[].class, values[0]));
            assertArrayEquals(new Object[]{
                3L, 4L
            }, assertInstanceOf(Object[].class, values[1]));
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Infers array element types across all set branches independently of order.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void promotesSetArrayElements(boolean predicate, boolean projection, @TempDir Path directory) throws Exception {
    writeFixture(directory);
    try (var connection = DriverManager.getConnection(url(directory, predicate, projection))) {
      String physical = "SELECT ints AS a FROM small WHERE id = 1";
      for (String computed : List.of("SELECT ARRAY[1.5] AS a", "SELECT ARRAY['x'] AS a",
          "SELECT decs AS a FROM small WHERE id = 1")) {
        for (String branches : List.of(physical + " UNION ALL " + computed, computed + " UNION ALL " + physical,
            physical + " UNION ALL " + computed + " UNION ALL " + physical)) {
          for (String sql : List.of("WITH t AS (" + branches + ") SELECT a FROM t",
              "SELECT a FROM (" + branches + ") t")) {
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1), sql);
              java.util.Set<List<Object>> actual = new java.util.HashSet<>();
              while (result.next()) {
                actual.add(java.util.Arrays.asList(assertInstanceOf(Object[].class, result.getObject(1), sql)));
              }
              java.util.Set<List<Object>> expected = computed.contains("decs")
                  ? java.util.Set.of(List.of(new BigDecimal("1.00"), new BigDecimal("2.00")),
                      List.of(new BigDecimal("1.50")))
                  : computed.contains("1.5")
                      ? java.util.Set.of(List.of(new BigDecimal("1.0"), new BigDecimal("2.0")),
                          List.of(new BigDecimal("1.5")))
                      : java.util.Set.of(List.of("1", "2"), List.of("x"));
              assertEquals(expected, actual, sql);
            }
          }
        }
      }
      for (String sql : List.of("SELECT JSON_OBJECT(KEY 'k' VALUE details) FROM small WHERE id = 1",
          "SELECT JSON_ARRAY(details) FROM small WHERE id = 1")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next());
          assertEquals(sql.contains("JSON_OBJECT") ? "{\"k\":{\"m\":\"m1\",\"w\":7}}" : "[{\"m\":\"m1\",\"w\":7}]",
              result.getString(1), sql);
        }
      }
    }
  }

  /**
   * Compares physical arrays by content across grouping and all set operators.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void arrayRelationalKeys(boolean predicate, boolean projection, @TempDir Path directory) throws Exception {
    writeFixture(directory);
    try (var connection = DriverManager.getConnection(url(directory, predicate, projection))) {
      for (String operator : List.of("UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL")) {
        String sql = "SELECT tags FROM small " + operator + " SELECT tags FROM small";
        int expected = operator.startsWith("EXCEPT") ? 0 : operator.equals("UNION ALL") ? 6 : 3;
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          int count = 0;
          while (result.next()) {
            Object value = result.getObject(1);
            if (value != null) {
              assertInstanceOf(Object[].class, value, sql);
            }
            count++;
          }
          assertEquals(expected, count, sql);
        }
      }
      String cte = "WITH t AS (SELECT tags FROM small UNION ALL SELECT tags FROM small) ";
      for (String sql : List.of(cte + "SELECT DISTINCT tags FROM t",
          cte + "SELECT tags, COUNT(*) FROM t GROUP BY tags")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1), sql);
          int count = 0;
          while (result.next()) {
            if (sql.contains("COUNT")) {
              assertEquals(2, result.getInt(2), sql);
            }
            count++;
          }
          assertEquals(3, count, sql);
        }
      }
    }
  }

  /**
   * Falls back safely when differing struct schemas contribute only NULLs.
   *
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @org.junit.jupiter.api.Test
  void allNullStructSetFallsBackSafely(@TempDir Path directory) throws Exception {
    writeFixture(directory);
    Schema details = SchemaBuilder.record("other_details").fields().requiredString("m").requiredInt("w").endRecord();
    Schema schema = SchemaBuilder.record("other").fields().name("details").type(optional(details)).noDefault()
        .endRecord();
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("other.parquet").toUri()),
        new Configuration(false));
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).build()) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("details", null);
      writer.write(record);
    }
    try (var connection = DriverManager.getConnection(url(directory, true, true))) {
      String first = "SELECT details FROM small WHERE id > 1";
      String second = "SELECT details FROM other";
      for (String branches : List.of(first + " UNION ALL " + second, second + " UNION ALL " + first)) {
        String sql = "WITH t AS (" + branches + ") SELECT * FROM t";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(Types.VARCHAR, result.getMetaData().getColumnType(1), sql);
          for (int row = 0; row < 3; row++) {
            assertTrue(result.next(), sql);
            assertNull(result.getObject(1), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Builds a JDBC URL with independent pushdown settings.
   *
   * @param directory
   *          dataset directory
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @return JDBC connection URL
   */
  private static String url(Path directory, boolean predicate, boolean projection) {
    return "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
  }

  /**
   * Writes physical record and logical array columns in the supported legacy list
   * layout.
   *
   * @param directory
   *          dataset directory
   * @throws Exception
   *           if writing fails
   */
  private static void writeFixture(Path directory) throws Exception {
    Schema details = SchemaBuilder.record("details").fields().requiredString("m").requiredInt("w").endRecord();
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema decimal = LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema schema = SchemaBuilder.record("small").fields().requiredInt("id").name("tags")
        .type(optional(Schema.createArray(Schema.create(Schema.Type.STRING)))).noDefault().name("details")
        .type(optional(details)).noDefault().name("dates").type(optional(Schema.createArray(date))).noDefault()
        .name("decs").type(optional(Schema.createArray(decimal))).noDefault().name("ints")
        .type(optional(Schema.createArray(Schema.create(Schema.Type.INT)))).noDefault().endRecord();
    Configuration configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", true);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("small.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 1; id <= 3; id++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("ints", id == 1 ? List.of(1, 2) : null);
        GenericRecord struct = new GenericData.Record(details);
        struct.put("m", "m1");
        struct.put("w", 7);
        record.put("details", id == 1 ? struct : null);
        record.put("tags", id == 1 ? List.of("Sport", "Family") : id == 2 ? List.of() : null);
        record.put("dates", id == 1 ? List.of(19723) : null);
        record.put("decs",
            id == 1 ? List.of(ByteBuffer.wrap(new BigDecimal("1.50").unscaledValue().toByteArray())) : null);
        writer.write(record);
      }
    }
  }

  /**
   * Adds column nullability.
   *
   * @param schema
   *          non-null schema
   * @return nullable schema
   */
  private static Schema optional(Schema schema) {
    return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
  }
}
