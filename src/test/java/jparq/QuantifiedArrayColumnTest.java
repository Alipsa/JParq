package jparq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.junit.jupiter.params.provider.ValueSource;

/**
 * JDBC coverage for quantified comparisons over physically stored array
 * columns.
 */
class QuantifiedArrayColumnTest {

  /**
   * Reads primitive and logical array elements through physical and materialized
   * inputs, preserving NULL and empty-array truth values.
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
  void comparesArrayColumns(boolean pushdown, @TempDir Path directory) throws Exception {
    writeArrays(directory);
    try (var connection = DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown)) {
      for (String quantifier : List.of("ANY", "SOME", "ALL")) {
        for (String[] comparison : List.of(new String[]{
            "'Sport'", "tags"
        }, new String[]{
            "2", "nums"
        }, new String[]{
            "DATE '2024-01-01'", "dates"
        }, new String[]{
            "1.50", "decs"
        }, new String[]{
            "TIMESTAMP '2024-01-01 12:00:00.123'", "times"
        })) {
          String predicate = comparison[0] + " = " + quantifier + "(a." + comparison[1] + ")";
          List<Integer> expected = "ALL".equals(quantifier) ? List.of(3) : List.of(1);
          for (String input : List.of("arrays a", "(SELECT * FROM arrays) a")) {
            assertEquals(expected, ids(connection, "SELECT id FROM " + input + " WHERE " + predicate + " ORDER BY id"),
                predicate + " in " + input);
          }
          assertEquals(expected,
              ids(connection, "WITH a AS (SELECT * FROM arrays) SELECT id FROM a WHERE " + predicate + " ORDER BY id"),
              predicate);
          try (var statement = connection.createStatement();
              var result = statement.executeQuery("SELECT " + predicate + " AS matched FROM arrays a ORDER BY id")) {
            for (Boolean value : Arrays.asList(!"ALL".equals(quantifier), false, "ALL".equals(quantifier), null)) {
              assertTrue(result.next(), predicate);
              assertEquals(value, result.getObject(1), predicate);
            }
            assertFalse(result.next());
          }
        }
      }
      assertEquals(List.of(1, 2, 3), ids(connection, "SELECT id FROM arrays WHERE id < ALL(nums) ORDER BY id"));
      assertEquals(List.of(2, 3), ids(connection, "SELECT id FROM arrays WHERE NOT (2 = ANY(nums)) ORDER BY id"));
    }
  }

  /**
   * Retains logical array elements when binding outer rows into nested queries.
   *
   * @param pushdown
   *          predicate and projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void correlatedLogicalArrays(boolean pushdown, @TempDir Path directory) throws Exception {
    writeArrays(directory);
    try (var connection = DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown)) {
      for (String predicate : List.of("DATE '2024-01-01' = ANY(a.dates)", "1.50 = ANY(a.decs)",
          "'Sport' = SOME(a.tags)", "2 = ANY(a.nums)", "TIMESTAMP '2024-01-01 12:00:00.123' = ANY(a.times)")) {
        assertEquals(List.of(1), ids(connection, "SELECT a.id FROM arrays a WHERE EXISTS ("
            + "SELECT 1 FROM arrays b WHERE b.id = a.id AND " + predicate + ") ORDER BY a.id"), predicate);
        String sql = "SELECT a.id, (SELECT COUNT(*) FROM arrays b WHERE " + predicate
            + ") AS n FROM arrays a ORDER BY a.id";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          for (int id = 1; id <= 4; id++) {
            assertTrue(result.next(), sql);
            assertEquals(id, result.getInt(1));
            assertEquals(id == 1 ? 4 : 0, result.getInt(2), sql);
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Checks ARRAY metadata and logical JDBC elements through physical projections,
   * derived tables, and CTEs.
   *
   * @param pushdown
   *          predicate and projection pushdown setting
   * @param directory
   *          temporary dataset directory
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void projectedArrayTypes(boolean pushdown, @TempDir Path directory) throws Exception {
    writeArrays(directory);
    try (var connection = DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown)) {
      for (String sql : List.of("SELECT tags, dates, decs, times FROM arrays WHERE id = 1",
          "SELECT tags, dates, decs, times FROM (SELECT * FROM arrays) a WHERE id = 1",
          "WITH a AS (SELECT * FROM arrays) SELECT tags, dates, decs, times FROM a WHERE id = 1")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          for (int column = 1; column <= 4; column++) {
            assertEquals(Types.ARRAY, result.getMetaData().getColumnType(column), sql);
          }
          assertTrue(result.next(), sql);
          assertArrayEquals(new Object[]{
              "Sport", "Comfort"
          }, assertInstanceOf(Object[].class, result.getObject(1)));
          assertArrayEquals(new Object[]{
              Date.valueOf("2024-01-01"), Date.valueOf("2024-01-02")
          }, assertInstanceOf(Object[].class, result.getObject(2)));
          assertArrayEquals(new Object[]{
              new BigDecimal("1.50"), new BigDecimal("2.50")
          }, assertInstanceOf(Object[].class, result.getObject(3)));
          assertArrayEquals(new Object[]{
              Timestamp.valueOf("2024-01-01 12:00:00.123"), Timestamp.valueOf("2024-01-02 12:00:00.123")
          }, assertInstanceOf(Object[].class, result.getObject(4)));
          assertFalse(result.next());
        }
      }
      for (String[] pair : List.of(new String[]{
          "dates", "DATE '2024-01-01'"
      }, new String[]{
          "decs", "1.50"
      }, new String[]{
          "times", "TIMESTAMP '2024-01-01 12:00:00.123'"
      })) {
        assertEquals(List.of(1), ids(connection, "SELECT a.id FROM (SELECT * FROM arrays) a, UNNEST(a." + pair[0]
            + ") d(v) " + "WHERE d.v = " + pair[1] + " ORDER BY a.id"));
      }
    }
  }

  /**
   * Validates stored arrays alongside constructors and preserves logical elements
   * in ARRAY subqueries.
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
  void constructsNestedStoredArrays(boolean pushdown, @TempDir Path directory) throws Exception {
    writeArrays(directory);
    try (var connection = DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown)) {
      for (String expression : List.of("ARRAY[nums,tags]", "ARRAY[tags,nums]", "ARRAY[dates,times]",
          "ARRAY[nums,ARRAY[ARRAY[1]]]")) {
        try (var statement = connection.createStatement()) {
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
            try (var result = statement.executeQuery("SELECT " + expression + " FROM arrays WHERE id = 1")) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, expression);
          assertEquals("42000", failure.getSQLState());
        }
      }
      for (String input : List.of("arrays", "(SELECT * FROM arrays) a")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT ARRAY[nums,decs], ARRAY[nums,ARRAY[1.5]], "
                + "ARRAY[tags,ARRAY['x']] FROM " + input + " WHERE id = 1")) {
          assertTrue(result.next());
          assertEquals(List.of(List.of(new BigDecimal("2"), new BigDecimal("4")),
              List.of(new BigDecimal("1.50"), new BigDecimal("2.50"))), result.getObject(1));
          assertEquals(List.of(List.of(new BigDecimal("2"), new BigDecimal("4")), List.of(new BigDecimal("1.5"))),
              result.getObject(2));
          assertEquals(List.of(List.of("Sport", "Comfort"), List.of("x")), result.getObject(3));
          assertFalse(result.next());
        }
      }
      var dates = List.of(Date.valueOf("2024-01-01"), Date.valueOf("2024-01-02"));
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT ARRAY(SELECT dates FROM arrays WHERE id = 1)")) {
        assertTrue(result.next());
        assertEquals(List.of(dates), result.getObject(1));
        assertFalse(result.next());
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery(
              "SELECT d FROM (SELECT ARRAY(SELECT dates FROM arrays WHERE id = 1) AS a) s, UNNEST(s.a) t(d)")) {
        assertTrue(result.next());
        assertArrayEquals(dates.toArray(), assertInstanceOf(Object[].class, result.getObject(1)));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Writes old-layout arrays to isolate comparison behavior from issue #217's
   * modern-list projection problem.
   *
   * @param directory
   *          dataset directory
   * @throws IOException
   *           if writing fails
   */
  private static void writeArrays(Path directory) throws IOException {
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema decimal = LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema timestamp = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema = SchemaBuilder.record("array_columns").fields().requiredInt("id").name("tags")
        .type(nullableArray(Schema.create(Schema.Type.STRING))).noDefault().name("nums")
        .type(nullableArray(Schema.create(Schema.Type.INT))).noDefault().name("dates").type(nullableArray(date))
        .noDefault().name("decs").type(nullableArray(decimal)).noDefault().name("times").type(nullableArray(timestamp))
        .noDefault().endRecord();
    Configuration configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", true);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("arrays.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 1; id <= 4; id++) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        for (String field : List.of("tags", "nums", "dates", "decs", "times")) {
          List<?> values;
          if (id == 1 || id == 2) {
            Object value = switch (field) {
              case "tags" -> id == 1 ? "Sport" : "Family";
              case "nums" -> id == 1 ? 2 : 3;
              case "dates" -> (int) LocalDate.parse(id == 1 ? "2024-01-01" : "2024-01-02").toEpochDay();
              case "times" ->
                Timestamp.valueOf(id == 1 ? "2024-01-01 12:00:00.123" : "2024-01-02 12:00:00.123").getTime();
              case "decs" -> ByteBuffer.wrap(new BigDecimal(id == 1 ? "1.50" : "2.50").unscaledValue().toByteArray());
              default -> throw new IllegalArgumentException("Unexpected field " + field);
            };
            values = id == 1 ? List.of(value, valueForSecondElement(field)) : List.of(value);
          } else {
            values = List.of();
          }
          Schema arraySchema = schema.getField(field).schema().getTypes().get(1);
          record.put(field, id == 4 ? null : new GenericData.Array<>(arraySchema, values));
        }
        writer.write(record);
      }
    }
  }

  /**
   * Supplies a nonmatching second value to distinguish ANY from ALL.
   *
   * @param field
   *          array field name
   * @return raw Avro element
   */
  private static Object valueForSecondElement(String field) {
    return switch (field) {
      case "tags" -> "Comfort";
      case "nums" -> 4;
      case "dates" -> (int) LocalDate.parse("2024-01-02").toEpochDay();
      case "times" -> Timestamp.valueOf("2024-01-02 12:00:00.123").getTime();
      case "decs" -> ByteBuffer.wrap(new BigDecimal("2.50").unscaledValue().toByteArray());
      default -> throw new IllegalArgumentException("Unexpected field " + field);
    };
  }

  /**
   * Creates an optional array with required elements.
   *
   * @param element
   *          element schema
   * @return nullable array schema
   */
  private static Schema nullableArray(Schema element) {
    return Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createArray(element));
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
    } catch (SQLException exception) {
      throw new SQLException(sql, exception);
    }
    return ids;
  }
}
