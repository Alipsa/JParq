package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
    Schema schema = SchemaBuilder.record("array_columns").fields().requiredInt("id").name("tags")
        .type(nullableArray(Schema.create(Schema.Type.STRING))).noDefault().name("nums")
        .type(nullableArray(Schema.create(Schema.Type.INT))).noDefault().name("dates").type(nullableArray(date))
        .noDefault().name("decs").type(nullableArray(decimal)).noDefault().endRecord();
    Configuration configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", true);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("arrays.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 1; id <= 4; id++) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        for (String field : List.of("tags", "nums", "dates", "decs")) {
          List<?> values;
          if (id == 1 || id == 2) {
            Object value = switch (field) {
              case "tags" -> id == 1 ? "Sport" : "Family";
              case "nums" -> id == 1 ? 2 : 3;
              case "dates" -> (int) LocalDate.parse(id == 1 ? "2024-01-01" : "2024-01-02").toEpochDay();
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
