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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression coverage for recursive SQL/JSON array and struct serialization.
 */
class JsonArraySerializationTest {
  @TempDir
  Path directory;

  /**
   * Embeds ARRAY constructors recursively, including NULL and empty arrays.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void serializesComputedArrays() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String function : List.of("JSON_OBJECT(KEY 'k' VALUE %s)", "JSON_ARRAY(%s)")) {
        for (String[] sample : List.of(new String[]{
            "ARRAY[1,2]", "[1,2]"
        }, new String[]{
            "ARRAY[1,NULL,2]", "[1,null,2]"
        }, new String[]{
            "ARRAY[ARRAY[1,2],NULL,ARRAY[3]]", "[[1,2],null,[3]]"
        }, new String[]{
            "ARRAY[]", "[]"
        }, new String[]{
            "ARRAY[TRUE,FALSE,NULL]", "[true,false,null]"
        }, new String[]{
            "ARRAY['x','it''s',NULL]", "[\"x\",\"it's\",null]"
        }, new String[]{
            "ARRAY(SELECT 1 UNION ALL SELECT 2)", "[1,2]"
        })) {
          assertJson(connection, function, sample[0], "", sample[1]);
        }
      }
    }
  }

  /**
   * Embeds physical arrays and records through projections and materializations,
   * decoding logical fields and preserving NULL elements and empty arrays.
   *
   * @param pushdown
   *          whether projection and predicate pushdown are enabled
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void serializesStoredArrays(boolean pushdown) throws Exception {
    writeArrays();
    String record = "{\"m\":\"m\\\"1\",\"w\":7,\"dec\":1.50,\"dt\":\"2024-01-01\","
        + "\"b\":\"AQI=\",\"nums\":[1,null,2],\"text\":\"[1,2]\"}";
    try (var connection = DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?projectionPushdown=" + pushdown + "&predicatePushdown=" + pushdown)) {
      for (String input : List.of("arrays", "(SELECT * FROM arrays) a",
          "(WITH c AS (SELECT * FROM arrays) SELECT * FROM c) a")) {
        for (String function : List.of("JSON_OBJECT(KEY 'k' VALUE %s)", "JSON_ARRAY(%s)")) {
          assertJson(connection, function, "ints", " FROM " + input + " WHERE id = 1", "[1,null,2]");
          assertJson(connection, function, "recs", " FROM " + input + " WHERE id = 1", "[" + record + ",null]");
          assertJson(connection, function, "details", " FROM " + input + " WHERE id = 1", record);
          for (String field : List.of("ints", "recs")) {
            assertJson(connection, function, field, " FROM " + input + " WHERE id = 2", "[]");
            assertJson(connection, function, field, " FROM " + input + " WHERE id = 3", "null");
          }
        }
      }
    }
  }

  /**
   * Checks one constructor's serialized output and result cardinality.
   *
   * @param connection
   *          open JDBC connection
   * @param function
   *          constructor format string
   * @param expression
   *          value expression
   * @param from
   *          optional FROM and WHERE clauses
   * @param value
   *          expected JSON value
   * @throws SQLException
   *           if querying fails
   */
  private static void assertJson(Connection connection, String function, String expression, String from, String value)
      throws SQLException {
    String sql = "SELECT " + function.formatted(expression) + from;
    String expected = function.startsWith("JSON_OBJECT") ? "{\"k\":" + value + "}" : "[" + value + "]";
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      assertTrue(result.next(), sql);
      assertEquals(expected, result.getString(1), sql);
      assertFalse(result.next(), sql);
    }
  }

  /**
   * Writes nullable arrays of scalars and records with logical and nested fields.
   *
   * @throws IOException
   *           if fixture writing fails
   */
  private void writeArrays() throws IOException {
    Schema integer = Schema.create(Schema.Type.INT);
    Schema ints = nullableArray(integer);
    Schema decimal = LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema details = SchemaBuilder.record("details").fields().requiredString("m").requiredInt("w").name("dec")
        .type(decimal).noDefault().name("dt").type(date).noDefault().requiredBytes("b").name("nums").type(ints)
        .noDefault().requiredString("text").endRecord();
    final Schema schema = SchemaBuilder.record("arrays").fields().requiredInt("id").name("ints").type(ints).noDefault()
        .name("recs").type(nullableArray(details)).noDefault().name("details").type(details).noDefault().endRecord();
    var detail = new GenericData.Record(details);
    detail.put("m", "m\"1");
    detail.put("w", 7);
    detail.put("dec", ByteBuffer.wrap(new BigDecimal("1.50").unscaledValue().toByteArray()));
    detail.put("dt", (int) LocalDate.parse("2024-01-01").toEpochDay());
    detail.put("b", ByteBuffer.wrap(new byte[]{
        1, 2
    }));
    detail.put("nums", Arrays.asList(1, null, 2));
    detail.put("text", "[1,2]");
    Configuration configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("arrays.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 1; id <= 3; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("ints", id == 1 ? Arrays.asList(1, null, 2) : id == 2 ? List.of() : null);
        row.put("recs", id == 1 ? Arrays.asList(detail, null) : id == 2 ? List.of() : null);
        row.put("details", detail);
        writer.write(row);
      }
    }
  }

  /**
   * Creates a nullable array with nullable elements.
   *
   * @param element
   *          non-null element schema
   * @return nullable array schema
   */
  private static Schema nullableArray(Schema element) {
    Schema nullableElement = Schema.createUnion(Schema.create(Schema.Type.NULL), element);
    return Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createArray(nullableElement));
  }
}
