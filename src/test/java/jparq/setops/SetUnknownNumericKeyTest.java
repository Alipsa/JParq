package jparq.setops;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
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
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression coverage for mixed integer representations in OTHER set columns.
 */
class SetUnknownNumericKeyTest {
  @TempDir
  Path directory;

  /**
   * Writes the input for CASE branches yielding text, Integer, and Long values.
   *
   * @throws Exception
   *           if writing the fixture fails
   */
  @BeforeEach
  void writeFixture() throws Exception {
    var schema = SchemaBuilder.record("operands").fields().requiredInt("id").requiredLong("l").endRecord();
    var configuration = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(directory.resolve("o.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      for (int id = 0; id < 3; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("l", 5L);
        writer.write(row);
      }
    }
  }

  /**
   * Applies per-column normalization to DISTINCT and ALL set operators, including
   * intermediate results and an empty right operand.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void otherColumnsRetainNumericKeyEquality(boolean pushdown) throws Exception {
    String left = "SELECT 200000 AS stable, CASE WHEN id = 0 THEN 'a' WHEN id = 1 THEN 5 ELSE l END AS x FROM o";
    String text = "SELECT 200000, 'b' FROM o WHERE id = 0";
    String numbers = "SELECT 200000, CASE WHEN id = 0 THEN 'b' ELSE l END FROM o WHERE id IN (0, 2)";
    String mixed = "SELECT 200000, CASE WHEN id = 0 THEN 'a' ELSE l END FROM o";
    try (
        var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
        var statement = connection.createStatement()) {
      for (String operator : List.of("UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL")) {
        String right = operator.startsWith("UNION") ? text : operator.startsWith("INTERSECT") ? mixed : numbers;
        List<String> expected = switch (operator) {
          case "UNION" -> List.of("5", "a", "b");
          case "UNION ALL" -> List.of("5", "5", "a", "b");
          case "INTERSECT", "EXCEPT ALL" -> List.of("5", "a");
          case "INTERSECT ALL" -> List.of("5", "5", "a");
          case "EXCEPT" -> List.of("a");
          default -> throw new IllegalArgumentException("Unexpected test operator: " + operator);
        };
        String sql = left + " " + operator + " " + right;
        try (var result = statement.executeQuery(sql)) {
          assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1), sql);
          assertEquals(Types.OTHER, result.getMetaData().getColumnType(2), sql);
          List<String> actual = new ArrayList<>();
          while (result.next()) {
            assertEquals(200000, result.getObject(1), sql);
            actual.add(result.getString(2));
          }
          Collections.sort(actual);
          assertEquals(expected, actual, sql);
        }
      }
      for (String sql : List.of(left + " UNION " + text + " UNION " + numbers,
          left + " EXCEPT SELECT 200000, 'b' FROM o WHERE id = 99")) {
        try (var result = statement.executeQuery(sql)) {
          List<String> actual = new ArrayList<>();
          while (result.next()) {
            actual.add(result.getString(2));
          }
          Collections.sort(actual);
          assertEquals(sql.contains("EXCEPT") ? List.of("5", "a") : List.of("5", "a", "b"), actual, sql);
        }
      }
    }
  }
}
