package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Keeps underscore-containing column names distinct from CONVERT type
 * arguments.
 */
class ConvertUnderscoreColumnTest {
  private static final List<String> COLUMNS = List.of("big_int", "small_int", "tiny_int", "int_4", "float_8",
      "double_precision");

  @TempDir
  static Path directory;

  /**
   * Writes columns whose names resemble numeric types after removing underscores.
   *
   * @throws Exception
   *           if writing the fixture fails
   */
  @BeforeAll
  static void writeFixture() throws Exception {
    var fields = SchemaBuilder.record("conv").fields();
    for (String column : COLUMNS) {
      fields.requiredInt(column);
    }
    var schema = fields.endRecord();
    var configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("conv.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int value : List.of(10, 20)) {
        var record = new GenericData.Record(schema);
        for (String column : COLUMNS) {
          record.put(column, value);
        }
        writer.write(record);
      }
    }
  }

  /**
   * Resolves both CONVERT argument orders against actual physical columns.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void underscoreColumnsRemainValueArguments(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown="
        + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String column : COLUMNS) {
        for (String type : List.of("VARCHAR", "DOUBLE")) {
          for (String arguments : List.of(type + ", " + column, column + ", " + type)) {
            String sql = "SELECT CONVERT(" + arguments + ") AS v FROM conv WHERE " + column + " >= 10 ORDER BY "
                + column;
            try (var result = statement.executeQuery(sql)) {
              for (int value : List.of(10, 20)) {
                assertTrue(result.next(), sql);
                if ("VARCHAR".equals(type)) {
                  assertEquals(Integer.toString(value), result.getObject(1), sql);
                } else {
                  assertEquals(Double.valueOf(value), result.getObject(1), sql);
                }
              }
              assertFalse(result.next(), sql);
            }
          }
        }
      }
    }
  }
}
