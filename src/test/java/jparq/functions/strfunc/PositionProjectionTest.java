package jparq.functions.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
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
import org.junit.jupiter.params.provider.CsvSource;

/** Regression coverage for POSITION columns required by projection pushdown. */
class PositionProjectionTest {
  @TempDir
  Path directory;

  /**
   * Writes matching, nonmatching, empty, and null string inputs.
   *
   * @throws Exception
   *           if fixture creation fails
   */
  @BeforeEach
  void writeFixture() throws Exception {
    var schema = SchemaBuilder.record("names").fields().requiredInt("id").optionalString("name")
        .requiredString("needle").endRecord();
    var conf = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(directory.resolve("names.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, conf)).withSchema(schema)
        .withConf(conf).build()) {
      String[] names = {
          "ax", "xb", "bxx", "", null
      };
      for (int i = 0; i < names.length; i++) {
        var record = new GenericData.Record(schema);
        record.put("id", i + 1);
        record.put("name", names[i]);
        record.put("needle", "x");
        writer.write(record);
      }
    }
  }

  /**
   * Keeps both POSITION operands available even when only id is projected.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void filtersWithPosition(boolean predicate, boolean projection) throws Exception {
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String expression : List.of("POSITION('x' IN name)", "POSITION(needle IN name)",
          "POSITION(n.needle IN n.name)", "POSITION('x' IN LOWER(n.name))", "LOCATE('x', name)")) {
        String query = "SELECT id FROM names n WHERE " + expression + " = 2 ORDER BY id";
        try (var result = statement.executeQuery(query)) {
          List<Integer> ids = new ArrayList<>();
          while (result.next()) {
            ids.add(result.getInt(1));
          }
          assertEquals(List.of(1, 3), ids, query);
        }
      }
    }
  }
}
