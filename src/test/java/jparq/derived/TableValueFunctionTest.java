package jparq.derived;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests verifying support for {@code TABLE()} wrapped table value
 * functions.
 */
class TableValueFunctionTest {

  private static Path tempDirectory;
  private static JParqSql jparqSql;
  private static Schema vehicleSchema;

  /**
   * Create a temporary dataset with an array column used for {@code UNNEST}
   * evaluation.
   *
   * @throws IOException
   *           if the test data cannot be created
   */
  @BeforeAll
  static void setUp() throws IOException {
    tempDirectory = Files.createTempDirectory("jparq-table-function-test");
    vehicleSchema = buildVehicleSchema();
    writeVehicles();
    jparqSql = new JParqSql("jdbc:jparq:" + tempDirectory.toAbsolutePath());
  }

  /**
   * Remove all files created for the test dataset.
   *
   * @throws IOException
   *           if removing the files fails
   */
  @AfterAll
  static void tearDown() throws IOException {
    if (tempDirectory == null) {
      return;
    }
    try (Stream<Path> paths = Files.walk(tempDirectory)) {
      paths.sorted((left, right) -> right.compareTo(left)).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          // Ignore cleanup failures to avoid masking test results.
        }
      });
    }
  }

  /**
   * Verify that {@code TABLE(UNNEST(...))} can be parsed and executed through the
   * LATERAL pipeline.
   */
  @Test
  void tableWrapperUnnestLateral() {
    String sql = """
        SELECT
            v.model,
            g.allowed_gear
        FROM
            vehicles v
            JOIN LATERAL TABLE(UNNEST(v.gear_ranges)) AS g(allowed_gear)
                ON TRUE
        WHERE
            g.allowed_gear > 4
        ORDER BY
            v.model,
            g.allowed_gear
        """;

    List<String> results = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          results.add(rs.getString("model") + ":" + rs.getInt("allowed_gear"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to read TABLE(UNNEST()) results", e);
      }
    });

    Assertions.assertEquals(List.of("Coupe:5", "Sedan:5"), results);
  }

  /**
   * Verify that {@code TABLE(UNNEST(...))} works when driven solely by a scalar
   * array constructor.
   */
  @Test
  void tableWrapperUnnestScalarArray() {
    String sql = """
        SELECT
            numbers.val AS value_column
        FROM
            TABLE(UNNEST(ARRAY[1, 2, 3])) AS numbers(val)
        ORDER BY
            value_column
        """;

    List<Integer> values = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          values.add(rs.getInt("value_column"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to read scalar TABLE(UNNEST()) results", e);
      }
    });

    Assertions.assertEquals(List.of(1, 2, 3), values);
  }

  private static Schema buildVehicleSchema() {
    return SchemaBuilder.record("vehicle").namespace("jparq.derived").fields().requiredString("model")
        .name("gear_ranges").type().array().items().intType().noDefault().endRecord();
  }

  private static void writeVehicles() throws IOException {
    var outputPath = new org.apache.hadoop.fs.Path(tempDirectory.resolve("vehicles.parquet").toUri());
    Configuration conf = new Configuration(false);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(outputPath, conf))
        .withSchema(vehicleSchema).withConf(conf).withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      writer.write(buildVehicle("Coupe", List.of(3, 4, 5)));
      writer.write(buildVehicle("Sedan", List.of(4, 5)));
      writer.write(buildVehicle("Truck", List.of(3, 4)));
    }
  }

  private static GenericRecord buildVehicle(String model, List<Integer> gearRanges) {
    GenericData.Record record = new GenericData.Record(vehicleSchema);
    record.put("model", model);
    record.put("gear_ranges", buildIntArray(vehicleSchema.getField("gear_ranges").schema(), gearRanges));
    return record;
  }

  private static GenericData.Array<Integer> buildIntArray(Schema arraySchema, List<Integer> values) {
    GenericData.Array<Integer> array = new GenericData.Array<>(values.size(), arraySchema);
    for (Integer value : values) {
      array.add(value);
    }
    return array;
  }
}
