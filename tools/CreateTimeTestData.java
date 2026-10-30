import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/** Regenerates the nullable TIME fixture used by ParquetTimeTest. */
public class CreateTimeTestData {

  /**
   * Writes raw millisecond and microsecond clock values in deliberately unsorted order.
   *
   * @param args unused command-line arguments
   * @throws Exception if the fixture cannot be written
   */
  public static void main(String[] args) throws Exception {
    Path target = Path.of("src/test/resources/time/typed.parquet");
    Files.createDirectories(target.getParent());
    Schema schema = SchemaBuilder.record("typed").fields().requiredInt("id")
        .name("t").type(Schema.createUnion(Schema.create(Schema.Type.NULL),
            LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)))).noDefault()
        .name("u").type(Schema.createUnion(Schema.create(Schema.Type.NULL),
            LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)))).noDefault()
        .endRecord();
    Configuration configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(target.toUri()), configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema)
        .withConf(configuration).withWriteMode(ParquetFileWriter.Mode.OVERWRITE).build()) {
      for (int id : new int[]{2, 1, 3}) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("t", id == 3 ? null : (9 + id) * 3_600_000);
        record.put("u", id == 3 ? null : (9 + id) * 3_600_000_000L);
        writer.write(record);
      }
    }
    Files.deleteIfExists(target.resolveSibling(".typed.parquet.crc"));
  }
}
