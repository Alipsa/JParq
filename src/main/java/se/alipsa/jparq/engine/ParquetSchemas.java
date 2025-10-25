package se.alipsa.jparq.engine;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

/** Utility methods for reading Parquet schemas. */
public final class ParquetSchemas {

  private ParquetSchemas() {
  }

  /**
   * Reads the Avro schema from a Parquet file.
   *
   * @param path
   *          the Parquet file path
   * @param conf
   *          the Hadoop configuration
   * @return the Avro schema
   * @throws IOException
   *           if an I/O error occurs
   */
  public static Schema readAvroSchema(Path path, Configuration conf) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {

      var meta = reader.getFooter().getFileMetaData();
      var kv = meta.getKeyValueMetaData();

      // Try common keys used by parquet-avro writers
      String avroJson = kv.get("parquet.avro.schema");
      if (avroJson == null) {
        avroJson = kv.get("avro.schema");
      }
      if (avroJson != null && !avroJson.isEmpty()) {
        return new Schema.Parser().parse(avroJson);
      }

      // Fallback: derive Avro schema from the Parquet schema
      MessageType parquetSchema = meta.getSchema();
      return new AvroSchemaConverter().convert(parquetSchema);
    }
  }
}
