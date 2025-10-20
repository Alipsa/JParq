package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

public final class ParquetSchemas {
  private ParquetSchemas() {
  }

  /**
   * Return a record schema that is a subset of fileSchema with only the given
   * columns.
   */
  public static Schema requestedSubset(Schema fileSchema, Set<String> columnsOrNull) {
    if (fileSchema == null || columnsOrNull == null) {
      return null;
    }

    Set<String> want = new HashSet<>(columnsOrNull);
    List<Schema.Field> kept = new ArrayList<>();
    for (Schema.Field f : fileSchema.getFields()) {
      if (want.contains(f.name())) {
        // Keep the same field (name, schema, doc, default). In Avro 1.16 this ctor is
        // fine.
        kept.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
      }
    }
    if (kept.isEmpty()) {
      return null;
    }

    Schema subset = Schema.createRecord(fileSchema.getName(), fileSchema.getDoc(), fileSchema.getNamespace(), false);
    subset.setFields(kept);
    return subset;
  }

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
