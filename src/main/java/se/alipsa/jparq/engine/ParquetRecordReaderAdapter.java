package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Adapts a {@link ParquetReader} to the {@link RecordReader} abstraction.
 * Records are copied only when their projected field schemas need enrichment.
 */
public final class ParquetRecordReaderAdapter implements RecordReader {

  private final ParquetReader<GenericRecord> delegate;
  private final Schema fileSchema;
  private Schema projectedSchema;
  private boolean copyRequired;

  /**
   * Create a new adapter for the supplied {@link ParquetReader}.
   *
   * @param delegate
   *          the reader to adapt
   */
  public ParquetRecordReaderAdapter(ParquetReader<GenericRecord> delegate) {
    this(delegate, null);
  }

  /**
   * Creates a reader preserving logical annotations missing from parquet-avro.
   * 
   * @param delegate
   *          the Parquet reader
   * @param fileSchema
   *          the enriched full file schema, or null
   */
  public ParquetRecordReaderAdapter(ParquetReader<GenericRecord> delegate, Schema fileSchema) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.fileSchema = fileSchema;
  }

  @Override
  public GenericRecord read() throws IOException {
    GenericRecord raw = delegate.read();
    if (raw == null || fileSchema == null) {
      return raw;
    }
    if (projectedSchema == null) {
      var names = new LinkedHashSet<String>();
      for (var field : raw.getSchema().getFields()) {
        names.add(field.name());
      }
      projectedSchema = AvroProjections.project(fileSchema, names);
      for (var field : projectedSchema.getFields()) {
        if (!field.schema().equals(raw.getSchema().getField(field.name()).schema())) {
          copyRequired = true;
          break;
        }
      }
    }
    if (!copyRequired) {
      return raw;
    }
    GenericRecord record = new GenericData.Record(projectedSchema);
    for (var field : projectedSchema.getFields()) {
      record.put(field.name(), raw.get(field.name()));
    }
    return record;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
