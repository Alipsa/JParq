package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * {@link RecordReader} decorator that renames every record to an equivalent
 * schema by position. Field order and types must match the delegate schema;
 * values are copied positionally so the renamed records expose the target
 * schema's field names (and SQL lookup metadata) while the underlying read
 * stays lazy and closeable through the delegate.
 */
public final class RenamingRecordReader implements RecordReader {

  private final RecordReader delegate;
  private final Schema renamedSchema;
  private final int fieldCount;

  /**
   * Creates a reader exposing the delegate's records under the renamed schema.
   *
   * @param delegate
   *          source reader producing records positionally compatible with the
   *          renamed schema
   * @param renamedSchema
   *          schema with the same field order and types as the delegate's records
   */
  public RenamingRecordReader(RecordReader delegate, Schema renamedSchema) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.renamedSchema = Objects.requireNonNull(renamedSchema, "renamedSchema");
    this.fieldCount = renamedSchema.getFields().size();
  }

  @Override
  public GenericRecord read() throws IOException {
    GenericRecord record = delegate.read();
    if (record == null) {
      return null;
    }
    GenericRecord renamed = new GenericData.Record(renamedSchema);
    for (int i = 0; i < fieldCount; i++) {
      renamed.put(i, record.get(i));
    }
    return renamed;
  }

  @Override
  public Schema schema() {
    return renamedSchema;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
