package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Adapts a {@link ParquetReader} to the {@link RecordReader} abstraction.
 * Records are copied, including nested values, only when their projected field
 * schemas need enrichment.
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

  /**
   * Returns the enriched full file schema when available.
   *
   * @return the file schema, or {@code null} when the adapter was created without
   *         one
   */
  @Override
  public Schema schema() {
    return fileSchema;
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
    // Rebind nested records as well as the outer row so consumers see restored
    // logical annotations.
    return (GenericRecord) enrich(raw, raw.getSchema(), projectedSchema);
  }

  /**
   * Rebinds container schemas while retaining raw scalar representations for JDBC
   * decoding.
   *
   * @param value
   *          raw field value
   * @param source
   *          reader declaration
   * @param target
   *          declaration with restored annotations
   * @return enriched container or the unchanged scalar value
   */
  private static Object enrich(Object value, Schema source, Schema target) {
    if (value == null || source.equals(target)) {
      return value;
    }
    Schema from = AvroCoercions.effectiveSchema(source);
    Schema to = AvroCoercions.effectiveSchema(target);
    if (value instanceof IndexedRecord record && to.getType() == Schema.Type.RECORD) {
      var result = new GenericData.Record(to);
      for (Schema.Field field : to.getFields()) {
        Schema.Field original = from.getField(field.name());
        result.put(field.pos(), enrich(record.get(original.pos()), original.schema(), field.schema()));
      }
      return result;
    }
    if (to.getType() == Schema.Type.ARRAY) {
      Collection<?> values = value instanceof Object[] array ? Arrays.asList(array) : (Collection<?>) value;
      var result = new ArrayList<Object>(values.size());
      for (Object item : values) {
        result.add(enrich(item, from.getElementType(), to.getElementType()));
      }
      return result;
    }
    if (to.getType() == Schema.Type.MAP) {
      var result = new LinkedHashMap<Object, Object>();
      for (var entry : ((Map<?, ?>) value).entrySet()) {
        result.put(entry.getKey(), enrich(entry.getValue(), from.getValueType(), to.getValueType()));
      }
      return result;
    }
    return value;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
