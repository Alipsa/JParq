package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * {@link RecordReader} decorator that renames every record produced by the
 * delegate to a positionally equivalent schema. Each renamed field sources its
 * value from the delegate field captured by name at construction, so records
 * whose schema is a reordered or narrowed projection of the source schema are
 * handled correctly; source fields absent from a projected record leave the
 * renamed field null and are never read because projection only drops columns
 * the query does not reference.
 */
public final class RenamingRecordReader implements RecordReader, ColumnMappingProvider {

  private final RecordReader delegate;
  private final Schema renamedSchema;
  private final List<String> sourceNames;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;

  /**
   * Creates a reader exposing the delegate's records under the renamed schema.
   *
   * @param delegate
   *          source reader producing records compatible with the source schema
   * @param sourceSchema
   *          schema describing the delegate's records by physical field name;
   *          must have the same field count and order as the renamed schema
   * @param renamedSchema
   *          schema exposing the same columns in the same order under new names
   */
  public RenamingRecordReader(RecordReader delegate, Schema sourceSchema, Schema renamedSchema) {
    this(delegate, sourceSchema, renamedSchema, null);
  }

  /**
   * Creates a reader exposing the delegate's records under the renamed schema,
   * with an optional qualifier for correlation mappings.
   *
   * @param delegate
   *          source reader producing records compatible with the source schema
   * @param sourceSchema
   *          schema describing the delegate's records by physical field name;
   *          must have the same field count and order as the renamed schema
   * @param renamedSchema
   *          schema exposing the same columns in the same order under new names
   * @param qualifier
   *          optional table name or alias scoping the exposed column mapping
   */
  public RenamingRecordReader(RecordReader delegate, Schema sourceSchema, Schema renamedSchema, String qualifier) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.renamedSchema = Objects.requireNonNull(renamedSchema, "renamedSchema");
    Objects.requireNonNull(sourceSchema, "sourceSchema");
    List<Schema.Field> sourceFields = sourceSchema.getFields();
    if (sourceFields.size() != renamedSchema.getFields().size()) {
      throw new IllegalArgumentException("Source schema has " + sourceFields.size() + " fields but renamed schema has "
          + renamedSchema.getFields().size());
    }
    List<String> names = new ArrayList<>(sourceFields.size());
    for (Schema.Field field : sourceFields) {
      names.add(field.name());
    }
    this.sourceNames = List.copyOf(names);
    unqualifiedColumnMapping = ColumnMappingUtil.unqualifiedColumnMapping(renamedSchema);
    qualifierColumnMapping = ColumnMappingUtil.qualifierColumnMapping(unqualifiedColumnMapping, qualifier);
  }

  @Override
  public GenericRecord read() throws IOException {
    GenericRecord record = delegate.read();
    if (record == null) {
      return null;
    }
    Schema recordSchema = record.getSchema();
    GenericRecord renamed = new GenericData.Record(renamedSchema);
    for (int i = 0; i < sourceNames.size(); i++) {
      if (recordSchema.getField(sourceNames.get(i)) != null) {
        renamed.put(i, record.get(sourceNames.get(i)));
      }
    }
    return renamed;
  }

  @Override
  public Schema schema() {
    return renamedSchema;
  }

  @Override
  public Map<String, Map<String, String>> qualifierColumnMapping() {
    return qualifierColumnMapping;
  }

  @Override
  public Map<String, String> unqualifiedColumnMapping() {
    return unqualifiedColumnMapping;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
