package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.helper.JParqUtil;

/**
 * {@link RecordReader} implementation backed by an in-memory list of
 * {@link GenericRecord} instances. The reader iterates over the supplied rows
 * without performing any additional I/O which makes it suitable for temporary
 * data sets such as common table expression results.
 */
public final class InMemoryRecordReader implements RecordReader, ColumnMappingProvider {

  private final List<GenericRecord> records;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;
  private int index;

  /**
   * Create a new reader that iterates over the provided records.
   *
   * @param records
   *          the records to expose through the reader
   */
  public InMemoryRecordReader(List<GenericRecord> records) {
    this(records, null, null);
  }

  /**
   * Create a new reader that iterates over the provided records with optional
   * qualifier-aware column mappings.
   *
   * @param records
   *          the records to expose through the reader
   * @param schema
   *          schema describing the records; when {@code null}, the schema is
   *          derived from the first record if available
   * @param qualifier
   *          optional qualifier used to scope column mappings for correlation
   */
  public InMemoryRecordReader(List<GenericRecord> records, Schema schema, String qualifier) {
    this.records = List.copyOf(Objects.requireNonNull(records, "records"));
    this.index = 0;
    Schema effectiveSchema = schema;
    if (effectiveSchema == null && !records.isEmpty() && records.get(0) != null) {
      effectiveSchema = records.get(0).getSchema();
    }
    Map<String, String> columnMap = buildColumnMapping(effectiveSchema);
    unqualifiedColumnMapping = columnMap.isEmpty() ? Map.of() : Map.copyOf(columnMap);

    String normalizedQualifier = JParqUtil.normalizeQualifier(qualifier);
    if (normalizedQualifier != null && !unqualifiedColumnMapping.isEmpty()) {
      qualifierColumnMapping = Map.of(normalizedQualifier, unqualifiedColumnMapping);
    } else {
      qualifierColumnMapping = Map.of();
    }
  }

  @Override
  public GenericRecord read() {
    if (index >= records.size()) {
      return null;
    }
    GenericRecord record = records.get(index);
    index++;
    return record;
  }

  @Override
  public void close() throws IOException {
    // No resources to release for in-memory data.
  }

  @Override
  public Map<String, Map<String, String>> qualifierColumnMapping() {
    return qualifierColumnMapping;
  }

  @Override
  public Map<String, String> unqualifiedColumnMapping() {
    return unqualifiedColumnMapping;
  }

  private static Map<String, String> buildColumnMapping(Schema schema) {
    if (schema == null || schema.getFields() == null) {
      return Map.of();
    }
    Map<String, String> mapping = new LinkedHashMap<>();
    for (Schema.Field field : schema.getFields()) {
      if (field == null || field.name() == null) {
        continue;
      }
      String normalized = field.name().toLowerCase(Locale.ROOT);
      mapping.putIfAbsent(normalized, field.name());
    }
    return mapping;
  }
}
