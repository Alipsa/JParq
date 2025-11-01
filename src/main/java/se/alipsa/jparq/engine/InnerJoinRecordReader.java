package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * In-memory implementation of an INNER JOIN using a simple Cartesian product
 * followed by evaluation of the query {@code WHERE} clause. Each table is fully
 * materialized in memory which keeps the implementation straightforward and is
 * sufficient for the test scenarios shipped with the project.
 */
public final class InnerJoinRecordReader implements RecordReader {

  private final Schema schema;
  private final List<List<GenericRecord>> tableRows;
  private final List<FieldMapping> fieldMappings;
  private final int[] indices;
  private final List<String> columnNames;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;
  private boolean hasNext;

  /**
   * Representation of a table participating in the join.
   *
   * @param tableName
   *          the referenced table name
   * @param alias
   *          alias assigned to the table (may be {@code null})
   * @param schema
   *          schema describing the table
   * @param rows
   *          all rows read from the table
   */
  public record JoinTable(String tableName, String alias, Schema schema, List<GenericRecord> rows) {

    public JoinTable {
      Objects.requireNonNull(tableName, "tableName");
      Objects.requireNonNull(schema, "schema");
      Objects.requireNonNull(rows, "rows");
    }
  }

  private record FieldMapping(int tableIndex, String sourceField, String targetField) {
  }

  /**
   * Create a new reader capable of iterating over the Cartesian product of the
   * supplied tables.
   *
   * @param tables
   *          tables participating in the INNER JOIN
   */
  public InnerJoinRecordReader(List<JoinTable> tables) {
    Objects.requireNonNull(tables, "tables");
    if (tables.isEmpty()) {
      throw new IllegalArgumentException("At least one table is required for a join");
    }
    this.fieldMappings = new ArrayList<>();
    SchemaContext context = buildSchema(tables, fieldMappings);
    this.schema = context.schema();
    this.columnNames = buildColumnNames(fieldMappings);
    this.qualifierColumnMapping = context.qualifierMapping();
    this.unqualifiedColumnMapping = context.unqualifiedMapping();
    this.tableRows = new ArrayList<>(tables.size());
    for (JoinTable table : tables) {
      tableRows.add(new ArrayList<>(table.rows()));
    }
    this.indices = new int[tables.size()];
    this.hasNext = tableRows.stream().allMatch(list -> !list.isEmpty());
  }

  private static SchemaContext buildSchema(List<JoinTable> tables, List<FieldMapping> mappings) {
    List<Schema.Field> fields = new ArrayList<>();
    Set<String> usedNames = new HashSet<>();
    Map<String, Integer> columnCounts = new HashMap<>();
    for (JoinTable table : tables) {
      for (Schema.Field field : table.schema().getFields()) {
        columnCounts.merge(field.name(), 1, Integer::sum);
      }
    }
    Map<String, Map<String, String>> qualifierMap = new LinkedHashMap<>();
    Map<String, String> unqualifiedMap = new LinkedHashMap<>();
    for (int tableIndex = 0; tableIndex < tables.size(); tableIndex++) {
      Schema tableSchema = tables.get(tableIndex).schema();
      String qualifierPrefix = qualifierPrefix(tables.get(tableIndex), tableIndex);
      Set<String> qualifiers = qualifierSet(tables.get(tableIndex), tableIndex);
      for (Schema.Field field : tableSchema.getFields()) {
        String name = field.name();
        boolean duplicate = columnCounts.getOrDefault(name, 0) > 1;
        String canonical = duplicate ? qualifierPrefix + "__" + name : name;
        if (!usedNames.add(canonical)) {
          throw new IllegalArgumentException("Duplicate column name '" + canonical + "' in join schema");
        }
        Schema.Field newField = new Schema.Field(canonical, field.schema(), field.doc(), field.defaultVal());
        fields.add(newField);
        mappings.add(new FieldMapping(tableIndex, name, canonical));
        for (String qualifier : qualifiers) {
          qualifierMap.computeIfAbsent(normalize(qualifier), k -> new LinkedHashMap<>())
              .put(name.toLowerCase(Locale.ROOT), canonical);
        }
        if (!duplicate) {
          unqualifiedMap.put(name.toLowerCase(Locale.ROOT), canonical);
        }
      }
    }
    Schema joinSchema = Schema.createRecord("inner_join_record", null, null, false);
    joinSchema.setFields(fields);
    return new SchemaContext(joinSchema, qualifierMap, unqualifiedMap);
  }

  private static String qualifierPrefix(JoinTable table, int index) {
    if (table.alias() != null && !table.alias().isBlank()) {
      return table.alias();
    }
    if (table.tableName() != null && !table.tableName().isBlank()) {
      return table.tableName();
    }
    return "t" + index;
  }

  private static Set<String> qualifierSet(JoinTable table, int index) {
    Set<String> qualifiers = new LinkedHashSet<>();
    if (table.tableName() != null && !table.tableName().isBlank()) {
      qualifiers.add(table.tableName());
    }
    if (table.alias() != null && !table.alias().isBlank()) {
      qualifiers.add(table.alias());
    }
    if (qualifiers.isEmpty()) {
      qualifiers.add("t" + index);
    }
    return qualifiers;
  }

  private static String normalize(String qualifier) {
    return qualifier == null ? null : qualifier.toLowerCase(Locale.ROOT);
  }

  private record SchemaContext(Schema schema, Map<String, Map<String, String>> qualifierMapping,
      Map<String, String> unqualifiedMapping) {
  }

  private static List<String> buildColumnNames(List<FieldMapping> mappings) {
    List<String> names = new ArrayList<>(mappings.size());
    for (FieldMapping mapping : mappings) {
      names.add(mapping.targetField());
    }
    return List.copyOf(names);
  }

  /**
   * Retrieve the schema that describes the records produced by this reader.
   *
   * @return the combined schema
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Retrieve the column names in the order they appear in the combined schema.
   *
   * @return immutable list of column names
   */
  public List<String> columnNames() {
    return columnNames;
  }

  /**
   * Mapping from qualifier (table or alias) to canonical column names.
   *
   * @return immutable mapping used for expression resolution
   */
  public Map<String, Map<String, String>> qualifierColumnMapping() {
    Map<String, Map<String, String>> copy = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : qualifierColumnMapping.entrySet()) {
      copy.put(entry.getKey(), Map.copyOf(entry.getValue()));
    }
    return Map.copyOf(copy);
  }

  /**
   * Mapping of unqualified column names that remain unique across the join.
   *
   * @return immutable mapping for unqualified column resolution
   */
  public Map<String, String> unqualifiedColumnMapping() {
    return Map.copyOf(unqualifiedColumnMapping);
  }

  @Override
  public GenericRecord read() throws IOException {
    if (!hasNext) {
      return null;
    }
    GenericData.Record record = new GenericData.Record(schema);
    for (FieldMapping mapping : fieldMappings) {
      GenericRecord source = tableRows.get(mapping.tableIndex()).get(indices[mapping.tableIndex()]);
      record.put(mapping.targetField(), source.get(mapping.sourceField()));
    }
    advance();
    return record;
  }

  private void advance() {
    for (int i = indices.length - 1; i >= 0; i--) {
      int nextIndex = indices[i] + 1;
      if (nextIndex < tableRows.get(i).size()) {
        indices[i] = nextIndex;
        for (int j = i + 1; j < indices.length; j++) {
          indices[j] = 0;
        }
        return;
      }
    }
    hasNext = false;
  }

  @Override
  public void close() throws IOException {
    // nothing to release, data already materialised
  }
}
