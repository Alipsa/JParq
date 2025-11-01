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
import net.sf.jsqlparser.expression.Expression;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * In-memory implementation of explicit SQL joins using a simple materialised
 * representation of each participating table.
 *
 * <p>
 * The reader currently supports {@code INNER}, {@code CROSS}, and {@code LEFT
 * OUTER} joins. Rows from the left side of a {@code LEFT JOIN} are always
 * emitted, even when no matching row exists on the right side.
 * </p>
 */
public final class JoinRecordReader implements RecordReader {

  private final Schema schema;
  private final List<String> columnNames;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;
  private final List<GenericRecord> joinedRows;
  private int position = 0;

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
   * @param joinType
   *          type of join introducing the table (BASE for the first table)
   * @param joinCondition
   *          condition associated with the join (may be {@code null})
   */
  public record JoinTable(String tableName, String alias, Schema schema, List<GenericRecord> rows,
      SqlParser.JoinType joinType, Expression joinCondition) {

    /**
     * Validates mandatory state for the join table to guard against null elements.
     */
    public JoinTable {
      Objects.requireNonNull(tableName, "tableName");
      Objects.requireNonNull(schema, "schema");
      Objects.requireNonNull(rows, "rows");
      Objects.requireNonNull(joinType, "joinType");
      if (joinType == SqlParser.JoinType.BASE && joinCondition != null) {
        throw new IllegalArgumentException("The base table cannot specify a join condition");
      }
      rows = List.copyOf(rows);
    }

    @Override
    public List<GenericRecord> rows() {
      return rows;
    }
  }

  private record FieldMapping(int tableIndex, String sourceField, String targetField) {
  }

  private record SchemaContext(Schema schema, Map<String, Map<String, String>> qualifierMapping,
      Map<String, String> unqualifiedMapping) {
  }

  /**
   * Create a new reader capable of iterating over the joined result set.
   *
   * @param tables
   *          tables participating in the join, in evaluation order
   */
  public JoinRecordReader(List<JoinTable> tables) {
    Objects.requireNonNull(tables, "tables");
    if (tables.isEmpty()) {
      throw new IllegalArgumentException("At least one table is required for a join");
    }
    List<JoinTable> joinTables = List.copyOf(tables);
    if (joinTables.get(0).joinType() != SqlParser.JoinType.BASE) {
      throw new IllegalArgumentException("The first table must be marked as BASE");
    }
    List<FieldMapping> fieldMappings = new ArrayList<>();
    SchemaContext context = buildSchema(joinTables, fieldMappings);
    this.schema = context.schema();
    this.columnNames = buildColumnNames(fieldMappings);
    this.qualifierColumnMapping = context.qualifierMapping();
    this.unqualifiedColumnMapping = context.unqualifiedMapping();
    ExpressionEvaluator evaluator = new ExpressionEvaluator(schema, null, List.of(), qualifierColumnMapping,
        unqualifiedColumnMapping);
    this.joinedRows = buildJoinedRows(joinTables, fieldMappings, schema, evaluator);
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
    Schema joinSchema = Schema.createRecord("join_record", null, null, false);
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

  private static List<String> buildColumnNames(List<FieldMapping> mappings) {
    List<String> names = new ArrayList<>(mappings.size());
    for (FieldMapping mapping : mappings) {
      names.add(mapping.targetField());
    }
    return List.copyOf(names);
  }

  private static List<GenericRecord> buildJoinedRows(List<JoinTable> tables, List<FieldMapping> mappings, Schema schema,
      ExpressionEvaluator evaluator) {
    List<GenericRecord> results = new ArrayList<>();
    if (tables.isEmpty()) {
      return results;
    }
    List<GenericRecord> assignments = new ArrayList<>();
    joinRecursive(tables, mappings, schema, evaluator, 0, assignments, results);
    return results;
  }

  private static void joinRecursive(List<JoinTable> tables, List<FieldMapping> mappings, Schema schema,
      ExpressionEvaluator evaluator, int index, List<GenericRecord> assignments, List<GenericRecord> results) {
    JoinTable current = tables.get(index);
    if (index == 0) {
      for (GenericRecord row : current.rows()) {
        assignments.add(row);
        if (tables.size() == 1) {
          results.add(buildRecord(assignments, mappings, schema));
        } else {
          joinRecursive(tables, mappings, schema, evaluator, index + 1, assignments, results);
        }
        assignments.remove(assignments.size() - 1);
      }
      return;
    }

    boolean matched = false;
    for (GenericRecord row : current.rows()) {
      assignments.add(row);
      if (conditionMatches(current.joinCondition(), evaluator, assignments, mappings, schema)) {
        matched = true;
        if (index == tables.size() - 1) {
          results.add(buildRecord(assignments, mappings, schema));
        } else {
          joinRecursive(tables, mappings, schema, evaluator, index + 1, assignments, results);
        }
      }
      assignments.remove(assignments.size() - 1);
    }

    if (!matched && current.joinType() == SqlParser.JoinType.LEFT_OUTER) {
      assignments.add(null);
      if (index == tables.size() - 1) {
        results.add(buildRecord(assignments, mappings, schema));
      } else {
        joinRecursive(tables, mappings, schema, evaluator, index + 1, assignments, results);
      }
      assignments.remove(assignments.size() - 1);
    }
  }

  private static boolean conditionMatches(Expression condition, ExpressionEvaluator evaluator,
      List<GenericRecord> assignments, List<FieldMapping> mappings, Schema schema) {
    if (condition == null) {
      return true;
    }
    GenericRecord record = buildRecord(assignments, mappings, schema);
    return evaluator.eval(condition, record);
  }

  private static GenericRecord buildRecord(List<GenericRecord> assignments,
      List<FieldMapping> mappings, Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    for (FieldMapping mapping : mappings) {
      GenericRecord source = mapping.tableIndex() < assignments.size()
          ? assignments.get(mapping.tableIndex())
          : null;
      Object value = source == null ? null : source.get(mapping.sourceField());
      record.put(mapping.targetField(), value);
    }
    return record;
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
    if (position >= joinedRows.size()) {
      return null;
    }
    GenericRecord record = joinedRows.get(position);
    position++;
    return record;
  }

  @Override
  public void close() throws IOException {
    // nothing to release, data already materialised
  }
}
