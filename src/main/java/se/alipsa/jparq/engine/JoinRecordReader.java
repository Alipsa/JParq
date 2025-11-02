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
 * In-memory implementation of explicit SQL joins using eager materialisation of
 * the combined result set.
 *
 * <p>
 * The reader supports {@code INNER}, {@code CROSS}, {@code LEFT OUTER},
 * {@code RIGHT OUTER}, and {@code FULL OUTER} joins. Rows from the preserved
 * side of an outer join are always emitted, even when the other side has no
 * matching row.
 * </p>
 */
public final class JoinRecordReader implements RecordReader {

  private final Schema schema;
  private final List<String> columnNames;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;
  private final List<JoinTable> joinTables;
  private final List<FieldMapping> fieldMappings;
  private final ExpressionEvaluator evaluator;
  private final List<GenericRecord> resultRows;
  private int resultIndex;
  private final int tableCount;

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

    /**
     * Retrieve all rows from the table.
     *
     * @return an immutable list of all rows
     */
    @Override
    public List<GenericRecord> rows() {
      return rows;
    }
  }

  /**
   * Describes how a field from an input table maps into the combined join record.
   *
   * @param tableIndex
   *          zero-based index of the table that owns the field
   * @param sourceField
   *          original name of the field in the source table
   * @param targetField
   *          canonical field name in the joined schema
   */
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
    List<FieldMapping> mappings = new ArrayList<>();
    SchemaContext context = buildSchema(joinTables, mappings);
    this.schema = context.schema();
    this.columnNames = buildColumnNames(mappings);
    this.qualifierColumnMapping = context.qualifierMapping();
    this.unqualifiedColumnMapping = context.unqualifiedMapping();
    this.joinTables = joinTables;
    this.fieldMappings = List.copyOf(mappings);
    this.evaluator = new ExpressionEvaluator(schema, null, List.of(), qualifierColumnMapping, unqualifiedColumnMapping);
    this.tableCount = joinTables.size();
    this.resultRows = computeResultRows();
    this.resultIndex = 0;
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
    Set<String> reservedTableQualifiers = new HashSet<>();
    Map<String, Integer> aliaslessTableCounts = new HashMap<>();
    for (int tableIndex = 0; tableIndex < tables.size(); tableIndex++) {
      JoinTable joinTable = tables.get(tableIndex);
      Schema tableSchema = joinTable.schema();
      String qualifierPrefix = qualifierPrefix(joinTable, tableIndex);
      Set<String> qualifiers = qualifierSet(joinTable, tableIndex);
      boolean aliasPresent = joinTable.alias() != null && !joinTable.alias().isBlank();
      String normalizedTableName = normalize(joinTable.tableName());
      boolean includeTableQualifier = true;
      if (normalizedTableName != null) {
        int aliaslessCount = aliaslessTableCounts.getOrDefault(normalizedTableName, 0);
        boolean duplicateTable = !reservedTableQualifiers.add(normalizedTableName);
        if (duplicateTable) {
          if (!aliasPresent && aliaslessCount >= 1) {
            throw new IllegalArgumentException("Multiple references to table '" + joinTable.tableName()
                + "' require aliases to disambiguate column references");
          }
          if (aliasPresent) {
            includeTableQualifier = false;
          }
        }
        if (!aliasPresent) {
          aliaslessTableCounts.put(normalizedTableName, aliaslessCount + 1);
        }
      }
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
        String lookupKey = name.toLowerCase(Locale.ROOT);
        for (String qualifier : qualifiers) {
          String normalizedQualifier = normalize(qualifier);
          if (normalizedQualifier == null) {
            continue;
          }
          if (!includeTableQualifier && normalizedQualifier.equals(normalizedTableName)) {
            continue;
          }
          qualifierMap.computeIfAbsent(normalizedQualifier, k -> new LinkedHashMap<>()).put(lookupKey, canonical);
        }
        if (!duplicate) {
          unqualifiedMap.put(lookupKey, canonical);
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

  private static GenericRecord buildRecord(List<GenericRecord> assignments, List<FieldMapping> mappings,
      Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    for (FieldMapping mapping : mappings) {
      GenericRecord source = mapping.tableIndex() < assignments.size() ? assignments.get(mapping.tableIndex()) : null;
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
    if (resultIndex < resultRows.size()) {
      GenericRecord record = resultRows.get(resultIndex);
      resultIndex++;
      return record;
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    // nothing to release, all data is sourced from in-memory tables
  }

  private List<GenericRecord> computeResultRows() {
    List<List<GenericRecord>> combinations = initialiseCombinations();
    for (int index = 1; index < joinTables.size(); index++) {
      JoinTable table = joinTables.get(index);
      if (table.joinType() == SqlParser.JoinType.RIGHT_OUTER) {
        combinations = combineRightOuter(combinations, table, index);
      } else if (table.joinType() == SqlParser.JoinType.FULL_OUTER) {
        combinations = combineFullOuter(combinations, table, index);
      } else {
        combinations = combineStandard(combinations, table, index);
      }
    }
    if (joinTables.isEmpty()) {
      return List.of();
    }
    List<GenericRecord> results = new ArrayList<>(combinations.size());
    for (List<GenericRecord> assignment : combinations) {
      results.add(buildRecord(assignment, fieldMappings, schema));
    }
    return List.copyOf(results);
  }

  private List<List<GenericRecord>> initialiseCombinations() {
    if (joinTables.isEmpty()) {
      return List.of();
    }
    JoinTable base = joinTables.get(0);
    List<List<GenericRecord>> combinations = new ArrayList<>();
    if (base.rows().isEmpty()) {
      return combinations;
    }
    for (GenericRecord row : base.rows()) {
      List<GenericRecord> assignment = emptyAssignment();
      assignment.set(0, row);
      combinations.add(assignment);
    }
    return combinations;
  }

  private List<GenericRecord> emptyAssignment() {
    List<GenericRecord> assignment = new ArrayList<>(tableCount);
    for (int i = 0; i < tableCount; i++) {
      assignment.add(null);
    }
    return assignment;
  }

  private List<List<GenericRecord>> combineStandard(List<List<GenericRecord>> leftCombos, JoinTable table, int index) {
    List<List<GenericRecord>> results = new ArrayList<>();
    if (leftCombos.isEmpty()) {
      return results;
    }
    boolean cross = table.joinType() == SqlParser.JoinType.CROSS;
    for (List<GenericRecord> combo : leftCombos) {
      boolean matched = false;
      if (cross) {
        for (GenericRecord row : table.rows()) {
          List<GenericRecord> assignment = new ArrayList<>(combo);
          assignment.set(index, row);
          results.add(assignment);
        }
        continue;
      }
      for (GenericRecord row : table.rows()) {
        List<GenericRecord> assignment = new ArrayList<>(combo);
        assignment.set(index, row);
        if (conditionMatches(table.joinCondition(), assignment)) {
          results.add(assignment);
          matched = true;
        }
      }
      if (!matched && table.joinType() == SqlParser.JoinType.LEFT_OUTER) {
        List<GenericRecord> assignment = new ArrayList<>(combo);
        assignment.set(index, null);
        results.add(assignment);
      }
    }
    return results;
  }

  private List<List<GenericRecord>> combineRightOuter(List<List<GenericRecord>> leftCombos, JoinTable table,
      int index) {
    List<List<GenericRecord>> results = new ArrayList<>();
    if (table.rows().isEmpty()) {
      return results;
    }
    if (leftCombos.isEmpty()) {
      for (GenericRecord row : table.rows()) {
        List<GenericRecord> assignment = emptyAssignment();
        assignment.set(index, row);
        results.add(assignment);
      }
      return results;
    }
    for (GenericRecord row : table.rows()) {
      boolean matched = false;
      for (List<GenericRecord> combo : leftCombos) {
        List<GenericRecord> assignment = new ArrayList<>(combo);
        assignment.set(index, row);
        if (conditionMatches(table.joinCondition(), assignment)) {
          results.add(assignment);
          matched = true;
        }
      }
      if (!matched) {
        List<GenericRecord> assignment = emptyAssignment();
        assignment.set(index, row);
        results.add(assignment);
      }
    }
    return results;
  }

  /**
   * Combine the accumulated left-hand assignments with the rows from the current
   * table when performing a {@code FULL [OUTER] JOIN}.
   *
   * @param leftCombos
   *          assignments produced by the previous join stages
   * @param table
   *          table participating in the join at the supplied {@code index}
   * @param index
   *          zero-based index of the table within the join order
   * @return a list containing the expanded assignments representing the full
   *         outer join result
   */
  private List<List<GenericRecord>> combineFullOuter(List<List<GenericRecord>> leftCombos, JoinTable table, int index) {
    List<List<GenericRecord>> results = new ArrayList<>();
    List<GenericRecord> rightRows = table.rows();
    if (leftCombos.isEmpty() && rightRows.isEmpty()) {
      return results;
    }
    if (leftCombos.isEmpty()) {
      for (GenericRecord row : rightRows) {
        List<GenericRecord> assignment = emptyAssignment();
        assignment.set(index, row);
        results.add(assignment);
      }
      return results;
    }
    if (rightRows.isEmpty()) {
      for (List<GenericRecord> combo : leftCombos) {
        List<GenericRecord> assignment = new ArrayList<>(combo);
        assignment.set(index, null);
        results.add(assignment);
      }
      return results;
    }
    boolean[] leftMatched = new boolean[leftCombos.size()];
    boolean[] rightMatched = new boolean[rightRows.size()];
    for (int leftIndex = 0; leftIndex < leftCombos.size(); leftIndex++) {
      List<GenericRecord> combo = leftCombos.get(leftIndex);
      for (int rightIndex = 0; rightIndex < rightRows.size(); rightIndex++) {
        List<GenericRecord> assignment = new ArrayList<>(combo);
        assignment.set(index, rightRows.get(rightIndex));
        if (conditionMatches(table.joinCondition(), assignment)) {
          results.add(assignment);
          leftMatched[leftIndex] = true;
          rightMatched[rightIndex] = true;
        }
      }
    }
    for (int leftIndex = 0; leftIndex < leftCombos.size(); leftIndex++) {
      if (!leftMatched[leftIndex]) {
        List<GenericRecord> assignment = new ArrayList<>(leftCombos.get(leftIndex));
        assignment.set(index, null);
        results.add(assignment);
      }
    }
    for (int rightIndex = 0; rightIndex < rightRows.size(); rightIndex++) {
      if (!rightMatched[rightIndex]) {
        List<GenericRecord> assignment = emptyAssignment();
        assignment.set(index, rightRows.get(rightIndex));
        results.add(assignment);
      }
    }
    return results;
  }

  private boolean conditionMatches(Expression condition, List<GenericRecord> assignments) {
    if (condition == null) {
      return true;
    }
    GenericRecord record = buildRecord(assignments, fieldMappings, schema);
    return evaluator.eval(condition, record);
  }
}
