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
public final class JoinRecordReader implements RecordReader, ColumnMappingProvider {

  private final Schema schema;
  private final List<String> columnNames;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;
  private final Map<String, String> canonicalLabels;
  private final List<JoinTable> joinTables;
  private final List<FieldMapping> fieldMappings;
  private final ExpressionEvaluator evaluator;
  private final List<GenericRecord> resultRows;
  private int resultIndex;
  private final int tableCount;
  private final UsingMetadata usingMetadata;

  /**
   * Produces rows for correlated table functions based on the current join
   * assignments.
   */
  @FunctionalInterface
  public interface CorrelatedRowsSupplier {

    /**
     * Produce the rows contributed by a correlated table function for the supplied
     * assignments.
     *
     * @param assignments
     *          the current join assignments representing the left-hand side
     * @return rows emitted by the correlated table function (never {@code null})
     */
    List<GenericRecord> rows(List<GenericRecord> assignments);
  }

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
   * @param usingColumns
   *          list of column names supplied via a {@code USING} clause for this
   *          table (empty when {@code USING} is not used)
   * @param correlatedRowsSupplier
   *          supplier invoked to produce rows when the table originates from a
   *          correlated table function (may be {@code null})
   */
  public record JoinTable(String tableName, String alias, Schema schema, List<GenericRecord> rows,
      SqlParser.JoinType joinType, Expression joinCondition, List<String> usingColumns,
      CorrelatedRowsSupplier correlatedRowsSupplier) {

    /**
     * Validates mandatory state for the join table to guard against null elements.
     */
    public JoinTable {
      Objects.requireNonNull(tableName, "tableName");
      Objects.requireNonNull(schema, "schema");
      Objects.requireNonNull(rows, "rows");
      Objects.requireNonNull(joinType, "joinType");
      usingColumns = usingColumns == null ? List.of() : List.copyOf(usingColumns);
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

    /**
     * Determine whether this join table produces rows through a correlated
     * evaluation strategy.
     *
     * @return {@code true} if the table depends on prior join assignments,
     *         otherwise {@code false}
     */
    public boolean isCorrelated() {
      return correlatedRowsSupplier != null;
    }

    /**
     * Resolve the rows that should participate in the join for the supplied
     * left-hand assignments.
     *
     * @param assignments
     *          the current left-hand assignments; may be empty when evaluating the
     *          base table
     * @return rows emitted by the table for the provided assignments
     */
    public List<GenericRecord> rowsFor(List<GenericRecord> assignments) {
      if (correlatedRowsSupplier == null) {
        return rows;
      }
      List<GenericRecord> evaluated = correlatedRowsSupplier.rows(assignments);
      if (evaluated == null || evaluated.isEmpty()) {
        return List.of();
      }
      return List.copyOf(evaluated);
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
  private record FieldMapping(int tableIndex, String sourceField, String targetField,
      List<AlternateSource> alternates) {

    FieldMapping {
      alternates = alternates == null ? List.of() : List.copyOf(alternates);
    }

    Object resolveValue(List<GenericRecord> assignments) {
      Object primary = read(assignments, tableIndex, sourceField);
      if (primary != null || hasRecord(assignments, tableIndex)) {
        return primary;
      }
      for (AlternateSource alternate : alternates) {
        Object candidate = read(assignments, alternate.tableIndex(), alternate.sourceField());
        if (candidate != null || hasRecord(assignments, alternate.tableIndex())) {
          return candidate;
        }
      }
      return null;
    }

    private static Object read(List<GenericRecord> assignments, int index, String field) {
      if (index < 0 || index >= assignments.size()) {
        return null;
      }
      GenericRecord record = assignments.get(index);
      if (record == null) {
        return null;
      }
      return record.get(field);
    }

    private static boolean hasRecord(List<GenericRecord> assignments, int index) {
      return index >= 0 && index < assignments.size() && assignments.get(index) != null;
    }
  }

  private record AlternateSource(int tableIndex, String sourceField) {
  }

  private static final class UsingFieldState {

    private Schema.Field field;
    private int ownerIndex = -1;
    private String ownerField;
    private final List<AlternateSource> alternates = new ArrayList<>();

    void registerField(Schema.Field candidate, boolean owner) {
      if (owner || field == null) {
        field = candidate;
      }
    }

    void setOwner(int index, String fieldName) {
      ownerIndex = index;
      ownerField = fieldName;
    }

    void addAlternate(int index, String fieldName) {
      alternates.add(new AlternateSource(index, fieldName));
    }

    Schema.Field field() {
      return field;
    }

    int ownerIndex() {
      return ownerIndex;
    }

    String ownerField() {
      return ownerField;
    }

    List<AlternateSource> alternates() {
      return List.copyOf(alternates);
    }
  }

  /**
   * Describes a {@code USING} column and the table that contributes the canonical
   * value retained in the join output.
   */
  private static final class UsingColumnInfo {

    private final String canonicalName;
    private final int ownerIndex;

    UsingColumnInfo(String canonicalName, int ownerIndex) {
      this.canonicalName = canonicalName;
      this.ownerIndex = ownerIndex;
    }

    String canonicalName() {
      return canonicalName;
    }

    int ownerIndex() {
      return ownerIndex;
    }

    boolean isOwner(int tableIndex) {
      return tableIndex == ownerIndex;
    }
  }

  /**
   * Aggregated metadata describing all {@code USING} columns across a join.
   */
  private record UsingMetadata(Map<Integer, Map<String, UsingColumnInfo>> lookup, List<String> order) {

    UsingColumnInfo lookup(int tableIndex, String fieldName) {
      if (lookup == null || lookup.isEmpty() || fieldName == null) {
        return null;
      }
      Map<String, UsingColumnInfo> tableMap = lookup.get(tableIndex);
      if (tableMap == null) {
        return null;
      }
      return tableMap.get(fieldName.toLowerCase(Locale.ROOT));
    }

    static UsingMetadata from(List<JoinTable> tables) {
      if (tables == null || tables.isEmpty()) {
        return new UsingMetadata(Map.of(), List.of());
      }
      Map<Integer, Map<String, UsingColumnInfo>> tableLookup = new HashMap<>();
      Map<String, UsingColumnInfo> byNormalized = new LinkedHashMap<>();
      List<String> order = new ArrayList<>();
      for (int tableIndex = 0; tableIndex < tables.size(); tableIndex++) {
        JoinTable table = tables.get(tableIndex);
        for (String usingColumn : table.usingColumns()) {
          if (usingColumn == null || usingColumn.isBlank()) {
            continue;
          }
          String normalized = usingColumn.toLowerCase(Locale.ROOT);
          UsingColumnInfo info = byNormalized.get(normalized);
          if (info == null) {
            int ownerIndex = locateOwnerTable(tables, tableIndex, usingColumn);
            if (ownerIndex < 0) {
              throw new IllegalArgumentException("USING column '" + usingColumn + "' not found in preceding tables");
            }
            String ownerField = resolveFieldName(tables.get(ownerIndex).schema(), usingColumn);
            if (ownerField == null) {
              throw new IllegalArgumentException(
                  "USING column '" + usingColumn + "' missing from table '" + tables.get(ownerIndex).tableName() + "'");
            }
            info = new UsingColumnInfo(ownerField, ownerIndex);
            byNormalized.put(normalized, info);
            order.add(ownerField);
            registerUsingField(tableLookup, ownerIndex, ownerField, info);
          }
          String participantField = resolveFieldName(table.schema(), usingColumn);
          if (participantField == null) {
            throw new IllegalArgumentException(
                "USING column '" + usingColumn + "' missing from table '" + table.tableName() + "'");
          }
          registerUsingField(tableLookup, tableIndex, participantField, info);
        }
      }
      Map<Integer, Map<String, UsingColumnInfo>> immutableLookup = new HashMap<>();
      for (Map.Entry<Integer, Map<String, UsingColumnInfo>> entry : tableLookup.entrySet()) {
        immutableLookup.put(entry.getKey(), Map.copyOf(entry.getValue()));
      }
      return new UsingMetadata(Map.copyOf(immutableLookup), List.copyOf(order));
    }

    private static int locateOwnerTable(List<JoinTable> tables, int currentIndex, String column) {
      for (int index = currentIndex - 1; index >= 0; index--) {
        JoinTable candidate = tables.get(index);
        if (resolveFieldName(candidate.schema(), column) != null) {
          return index;
        }
      }
      return -1;
    }

    private static void registerUsingField(Map<Integer, Map<String, UsingColumnInfo>> tableLookup, int tableIndex,
        String fieldName, UsingColumnInfo info) {
      tableLookup.computeIfAbsent(tableIndex, k -> new LinkedHashMap<>()).put(fieldName.toLowerCase(Locale.ROOT), info);
    }

    private static String resolveFieldName(Schema schema, String column) {
      if (schema == null || column == null) {
        return null;
      }
      Schema.Field direct = schema.getField(column);
      if (direct != null) {
        return direct.name();
      }
      for (Schema.Field field : schema.getFields()) {
        if (field.name().equalsIgnoreCase(column)) {
          return field.name();
        }
      }
      return null;
    }
  }

  private record SchemaContext(Schema schema, Map<String, Map<String, String>> qualifierMapping,
      Map<String, String> unqualifiedMapping, Map<String, String> canonicalLabels) {
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
    UsingMetadata metadata = UsingMetadata.from(joinTables);
    SchemaContext context = buildSchema(joinTables, mappings, metadata);
    this.schema = context.schema();
    this.columnNames = buildColumnNames(mappings);
    this.qualifierColumnMapping = context.qualifierMapping();
    this.unqualifiedColumnMapping = context.unqualifiedMapping();
    this.joinTables = joinTables;
    this.fieldMappings = List.copyOf(mappings);
    this.evaluator = new ExpressionEvaluator(schema, null, List.of(), qualifierColumnMapping, unqualifiedColumnMapping);
    this.tableCount = joinTables.size();
    this.usingMetadata = metadata;
    this.canonicalLabels = context.canonicalLabels();
    this.resultRows = computeResultRows();
    this.resultIndex = 0;
  }

  private static SchemaContext buildSchema(List<JoinTable> tables, List<FieldMapping> mappings,
      UsingMetadata usingMetadata) {
    Map<String, UsingFieldState> usingFieldStates = new LinkedHashMap<>();
    List<Schema.Field> otherFields = new ArrayList<>();
    Set<String> usedNames = new HashSet<>();
    Map<String, Integer> columnCounts = computeColumnCounts(tables, usingMetadata);
    Map<String, Map<String, String>> qualifierMap = new LinkedHashMap<>();
    Map<String, String> unqualifiedMap = new LinkedHashMap<>();
    Map<String, String> canonicalLabels = new LinkedHashMap<>();
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
        UsingColumnInfo usingInfo = usingMetadata.lookup(tableIndex, name);
        boolean usingColumn = usingInfo != null;
        boolean duplicate = columnCounts.getOrDefault(name, 0) > 1;
        String canonical;
        if (usingColumn) {
          canonical = usingInfo.canonicalName();
        } else {
          canonical = duplicate ? qualifierPrefix + "__" + name : name;
        }
        String label = (!usingColumn && duplicate) ? name : canonical;
        canonicalLabels.putIfAbsent(canonical, label);
        String lookupKey = name.toLowerCase(Locale.ROOT);
        registerQualifierMappings(qualifierMap, qualifiers, includeTableQualifier, normalizedTableName, lookupKey,
            canonical);
        if (usingColumn) {
          final boolean owner = usingInfo.isOwner(tableIndex);
          UsingFieldState state = usingFieldStates.computeIfAbsent(canonical, key -> new UsingFieldState());
          Schema.Field newField = new Schema.Field(canonical, field.schema(), field.doc(), field.defaultVal());
          state.registerField(newField, owner);
          if (owner) {
            if (!usedNames.add(canonical)) {
              throw new IllegalArgumentException("Duplicate column name '" + canonical + "' in join schema");
            }
            state.setOwner(tableIndex, name);
          } else {
            state.addAlternate(tableIndex, name);
          }
          unqualifiedMap.putIfAbsent(lookupKey, canonical);
          continue;
        } else {
          if (!usedNames.add(canonical)) {
            throw new IllegalArgumentException("Duplicate column name '" + canonical + "' in join schema");
          }
          Schema.Field newField = new Schema.Field(canonical, field.schema(), field.doc(), field.defaultVal());
          FieldMapping mapping = new FieldMapping(tableIndex, name, canonical, List.of());
          otherFields.add(newField);
          mappings.add(mapping);
          if (!duplicate) {
            unqualifiedMap.put(lookupKey, canonical);
          }
        }
      }
    }
    List<FieldMapping> orderedMappings = new ArrayList<>();
    List<Schema.Field> orderedFields = new ArrayList<>();
    for (String canonical : usingMetadata.order()) {
      UsingFieldState state = usingFieldStates.get(canonical);
      if (state != null && state.field() != null && state.ownerIndex() >= 0 && state.ownerField() != null) {
        FieldMapping mapping = new FieldMapping(state.ownerIndex(), state.ownerField(), canonical, state.alternates());
        orderedMappings.add(mapping);
        orderedFields.add(state.field());
      }
    }
    orderedMappings.addAll(mappings);
    orderedFields.addAll(otherFields);
    mappings.clear();
    mappings.addAll(orderedMappings);
    Schema joinSchema = Schema.createRecord("join_record", null, null, false);
    joinSchema.setFields(orderedFields);
    return new SchemaContext(joinSchema, qualifierMap, unqualifiedMap, Map.copyOf(canonicalLabels));
  }

  /**
   * Count how many times each column name appears across join participants while
   * respecting {@code USING} semantics (suppressed columns are ignored).
   *
   * @param tables
   *          ordered join tables
   * @param metadata
   *          precomputed {@code USING} column metadata
   * @return mapping of column name to occurrence count
   */
  private static Map<String, Integer> computeColumnCounts(List<JoinTable> tables, UsingMetadata metadata) {
    Map<String, Integer> columnCounts = new HashMap<>();
    for (int tableIndex = 0; tableIndex < tables.size(); tableIndex++) {
      JoinTable table = tables.get(tableIndex);
      for (Schema.Field field : table.schema().getFields()) {
        UsingColumnInfo usingInfo = metadata.lookup(tableIndex, field.name());
        if (usingInfo != null && !usingInfo.isOwner(tableIndex)) {
          continue;
        }
        columnCounts.merge(field.name(), 1, Integer::sum);
      }
    }
    return columnCounts;
  }

  /**
   * Register qualifier-to-canonical mappings so expression evaluators can resolve
   * column references originating from individual join inputs.
   *
   * @param qualifierMap
   *          accumulator receiving qualifier mappings
   * @param qualifiers
   *          qualifiers associated with the current table
   * @param includeTableQualifier
   *          whether the physical table name should be used as a qualifier when
   *          duplicates exist
   * @param normalizedTableName
   *          normalized physical table name (may be {@code null})
   * @param lookupKey
   *          lower-cased column name
   * @param canonical
   *          canonical column name in the join schema
   */
  private static void registerQualifierMappings(Map<String, Map<String, String>> qualifierMap, Set<String> qualifiers,
      boolean includeTableQualifier, String normalizedTableName, String lookupKey, String canonical) {
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
  }

  private static String qualifierPrefix(JoinTable table, int index) {
    String alias = IdentifierUtil.sanitizeIdentifier(table.alias());
    if (alias != null && !alias.isBlank()) {
      return alias;
    }
    String tableName = IdentifierUtil.sanitizeIdentifier(table.tableName());
    if (tableName != null && !tableName.isBlank()) {
      return tableName;
    }
    return "t" + index;
  }

  private static Set<String> qualifierSet(JoinTable table, int index) {
    Set<String> qualifiers = new LinkedHashSet<>();
    addQualifierVariant(qualifiers, table.tableName());
    addQualifierVariant(qualifiers, table.alias());
    if (qualifiers.isEmpty()) {
      qualifiers.add("t" + index);
    }
    return qualifiers;
  }

  private static void addQualifierVariant(Set<String> qualifiers, String candidate) {
    if (candidate == null) {
      return;
    }
    String trimmed = candidate.trim();
    if (trimmed.isEmpty()) {
      return;
    }
    qualifiers.add(trimmed);
    String sanitized = IdentifierUtil.sanitizeIdentifier(trimmed);
    if (!sanitized.equals(trimmed)) {
      qualifiers.add(sanitized);
    }
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
      record.put(mapping.targetField(), mapping.resolveValue(assignments));
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
  @Override
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
  @Override
  public Map<String, String> unqualifiedColumnMapping() {
    return Map.copyOf(unqualifiedColumnMapping);
  }

  /**
   * Retrieve the display labels associated with canonical column names.
   *
   * @return immutable mapping of canonical column names to their corresponding
   *         labels
   */
  public Map<String, String> canonicalLabels() {
    return Map.copyOf(canonicalLabels);
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
    List<GenericRecord> baseRows = base.rowsFor(List.of());
    if (baseRows.isEmpty()) {
      return combinations;
    }
    for (GenericRecord row : baseRows) {
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
      List<GenericRecord> candidates = table.rowsFor(combo);
      if (cross) {
        for (GenericRecord row : candidates) {
          List<GenericRecord> assignment = new ArrayList<>(combo);
          assignment.set(index, row);
          results.add(assignment);
        }
        continue;
      }
      for (GenericRecord row : candidates) {
        List<GenericRecord> assignment = new ArrayList<>(combo);
        assignment.set(index, row);
        if (usingMatches(combo, row, index) && conditionMatches(table.joinCondition(), assignment)) {
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
        if (usingMatches(combo, row, index) && conditionMatches(table.joinCondition(), assignment)) {
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
        if (usingMatches(combo, rightRows.get(rightIndex), index)
            && conditionMatches(table.joinCondition(), assignment)) {
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

  private boolean usingMatches(List<GenericRecord> assignments, GenericRecord row, int tableIndex) {
    if (row == null) {
      return false;
    }
    JoinTable table = joinTables.get(tableIndex);
    for (Schema.Field field : table.schema().getFields()) {
      UsingColumnInfo info = usingMetadata.lookup(tableIndex, field.name());
      if (info == null || info.isOwner(tableIndex)) {
        continue;
      }
      int ownerIndex = info.ownerIndex();
      GenericRecord owner = ownerIndex >= 0 && ownerIndex < assignments.size() ? assignments.get(ownerIndex) : null;
      if (owner == null) {
        return false;
      }
      Object leftValue = owner.get(info.canonicalName());
      Object rightValue = row.get(field.name());
      if (leftValue == null || rightValue == null) {
        return false;
      }
      if (!Objects.equals(leftValue, rightValue)) {
        return false;
      }
    }
    return true;
  }
}
