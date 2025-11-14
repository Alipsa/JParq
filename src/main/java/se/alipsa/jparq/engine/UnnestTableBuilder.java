package se.alipsa.jparq.engine;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import se.alipsa.jparq.engine.JoinRecordReader.CorrelatedRowsSupplier;
import se.alipsa.jparq.engine.JoinRecordReader.JoinTable;
import se.alipsa.jparq.engine.SqlParser.TableReference;
import se.alipsa.jparq.engine.SqlParser.UnnestDefinition;
import se.alipsa.jparq.helper.JParqUtil;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/**
 * Builds {@link JoinTable} representations for {@code UNNEST} table functions, including support for
 * {@code WITH ORDINALITY} and arrays of records.
 */
public final class UnnestTableBuilder {

  private UnnestTableBuilder() {
  }

  /**
   * Construct a {@link JoinTable} representation for the supplied {@code UNNEST} table reference.
   *
   * @param reference
   *          the table reference describing the {@code UNNEST} invocation
   * @param priorTables
   *          tables that precede the {@code UNNEST} call and establish the correlation context
   * @param qualifierIndex
   *          lookup of qualifier names to table positions used to resolve correlated column references
   * @return a {@link JoinTable} that lazily evaluates the {@code UNNEST} operation
   * @throws SQLException
   *           if the {@code UNNEST} definition is invalid or cannot be resolved
   */
  public static JoinTable build(TableReference reference, List<JoinTable> priorTables,
      Map<String, Integer> qualifierIndex) throws SQLException {
    Objects.requireNonNull(reference, "reference");
    UnnestDefinition definition = reference.unnest();
    if (definition == null) {
      throw new SQLException("Table reference does not describe an UNNEST definition");
    }
    if (priorTables == null || priorTables.isEmpty()) {
      throw new SQLException("UNNEST requires at least one preceding table");
    }

    Expression expression = definition.expression();
    if (!(expression instanceof Column column)) {
      throw new SQLException("UNNEST currently supports column references only: " + expression);
    }

    String columnName = column.getColumnName();
    if (columnName == null || columnName.isBlank()) {
      throw new SQLException("UNNEST column reference must specify a column name");
    }

    int sourceIndex = resolveSourceIndex(column, columnName, priorTables, qualifierIndex);
    JoinTable sourceTable = priorTables.get(sourceIndex);
    Schema.Field arrayField = resolveField(sourceTable.schema(), columnName);
    if (arrayField == null) {
      throw new SQLException("Column '" + columnName + "' not found in table '" + sourceTable.tableName() + "'");
    }

    Schema arraySchema = JdbcTypeMapper.nonNullSchema(arrayField.schema());
    if (arraySchema == null || arraySchema.getType() != Schema.Type.ARRAY) {
      throw new SQLException("UNNEST requires an ARRAY column but found: " + arrayField.schema());
    }
    Schema elementSchema = JdbcTypeMapper.nonNullSchema(arraySchema.getElementType());
    if (elementSchema == null) {
      throw new SQLException("Unable to resolve element schema for UNNEST column '" + columnName + "'");
    }

    AliasPlan aliasPlan = AliasPlan.build(definition, elementSchema, arrayField.name());

    Schema unnestSchema = buildSchema(reference, elementSchema, aliasPlan);
    CorrelatedRowsSupplier supplier = assignments -> evaluateAssignments(assignments, sourceIndex,
        arrayField.name(), elementSchema, aliasPlan, unnestSchema);

    SqlParser.JoinType joinType = reference.joinType();
    if (joinType == SqlParser.JoinType.RIGHT_OUTER || joinType == SqlParser.JoinType.FULL_OUTER) {
      throw new SQLException("UNNEST does not support " + joinType + " joins");
    }

    String tableName = reference.tableAlias() != null ? reference.tableAlias() : "unnest";
    return new JoinTable(tableName, reference.tableAlias(), unnestSchema, List.of(), joinType, null,
        List.of(), supplier);
  }

  private static int resolveSourceIndex(Column column, String columnName, List<JoinTable> priorTables,
      Map<String, Integer> qualifierIndex) throws SQLException {
    String qualifier = column.getTable() == null ? null : column.getTable().getName();
    String normalizedQualifier = JParqUtil.normalizeQualifier(qualifier);
    if (normalizedQualifier != null && qualifierIndex != null) {
      Integer index = qualifierIndex.get(normalizedQualifier);
      if (index == null) {
        throw new SQLException("UNNEST references unknown qualifier '" + qualifier + "'");
      }
      return index;
    }
    int resolved = -1;
    for (int i = priorTables.size() - 1; i >= 0; i--) {
      if (resolveField(priorTables.get(i).schema(), columnName) != null) {
        if (resolved >= 0) {
          throw new SQLException("Ambiguous UNNEST column reference '" + columnName + "'");
        }
        resolved = i;
      }
    }
    if (resolved < 0) {
      throw new SQLException("UNNEST column '" + columnName + "' not found in preceding tables");
    }
    return resolved;
  }

  private static Schema.Field resolveField(Schema schema, String columnName) {
    if (schema == null || columnName == null) {
      return null;
    }
    Schema.Field direct = schema.getField(columnName);
    if (direct != null) {
      return direct;
    }
    for (Schema.Field field : schema.getFields()) {
      if (field.name().equalsIgnoreCase(columnName)) {
        return field;
      }
    }
    return null;
  }

  private static Schema buildSchema(TableReference reference, Schema elementSchema, AliasPlan aliasPlan) {
    String schemaName = reference.tableAlias() != null ? reference.tableAlias() : "unnest_row";
    Schema schema = Schema.createRecord(schemaName, null, null, false);
    List<Schema.Field> fields = new ArrayList<>();
    if (elementSchema.getType() == Schema.Type.RECORD) {
      List<Schema.Field> nestedFields = elementSchema.getFields();
      for (int i = 0; i < nestedFields.size(); i++) {
        Schema.Field nested = nestedFields.get(i);
        fields.add(new Schema.Field(aliasPlan.elementAliases().get(i), nested.schema(), nested.doc(),
            nested.defaultVal()));
      }
    } else {
      fields.add(new Schema.Field(aliasPlan.elementAliases().get(0), elementSchema, null, (Object) null));
    }
    if (aliasPlan.withOrdinality()) {
      Schema.Field ordinalField = new Schema.Field(aliasPlan.ordinalAlias(), Schema.create(Schema.Type.INT),
          "1-based position within the original array", null);
      fields.add(ordinalField);
    }
    schema.setFields(fields);
    return schema;
  }

  private static List<GenericRecord> evaluateAssignments(List<GenericRecord> assignments, int sourceIndex,
      String arrayFieldName, Schema elementSchema, AliasPlan aliasPlan, Schema unnestSchema) {
    if (assignments == null || sourceIndex < 0 || sourceIndex >= assignments.size()) {
      return List.of();
    }
    GenericRecord sourceRecord = assignments.get(sourceIndex);
    if (sourceRecord == null) {
      return List.of();
    }
    Object raw = sourceRecord.get(arrayFieldName);
    if (raw == null) {
      return List.of();
    }
    Iterable<?> iterable = toIterable(raw);
    if (iterable == null) {
      return List.of();
    }
    List<GenericRecord> rows = new ArrayList<>();
    int position = 1;
    for (Object element : iterable) {
      GenericData.Record row = new GenericData.Record(unnestSchema);
      if (elementSchema.getType() == Schema.Type.RECORD) {
        GenericRecord nested = element instanceof GenericRecord ? (GenericRecord) element : null;
        List<Schema.Field> nestedFields = elementSchema.getFields();
        for (int i = 0; i < nestedFields.size(); i++) {
          Object value = nested == null ? null : nested.get(nestedFields.get(i).name());
          row.put(aliasPlan.elementAliases().get(i), value);
        }
      } else {
        row.put(aliasPlan.elementAliases().get(0), element);
      }
      if (aliasPlan.withOrdinality()) {
        row.put(aliasPlan.ordinalAlias(), position);
      }
      rows.add(row);
      position++;
    }
    return rows;
  }

  private static Iterable<?> toIterable(Object raw) {
    if (raw instanceof GenericData.Array<?> array) {
      return array;
    }
    if (raw instanceof Iterable<?> iterable) {
      return iterable;
    }
    if (raw instanceof Object[] objects) {
      return Arrays.asList(objects);
    }
    return null;
  }

  private record AliasPlan(List<String> elementAliases, boolean withOrdinality, String ordinalAlias) {

    static AliasPlan build(UnnestDefinition definition, Schema elementSchema, String fallbackAlias)
        throws SQLException {
      List<String> aliases = new ArrayList<>(definition.columnAliases());
      boolean withOrdinality = definition.withOrdinality();
      String ordAlias = null;
      if (withOrdinality) {
        if (!aliases.isEmpty()) {
          ordAlias = sanitizeAlias(aliases.remove(aliases.size() - 1), "ordinality");
        } else {
          ordAlias = "ordinality";
        }
      }
      List<String> elementAliases = resolveElementAliases(elementSchema, aliases, fallbackAlias);
      return new AliasPlan(elementAliases, withOrdinality, ordAlias);
    }

    private static List<String> resolveElementAliases(Schema elementSchema, List<String> aliases, String fallback)
        throws SQLException {
      List<String> resolved = new ArrayList<>();
      if (elementSchema.getType() == Schema.Type.RECORD) {
        List<Schema.Field> fields = elementSchema.getFields();
        if (!aliases.isEmpty() && aliases.size() != fields.size()) {
          throw new SQLException("UNNEST column alias count does not match record element fields");
        }
        for (int i = 0; i < fields.size(); i++) {
          String alias = aliases.isEmpty() ? fields.get(i).name() : aliases.get(i);
          resolved.add(sanitizeAlias(alias, fields.get(i).name()));
        }
      } else {
        if (!aliases.isEmpty() && aliases.size() > 1) {
          throw new SQLException("UNNEST element alias list is too long for a single-value array");
        }
        String alias = aliases.isEmpty() ? fallback : aliases.get(0);
        resolved.add(sanitizeAlias(alias, fallback));
      }
      return List.copyOf(resolved);
    }

    private static String sanitizeAlias(String alias, String fallback) {
      if (alias == null) {
        return fallback;
      }
      String trimmed = alias.trim();
      return trimmed.isEmpty() ? fallback : trimmed;
    }

    /**
     * Determine the alias that should be used for the ordinality column when {@code WITH ORDINALITY} is
     * present.
     *
     * @return the alias assigned to the ordinality column, defaulting to {@code ordinality} when unspecified
     */
    public String ordinalAlias() {
      return ordinalAlias == null || ordinalAlias.isBlank() ? "ordinality" : ordinalAlias;
    }
  }
}
