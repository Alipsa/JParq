package se.alipsa.jparq;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import se.alipsa.jparq.engine.BooleanExpressions;
import se.alipsa.jparq.engine.ColumnMappingUtil;
import se.alipsa.jparq.engine.CorrelationMappings;
import se.alipsa.jparq.engine.DecimalCasts;
import se.alipsa.jparq.engine.Identifier;
import se.alipsa.jparq.engine.MaterializedNames;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.model.ResultSetMetaDataAdapter;

/** An implementation of the java.sql.ResultSetMetaData interface. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqResultSetMetaData extends ResultSetMetaDataAdapter implements AvroColumnMetadata {

  private final Schema schema; // may be null if empty file
  private final List<String> labels; // projection labels (aliases if present)
  private final List<String> physicalNames; // underlying physical column names (null for computed)
  private final List<String> canonicalNames; // canonical column names used for schema lookup
  private final String tableName;
  private final String tableSchema;
  private final String tableCatalog;
  private final List<Expression> expressions;
  private final Map<String, Schema.Field> schemaFieldLookup;
  private final CorrelationMappings sourceMappings;

  /**
   * Constructor: labels (aliases) + physical names (null entries allowed).
   *
   * @param schema
   *          the Avro schema (may be null if empty file)
   * @param labels
   *          the column labels (aliases)
   * @param physicalNames
   *          the underlying physical column names (null for computed)
   * @param canonicalNames
   *          canonical column names used internally for schema lookups (may be
   *          {@code null})
   * @param tableName
   *          the table name
   * @param tableSchema
   *          schema that owns the table (may be {@code null})
   * @param tableCatalog
   *          catalog that owns the table (may be {@code null})
   * @param expressions
   *          parsed SELECT-list expressions corresponding to {@code labels}
   */
  public JParqResultSetMetaData(Schema schema, List<String> labels, List<String> physicalNames,
      List<String> canonicalNames, String tableName, String tableSchema, String tableCatalog,
      List<Expression> expressions) {
    this(schema, labels, physicalNames, canonicalNames, tableName, tableSchema, tableCatalog, expressions, null);
  }

  /**
   * Creates metadata using source mappings independently of output aliases.
   *
   * @param schema
   *          source Avro schema, including for empty results
   * @param labels
   *          output labels in projection order
   * @param physicalNames
   *          underlying column names, or {@code null}
   * @param canonicalNames
   *          canonical projection names, or {@code null}
   * @param tableName
   *          table name
   * @param tableSchema
   *          owning schema, or {@code null}
   * @param tableCatalog
   *          owning catalog, or {@code null}
   * @param expressions
   *          SELECT expressions in projection order
   * @param sourceMappings
   *          reader mappings without output aliases, or {@code null} to resolve
   *          directly against schema field names
   */
  public JParqResultSetMetaData(Schema schema, List<String> labels, List<String> physicalNames,
      List<String> canonicalNames, String tableName, String tableSchema, String tableCatalog,
      List<Expression> expressions, CorrelationMappings sourceMappings) {
    this.schema = schema;
    this.labels = labels == null ? List.of() : List.copyOf(labels);
    this.physicalNames = physicalNames == null ? null : Collections.unmodifiableList(new ArrayList<>(physicalNames));
    this.canonicalNames = canonicalNames == null ? null : Collections.unmodifiableList(new ArrayList<>(canonicalNames));
    this.tableName = tableName;
    this.tableSchema = tableSchema;
    this.tableCatalog = tableCatalog;
    this.expressions = expressions == null ? List.of() : List.copyOf(expressions);
    this.schemaFieldLookup = schema == null ? Map.of() : buildSchemaFieldLookup(schema);
    this.sourceMappings = sourceMappings;
  }

  @Override
  public int getColumnCount() {
    return labels.size();
  }

  /** {@inheritDoc} */
  @Override
  public String columnSqlLabel(int column) {
    String label = labels.get(column - 1);
    Schema.Field field = schema == null || !expressions.isEmpty() ? null : schema.getField(label);
    return field == null ? label : MaterializedNames.sqlLabel(field);
  }

  @Override
  public String getColumnLabel(int column) {
    return MaterializedNames.displayLabel(columnSqlLabel(column));
  }

  @Override
  public String getColumnName(int column) {
    String expressionName = resolveExpressionColumnName(column);
    if (expressionName != null && !expressionName.isBlank()) {
      return MaterializedNames.displayLabel(expressionName);
    }
    int index = column - 1;
    if (physicalNames != null && index >= 0 && index < physicalNames.size()) {
      String phys = physicalNames.get(index);
      if (phys != null && !phys.isBlank()) {
        Schema.Field field = schema == null ? null : schema.getField(phys);
        return MaterializedNames.displayLabel(field == null ? phys : MaterializedNames.sqlLabel(field));
      }
    }
    if (index >= 0 && index < labels.size()) {
      return getColumnLabel(column);
    }
    return "column_" + column;
  }

  @Override
  public String getTableName(int column) {
    return tableName;
  }

  @Override
  public String getSchemaName(int column) {
    return tableSchema == null ? "" : tableSchema;
  }

  @Override
  public String getCatalogName(int column) {
    return tableCatalog == null ? "" : tableCatalog;
  }

  private String resolveExpressionColumnName(int column) {
    if (expressions == null || expressions.isEmpty()) {
      return null;
    }
    int index = column - 1;
    if (index < 0 || index >= expressions.size()) {
      return null;
    }
    Expression expression = expressions.get(index);
    if (expression instanceof Column columnExpression) {
      return columnExpression.getColumnName();
    }
    return null;
  }

  @Override
  public int getColumnType(int column) {
    if (declaredDecimalType(column) != null) {
      return Types.DECIMAL;
    }
    int computedType = resolveComputedColumnType(column);
    // A computed Boolean alias may have the same name as a source field.
    if (computedType == Types.BOOLEAN) {
      return computedType;
    }
    Schema.Field field = resolveField(column);
    if (field == null) {
      return computedType;
    }
    return JdbcTypeMapper.mapSchemaToJdbcType(field.schema());
  }

  /**
   * Retains the source schema of a physical column during materialization.
   *
   * @param column
   *          one-based output column index
   * @return non-null source schema, or null when no physical field is known
   */
  @Override
  public Schema columnValueSchema(int column) {
    Schema.Field field = resolveField(column);
    Schema valueSchema = field == null ? null : JdbcTypeMapper.nonNullSchema(field.schema());
    return valueSchema;
  }

  @Override
  public String getColumnTypeName(int column) {
    return java.sql.JDBCType.valueOf(getColumnType(column)).getName();
  }

  /** {@inheritDoc} */
  @Override
  public int getPrecision(int column) {
    LogicalTypes.Decimal decimal = decimalType(column);
    return decimal == null ? 0 : decimal.getPrecision();
  }

  /** {@inheritDoc} */
  @Override
  public int getScale(int column) {
    LogicalTypes.Decimal decimal = decimalType(column);
    return decimal == null ? 0 : decimal.getScale();
  }

  /**
   * Resolves the decimal logical type of an explicit cast or projected source
   * field.
   *
   * @param column
   *          the one-based output column index
   * @return the decimal type, or null when no decimal bounds are available
   */
  private LogicalTypes.Decimal decimalType(int column) {
    LogicalTypes.Decimal declared = declaredDecimalType(column);
    if (declared != null) {
      return declared;
    }
    if (getColumnType(column) != Types.DECIMAL && getColumnType(column) != Types.NUMERIC) {
      return null;
    }
    Schema.Field field = resolveField(column);
    Schema valueSchema = field == null ? null : JdbcTypeMapper.nonNullSchema(field.schema());
    return valueSchema != null && valueSchema.getLogicalType() instanceof LogicalTypes.Decimal decimal ? decimal : null;
  }

  /**
   * Resolves explicitly declared precision and scale for a decimal cast.
   *
   * @param column
   *          the one-based output column index
   * @return the declared decimal type, or null without an explicit precision
   */
  private LogicalTypes.Decimal declaredDecimalType(int column) {
    return expressionAtColumn(column) instanceof CastExpression cast ? DecimalCasts.declaredType(cast) : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnClassName(int column) {
    if (getColumnType(column) == Types.BOOLEAN) {
      return Boolean.class.getName();
    }
    Schema.Field field = resolveField(column);
    if (field != null) {
      return JdbcTypeMapper.mapSchemaToJavaClassName(field.schema());
    }
    String computedClassName = resolveComputedColumnClassName(column);
    if (computedClassName != null) {
      return computedClassName;
    }
    return JdbcTypeMapper.mapJdbcTypeToClassName(getColumnType(column));
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * Parquet repetition is mapped to JDBC nullability using the derived Avro
   * schema: REQUIRED fields are {@link java.sql.ResultSetMetaData#columnNoNulls},
   * OPTIONAL unions and REPEATED arrays are
   * {@link java.sql.ResultSetMetaData#columnNullable}. Computed columns or
   * missing schema information fall back to
   * {@link java.sql.ResultSetMetaData#columnNullableUnknown}.
   *
   * @param column
   *          the 1-based column index from the result set
   * @return the JDBC nullability constant for the column
   */
  @Override
  public int isNullable(int column) {
    Schema.Field field = resolveField(column);
    if (field == null) {
      return columnNullableUnknown;
    }
    Schema fieldSchema = field.schema();
    if (JdbcTypeMapper.isNullable(fieldSchema)) {
      return columnNullable;
    }
    Schema nonNullSchema = JdbcTypeMapper.nonNullSchema(fieldSchema);
    if (nonNullSchema != null && nonNullSchema.getType() == Schema.Type.ARRAY) {
      return columnNullable;
    }
    return columnNoNulls;
  }

  /**
   * Resolve the Avro schema field associated with the supplied column index.
   * Canonical column names are preferred to ensure consistent lookups when joins
   * introduce disambiguated field names.
   *
   * @param column
   *          the 1-based column index from the result set
   * @return the matching {@link Schema.Field}, or {@code null} when the column is
   *         computed
   */
  private Schema.Field resolveField(int column) {
    if (schema == null) {
      return null;
    }
    String canonical = canonicalName(column);
    Schema.Field field = fieldBySchemaName(canonical);
    if (field != null) {
      return field;
    }
    return fieldBySchemaName(getColumnLabel(column));
  }

  private String canonicalName(int column) {
    return ColumnNameLookup.canonicalName(canonicalNames, physicalNames, labels, column);
  }

  /**
   * Determine the JDBC type for a computed column when no Avro schema field is
   * available, including Boolean predicates, arrays, and analytic functions.
   *
   * @param column
   *          the 1-based column index from the result set
   * @return the resolved {@link java.sql.Types JDBC type}
   */
  private int resolveComputedColumnType(int column) {
    Expression expression = expressionAtColumn(column);
    if (BooleanExpressions.isBoolean(expression, reference -> {
      Schema.Field field = resolveSourceField(reference);
      return field != null && JdbcTypeMapper.mapSchemaToJdbcType(field.schema()) == Types.BOOLEAN;
    })) {
      return Types.BOOLEAN;
    }
    return switch (expression) {
      case AnalyticExpression analytic -> resolveAnalyticFunctionType(analytic);
      case ArrayConstructor arrayConstructor -> Types.ARRAY;
      case net.sf.jsqlparser.expression.Function func when func.getName() != null
          && "ARRAY".equalsIgnoreCase(func.getName()) ->
        Types.ARRAY;
      case null, default -> Types.OTHER;
    };
  }

  private String resolveComputedColumnClassName(int column) {
    Expression expression = expressionAtColumn(column);
    return switch (expression) {
      case AnalyticExpression analytic -> resolveAnalyticFunctionClassName(analytic);
      case ArrayConstructor arrayConstructor -> List.class.getName();
      case net.sf.jsqlparser.expression.Function func when func.getName() != null
          && "ARRAY".equalsIgnoreCase(func.getName()) ->
        List.class.getName();
      case null, default -> null;
    };
  }

  private Expression expressionAtColumn(int column) {
    if (expressions == null || expressions.isEmpty()) {
      return null;
    }
    int index = column - 1;
    if (index < 0 || index >= expressions.size()) {
      return null;
    }
    return expressions.get(index);
  }

  /**
   * Resolve the JDBC type returned by an analytic window function.
   *
   * @param analytic
   *          the analytic expression describing the computed column
   * @return the JDBC type reported for the analytic function
   */
  private int resolveAnalyticFunctionType(AnalyticExpression analytic) {
    String functionName = analytic.getName();
    if (functionName == null) {
      return Types.OTHER;
    }
    String normalizedFunction = functionName.toUpperCase(Locale.ROOT);
    return switch (normalizedFunction) {
      case "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE" -> Types.BIGINT;
      case "PERCENT_RANK", "CUME_DIST" -> Types.DOUBLE;
      case "LAG", "LEAD" -> resolveNavigationResultType(analytic);
      default -> Types.OTHER;
    };
  }

  private String resolveAnalyticFunctionClassName(AnalyticExpression analytic) {
    String functionName = analytic.getName();
    if (functionName == null) {
      return null;
    }
    String normalizedFunction = functionName.toUpperCase(Locale.ROOT);
    return switch (normalizedFunction) {
      case "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE" -> Long.class.getName();
      case "PERCENT_RANK", "CUME_DIST" -> Double.class.getName();
      case "LAG", "LEAD" -> resolveNavigationResultClassName(analytic);
      default -> null;
    };
  }

  /**
   * Resolve the JDBC type for navigation analytic functions such as {@code LAG}
   * and {@code LEAD} based on the referenced column.
   *
   * @param analytic
   *          the analytic expression describing the navigation function
   * @return the JDBC type of the referenced column, or {@link Types#OTHER} when
   *         it cannot be determined
   */
  private int resolveNavigationResultType(AnalyticExpression analytic) {
    if (schema == null) {
      return Types.OTHER;
    }
    Expression argument = analytic.getExpression();
    if (!(argument instanceof Column lagColumn)) {
      return Types.OTHER;
    }
    Schema.Field baseField = resolveSourceField(lagColumn);
    if (baseField == null) {
      return Types.OTHER;
    }
    return JdbcTypeMapper.mapSchemaToJdbcType(baseField.schema());
  }

  private String resolveNavigationResultClassName(AnalyticExpression analytic) {
    if (schema == null) {
      return null;
    }
    Expression argument = analytic.getExpression();
    if (!(argument instanceof Column lagColumn)) {
      return null;
    }
    Schema.Field baseField = resolveSourceField(lagColumn);
    if (baseField == null) {
      return null;
    }
    return JdbcTypeMapper.mapSchemaToJavaClassName(baseField.schema());
  }

  /**
   * Resolves a source reference without consulting result labels. Unknown or
   * ambiguous references cannot establish a computed expression's type.
   *
   * @param column
   *          qualified or unqualified source column
   * @return the source field, or {@code null} when resolution is uncertain
   */
  private Schema.Field resolveSourceField(Column column) {
    if (schema == null || column == null || column.getColumnName() == null || column.getColumnName().isBlank()) {
      return null;
    }
    String name = column.getColumnName();
    String qualifier = column.getTable() == null ? null : column.getTable().getName();
    if (sourceMappings != null && (!sourceMappings.qualifierColumnMapping().isEmpty()
        || !sourceMappings.unqualifiedColumnMapping().isEmpty())) {
      try {
        String canonical = ColumnMappingUtil.canonicalFieldName(qualifier, name,
            sourceMappings.qualifierColumnMapping(), sourceMappings.unqualifiedColumnMapping(), Map.of());
        return fieldBySchemaName(canonical);
      } catch (IllegalArgumentException unresolved) {
        return null;
      }
    }
    Identifier owner = Identifier.of(qualifier);
    if (owner != null) {
      return fieldBySchemaName(owner.text() + "__" + Identifier.of(name).text());
    }
    return fieldBySchemaName(name);
  }

  private Schema.Field fieldBySchemaName(String name) {
    if (schema == null || name == null) {
      return null;
    }
    Schema.Field field = schemaFieldLookup.get(ColumnNameLookup.normalizeKey(name));
    if (field != null) {
      return field;
    }
    return schema.getField(name);
  }

  private Map<String, Schema.Field> buildSchemaFieldLookup(Schema sourceSchema) {
    if (sourceSchema == null) {
      return Map.of();
    }
    Map<String, Schema.Field> map = new LinkedHashMap<>();
    for (Schema.Field field : sourceSchema.getFields()) {
      String key = ColumnNameLookup.normalizeKey(field.name());
      if (!key.isEmpty()) {
        map.putIfAbsent(key, field);
      }
    }
    return Collections.unmodifiableMap(map);
  }

}
