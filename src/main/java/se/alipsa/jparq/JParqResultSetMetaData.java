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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.model.ResultSetMetaDataAdapter;

/** An implementation of the java.sql.ResultSetMetaData interface. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqResultSetMetaData extends ResultSetMetaDataAdapter {

  private final Schema schema; // may be null if empty file
  private final List<String> labels; // projection labels (aliases if present)
  private final List<String> physicalNames; // underlying physical column names (null for computed)
  private final List<String> canonicalNames; // canonical column names used for schema lookup
  private final String tableName;
  private final String tableSchema;
  private final String tableCatalog;
  private final List<Expression> expressions;
  private final Map<String, Integer> caseInsensitiveColumnIndex;
  private final Map<String, Schema.Field> schemaFieldLookup;

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
    this.schema = schema;
    this.labels = labels == null ? List.of() : List.copyOf(labels);
    this.physicalNames = physicalNames == null ? null : Collections.unmodifiableList(new ArrayList<>(physicalNames));
    this.canonicalNames = canonicalNames == null ? null : Collections.unmodifiableList(new ArrayList<>(canonicalNames));
    this.tableName = tableName;
    this.tableSchema = tableSchema;
    this.tableCatalog = tableCatalog;
    this.expressions = expressions == null ? List.of() : List.copyOf(expressions);
    this.caseInsensitiveColumnIndex = ColumnNameLookup.buildCaseInsensitiveIndex(this.labels.size(),
        this.canonicalNames, this.physicalNames, this.labels);
    this.schemaFieldLookup = schema == null ? Map.of() : buildSchemaFieldLookup(schema);
  }

  @Override
  public int getColumnCount() {
    return labels.size();
  }

  @Override
  public String getColumnLabel(int column) {
    return labels.get(column - 1);
  }

  @Override
  public String getColumnName(int column) {
    String expressionName = resolveExpressionColumnName(column);
    if (expressionName != null && !expressionName.isBlank()) {
      return expressionName;
    }
    int index = column - 1;
    if (physicalNames != null && index >= 0 && index < physicalNames.size()) {
      String phys = physicalNames.get(index);
      if (phys != null && !phys.isBlank()) {
        return phys;
      }
    }
    if (index >= 0 && index < labels.size()) {
      return labels.get(index);
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
    if (schema == null) {
      return Types.OTHER;
    }

    Schema.Field field = resolveField(column);
    if (field == null) {
      return resolveComputedColumnType(column);
    }
    return JdbcTypeMapper.mapSchemaToJdbcType(field.schema());
  }

  @Override
  public String getColumnTypeName(int column) {
    return java.sql.JDBCType.valueOf(getColumnType(column)).getName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnClassName(int column) {
    Schema.Field field = resolveField(column);
    if (field != null) {
      String className = JdbcTypeMapper.mapSchemaToJavaClassName(field.schema());
      if (className != null) {
        return className;
      }
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
   * schema: REQUIRED fields are {@link java.sql.ResultSetMetaData#columnNoNulls}, OPTIONAL unions and
   * REPEATED arrays are {@link java.sql.ResultSetMetaData#columnNullable}. Computed columns or missing
   * schema information fall back to {@link java.sql.ResultSetMetaData#columnNullableUnknown}.
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
   * available. At present only analytic window functions are recognised.
   *
   * @param column
   *          the 1-based column index from the result set
   * @return the resolved {@link java.sql.Types JDBC type}
   */
  private int resolveComputedColumnType(int column) {
    Expression expression = expressionAtColumn(column);
    if (expression == null) {
      return Types.OTHER;
    }
    if (expression instanceof AnalyticExpression analytic) {
      return resolveAnalyticFunctionType(analytic);
    }
    if (expression instanceof ArrayConstructor) {
      return Types.ARRAY;
    }
    if (expression instanceof net.sf.jsqlparser.expression.Function func && func.getName() != null
        && "ARRAY".equalsIgnoreCase(func.getName())) {
      return Types.ARRAY;
    }
    return Types.OTHER;
  }

  private String resolveComputedColumnClassName(int column) {
    Expression expression = expressionAtColumn(column);
    if (expression == null) {
      return null;
    }
    if (expression instanceof AnalyticExpression analytic) {
      return resolveAnalyticFunctionClassName(analytic);
    }
    if (expression instanceof ArrayConstructor) {
      return List.class.getName();
    }
    if (expression instanceof net.sf.jsqlparser.expression.Function func && func.getName() != null
        && "ARRAY".equalsIgnoreCase(func.getName())) {
      return List.class.getName();
    }
    return null;
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
    String columnName = lagColumn.getColumnName();
    if (columnName == null) {
      return Types.OTHER;
    }
    Schema.Field baseField = lookupFieldByName(columnName);
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
    String columnName = lagColumn.getColumnName();
    if (columnName == null) {
      return null;
    }
    Schema.Field baseField = lookupFieldByName(columnName);
    if (baseField == null) {
      return null;
    }
    return JdbcTypeMapper.mapSchemaToJavaClassName(baseField.schema());
  }

  private Schema.Field lookupFieldByName(String name) {
    if (schema == null || name == null) {
      return null;
    }
    Schema.Field direct = fieldBySchemaName(name);
    if (direct != null) {
      return direct;
    }
    Integer columnIndex = caseInsensitiveColumnIndex.get(ColumnNameLookup.normalizeKey(name));
    if (columnIndex == null) {
      return null;
    }
    String canonical = canonicalName(columnIndex);
    if (canonical == null) {
      return null;
    }
    return fieldBySchemaName(canonical);
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
