package se.alipsa.jparq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import se.alipsa.jparq.model.ResultSetMetaDataAdapter;

/** An implementation of the java.sql.ResultSetMetaData interface. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqResultSetMetaData extends ResultSetMetaDataAdapter {

  private final Schema schema; // may be null if empty file
  private final List<String> labels; // projection labels (aliases if present)
  private final List<String> physicalNames; // underlying physical column names (null for computed)
  private final List<String> canonicalNames; // canonical column names used for schema lookup
  private final String tableName;
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
   * @param expressions
   *          parsed SELECT-list expressions corresponding to {@code labels}
   */
  public JParqResultSetMetaData(Schema schema, List<String> labels, List<String> physicalNames,
      List<String> canonicalNames, String tableName, List<Expression> expressions) {
    this.schema = schema;
    this.labels = labels == null ? List.of() : List.copyOf(labels);
    this.physicalNames = physicalNames == null ? null
        : Collections.unmodifiableList(new ArrayList<>(physicalNames));
    this.canonicalNames = canonicalNames == null ? null
        : Collections.unmodifiableList(new ArrayList<>(canonicalNames));
    this.tableName = tableName;
    this.expressions = expressions == null ? List.of() : List.copyOf(expressions);
    this.caseInsensitiveColumnIndex = ColumnNameLookup.buildCaseInsensitiveIndex(
        this.labels.size(), this.canonicalNames, this.physicalNames, this.labels);
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

    Schema s = field.schema().getType() == Schema.Type.UNION
        ? field.schema().getTypes().stream().filter(t -> t.getType() != Schema.Type.NULL).findFirst()
            .orElse(field.schema())
        : field.schema();
    return mapSchemaToJdbcType(s);
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
      Schema fieldSchema = nonNullSchema(field.schema());
      String className = mapSchemaToJavaClassName(fieldSchema);
      if (className != null) {
        return className;
      }
    }
    String computedClassName = resolveComputedColumnClassName(column);
    if (computedClassName != null) {
      return computedClassName;
    }
    return mapJdbcTypeToClassName(getColumnType(column));
  }

  @Override
  public int isNullable(int column) {
    return columnNullableUnknown;
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
    return mapSchemaToJdbcType(nonNullSchema(baseField.schema()));
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
    return mapSchemaToJavaClassName(nonNullSchema(baseField.schema()));
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

  /**
   * Resolve a non-null schema from a field, unwrapping optional unions when
   * necessary.
   *
   * @param fieldSchema
   *          the schema associated with a column
   * @return the underlying non-null schema
   */
  private Schema nonNullSchema(Schema fieldSchema) {
    if (fieldSchema == null || fieldSchema.getType() != Schema.Type.UNION) {
      return fieldSchema;
    }
    return fieldSchema.getTypes().stream().filter(t -> t.getType() != Schema.Type.NULL).findFirst().orElse(fieldSchema);
  }

  /**
   * Map an Avro schema type to the corresponding JDBC type.
   *
   * @param schema
   *          the schema describing the value
   * @return the JDBC type constant matching {@code schema}
   */
  private int mapSchemaToJdbcType(Schema schema) {
    Schema base = nonNullSchema(schema);
    if (base == null) {
      return Types.OTHER;
    }
    return switch (base.getType()) {
      case STRING, ENUM -> Types.VARCHAR;
      case INT -> (LogicalTypes.date().equals(base.getLogicalType()) ? Types.DATE : Types.INTEGER);
      case LONG -> (base.getLogicalType() instanceof LogicalTypes.TimestampMillis
          || base.getLogicalType() instanceof LogicalTypes.TimestampMicros ? Types.TIMESTAMP : Types.BIGINT);
      case FLOAT -> Types.REAL;
      case DOUBLE -> Types.DOUBLE;
      case BOOLEAN -> Types.BOOLEAN;
      case BYTES, FIXED -> {
        if (base.getLogicalType() instanceof LogicalTypes.Decimal) {
          yield Types.DECIMAL;
        }
        yield isUtf8String(base) ? Types.VARCHAR : Types.BINARY;
      }
      case RECORD -> Types.STRUCT;
      case ARRAY -> Types.ARRAY;
      default -> Types.OTHER;
    };
  }

  /**
   * Map an Avro schema to the fully qualified Java class name representing
   * values for that schema.
   *
   * <p>The mapping mirrors {@link #mapSchemaToJdbcType(Schema)} to keep reported
   * JDBC types and Java classes aligned when falling back from schema-based
   * resolution.</p>
   *
   * @param schema
   *          the schema describing the value
   * @return the fully qualified Java class name representing {@code schema}
   */
  private String mapSchemaToJavaClassName(Schema schema) {
    Schema base = nonNullSchema(schema);
    if (base == null) {
      return Object.class.getName();
    }
    return switch (base.getType()) {
      case STRING, ENUM -> String.class.getName();
      case INT -> {
        if (LogicalTypes.date().equals(base.getLogicalType())) {
          yield Date.class.getName();
        }
        if (base.getLogicalType() instanceof LogicalTypes.TimeMillis) {
          yield Time.class.getName();
        }
        yield Integer.class.getName();
      }
      case LONG -> {
        if (base.getLogicalType() instanceof LogicalTypes.TimestampMillis
            || base.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
          yield Timestamp.class.getName();
        }
        if (base.getLogicalType() instanceof LogicalTypes.TimeMicros) {
          yield Time.class.getName();
        }
        yield Long.class.getName();
      }
      case FLOAT -> Float.class.getName();
      case DOUBLE -> Double.class.getName();
      case BOOLEAN -> Boolean.class.getName();
      case BYTES, FIXED -> (base.getLogicalType() instanceof LogicalTypes.Decimal)
          ? BigDecimal.class.getName()
          : byte[].class.getName();
      case RECORD -> Map.class.getName();
      case ARRAY -> List.class.getName();
      case MAP -> Map.class.getName();
      default -> Object.class.getName();
    };
  }

  private String mapJdbcTypeToClassName(int jdbcType) {
    return switch (jdbcType) {
      case Types.BIT, Types.BOOLEAN -> Boolean.class.getName();
      case Types.TINYINT -> Byte.class.getName();
      case Types.SMALLINT -> Short.class.getName();
      case Types.INTEGER -> Integer.class.getName();
      case Types.BIGINT -> Long.class.getName();
      case Types.REAL -> Float.class.getName();
      case Types.FLOAT, Types.DOUBLE -> Double.class.getName();
      case Types.NUMERIC, Types.DECIMAL -> BigDecimal.class.getName();
      case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> String.class
          .getName();
      case Types.DATE -> Date.class.getName();
      case Types.TIME, Types.TIME_WITH_TIMEZONE -> Time.class.getName();
      case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> Timestamp.class.getName();
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> byte[].class.getName();
      case Types.ARRAY -> List.class.getName();
      case Types.CLOB, Types.NCLOB -> java.sql.Clob.class.getName();
      case Types.BLOB -> java.sql.Blob.class.getName();
      case Types.STRUCT -> Map.class.getName();
      default -> Object.class.getName();
    };
  }

  private boolean isUtf8String(Schema schema) {
    if (schema == null) {
      return false;
    }
    if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
      return false;
    }
    String logicalType = schema.getProp("logicalType");
    if (logicalType != null && "decimal".equalsIgnoreCase(logicalType)) {
      return false;
    }
    if (logicalType != null && "string".equalsIgnoreCase(logicalType)) {
      return true;
    }
    String originalType = schema.getProp("originalType");
    if (originalType != null && "UTF8".equalsIgnoreCase(originalType)) {
      return true;
    }
    String convertedType = schema.getProp("convertedType");
    if (convertedType != null && "UTF8".equalsIgnoreCase(convertedType)) {
      return true;
    }
    Object javaString = schema.getObjectProp("avro.java.string");
    return javaString != null;
  }
}
