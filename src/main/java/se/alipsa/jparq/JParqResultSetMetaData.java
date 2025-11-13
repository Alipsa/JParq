package se.alipsa.jparq;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
    if (canonical != null) {
      Schema.Field field = schema.getField(canonical);
      if (field != null) {
        return field;
      }
    }
    int index = column - 1;
    if (physicalNames != null && index >= 0 && index < physicalNames.size()) {
      String physical = physicalNames.get(index);
      if (physical != null) {
        Schema.Field field = schema.getField(physical);
        if (field != null) {
          return field;
        }
      }
    }
    return schema.getField(getColumnLabel(column));
  }

  private String canonicalName(int column) {
    int index = column - 1;
    if (index < 0) {
      return null;
    }
    if (canonicalNames != null && index < canonicalNames.size()) {
      return canonicalNames.get(index);
    }
    if (physicalNames != null && index < physicalNames.size()) {
      return physicalNames.get(index);
    }
    if (index < labels.size()) {
      return labels.get(index);
    }
    return null;
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
    if (expressions == null || expressions.isEmpty()) {
      return Types.OTHER;
    }
    int index = column - 1;
    if (index < 0 || index >= expressions.size()) {
      return Types.OTHER;
    }
    Expression expression = expressions.get(index);
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

  private Schema.Field lookupFieldByName(String name) {
    if (schema == null || name == null) {
      return null;
    }
    Schema.Field direct = schema.getField(name);
    if (direct != null) {
      return direct;
    }
    if (canonicalNames != null) {
      for (int i = 0; i < canonicalNames.size(); i++) {
        String canonical = canonicalNames.get(i);
        if (canonical != null && canonical.equalsIgnoreCase(name)) {
          Schema.Field field = schema.getField(canonical);
          if (field != null) {
            return field;
          }
        }
      }
    }
    for (int i = 0; i < labels.size(); i++) {
      String label = labels.get(i);
      if (label != null && label.equalsIgnoreCase(name)) {
        String canonical = canonicalName(i + 1);
        if (canonical != null) {
          Schema.Field field = schema.getField(canonical);
          if (field != null) {
            return field;
          }
        }
      }
    }
    return null;
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
