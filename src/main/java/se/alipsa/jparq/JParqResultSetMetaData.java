package se.alipsa.jparq;

import java.sql.Types;
import java.util.List;
import java.util.Locale;
import net.sf.jsqlparser.expression.AnalyticExpression;
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
   * @param tableName
   *          the table name
   * @param expressions
   *          parsed SELECT-list expressions corresponding to {@code labels}
   */
  public JParqResultSetMetaData(Schema schema, List<String> labels, List<String> physicalNames, String tableName,
      List<Expression> expressions) {
    this.schema = schema;
    this.labels = labels;
    this.physicalNames = physicalNames;
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
    int i = column - 1;
    if (physicalNames != null && i < physicalNames.size()) {
      String phys = physicalNames.get(i);
      if (phys != null && !phys.isBlank()) {
        return phys; // prefer physical name when known
      }
    }
    return getColumnLabel(column); // fallback
  }

  @Override
  public String getTableName(int column) {
    return tableName;
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
   * Resolve the Avro schema field associated with the supplied column index. The
   * physical column name is preferred, falling back to the projected label when
   * the column originates from an expression.
   *
   * @param column
   *          the 1-based column index from the result set
   * @return the matching {@link Schema.Field}, or {@code null} when the column is
   *         computed
   */
  private Schema.Field resolveField(int column) {
    // Prefer physical name for schema lookup; fall back to label if needed
    String name = getColumnName(column);
    Schema.Field field = schema.getField(name);
    if (field != null) {
      return field;
    }
    return schema.getField(getColumnLabel(column));
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
    Schema.Field baseField = schema.getField(columnName);
    if (baseField == null) {
      return Types.OTHER;
    }
    return mapSchemaToJdbcType(nonNullSchema(baseField.schema()));
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
      case BYTES, FIXED -> (base.getLogicalType() instanceof LogicalTypes.Decimal ? Types.DECIMAL : Types.BINARY);
      case RECORD -> Types.STRUCT;
      case ARRAY -> Types.ARRAY;
      default -> Types.OTHER;
    };
  }
}
