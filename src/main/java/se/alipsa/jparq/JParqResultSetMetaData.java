package se.alipsa.jparq;

import java.sql.Types;
import java.util.List;
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
  private final List<net.sf.jsqlparser.expression.Expression> expressions;

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
      List<net.sf.jsqlparser.expression.Expression> expressions) {
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
    // Prefer physical name for schema lookup; fall back to label if needed
    String name = getColumnName(column);
    Schema.Field f = schema.getField(name);
    if (f == null) {
      f = schema.getField(getColumnLabel(column));
    }
    if (f == null) {
      if (expressions != null) {
        int index = column - 1;
        if (index >= 0 && index < expressions.size()) {
          net.sf.jsqlparser.expression.Expression expression = expressions.get(index);
          if (expression instanceof net.sf.jsqlparser.expression.AnalyticExpression analytic) {
            String functionName = analytic.getName();
            if (functionName != null && "ROW_NUMBER".equalsIgnoreCase(functionName)) {
              return Types.BIGINT;
            }
          }
        }
      }
      return Types.OTHER;
    }
    Schema s = f.schema().getType() == Schema.Type.UNION
        ? f.schema().getTypes().stream().filter(t -> t.getType() != Schema.Type.NULL).findFirst().orElse(f.schema())
        : f.schema();
    return switch (s.getType()) {
      case STRING, ENUM -> Types.VARCHAR;
      case INT -> (LogicalTypes.date().equals(s.getLogicalType()) ? Types.DATE : Types.INTEGER);
      case LONG -> (s.getLogicalType() instanceof LogicalTypes.TimestampMillis
          || s.getLogicalType() instanceof LogicalTypes.TimestampMicros ? Types.TIMESTAMP : Types.BIGINT);
      case FLOAT -> Types.REAL;
      case DOUBLE -> Types.DOUBLE;
      case BOOLEAN -> Types.BOOLEAN;
      case BYTES, FIXED -> (s.getLogicalType() instanceof LogicalTypes.Decimal ? Types.DECIMAL : Types.BINARY);
      case RECORD -> Types.STRUCT;
      case ARRAY -> Types.ARRAY;
      default -> Types.OTHER;
    };
  }

  @Override
  public String getColumnTypeName(int column) {
    return java.sql.JDBCType.valueOf(getColumnType(column)).getName();
  }

  @Override
  public int isNullable(int column) {
    return columnNullableUnknown;
  }
}
