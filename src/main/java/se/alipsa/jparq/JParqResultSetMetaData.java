package se.alipsa.jparq;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/** An implementation of the java.sql.ResultSetMetaData interface. */
public class JParqResultSetMetaData implements ResultSetMetaData {

  private final Schema schema; // may be null if empty file
  private final List<String> columns;
  private final String tableName;

  public JParqResultSetMetaData(Schema schema, List<String> columns, String tableName) {
    this.schema = schema;
    this.columns = columns;
    this.tableName = tableName;
  }

  @Override
  public int getColumnCount() {
    return columns.size();
  }

  @Override
  public String getColumnLabel(int column) {
    return columns.get(column - 1);
  }

  @Override
  public String getColumnName(int column) {
    return getColumnLabel(column);
  }

  @Override
  public String getTableName(int column) {
    return tableName;
  }

  @Override
  public int getColumnType(int column) {
    if (schema == null) return Types.OTHER;
    Schema.Field f = schema.getField(getColumnLabel(column));
    Schema s =
        f.schema().getType() == Schema.Type.UNION
            ? f.schema().getTypes().stream()
                .filter(t -> t.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(f.schema())
            : f.schema();
    return switch (s.getType()) {
      case STRING, ENUM -> Types.VARCHAR;
      case INT -> (LogicalTypes.date().equals(s.getLogicalType()) ? Types.DATE : Types.INTEGER);
      case LONG ->
          (s.getLogicalType() instanceof LogicalTypes.TimestampMillis
                  || s.getLogicalType() instanceof LogicalTypes.TimestampMicros
              ? Types.TIMESTAMP
              : Types.BIGINT);
      case FLOAT -> Types.REAL;
      case DOUBLE -> Types.DOUBLE;
      case BOOLEAN -> Types.BOOLEAN;
      case BYTES, FIXED ->
          (s.getLogicalType() instanceof LogicalTypes.Decimal ? Types.DECIMAL : Types.BINARY);
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
  public int getPrecision(int column) {
    return 0;
  }

  @Override
  public int getScale(int column) {
    return 0;
  }

  @Override
  public int isNullable(int column) {
    return columnNullableUnknown;
  }

  // Defaults
  @Override
  public String getCatalogName(int column) {
    return "";
  }

  @Override
  public String getSchemaName(int column) {
    return "";
  }

  @Override
  public boolean isAutoIncrement(int column) {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) {
    return true;
  }

  @Override
  public boolean isSearchable(int column) {
    return true;
  }

  @Override
  public boolean isCurrency(int column) {
    return false;
  }

  @Override
  public boolean isSigned(int column) {
    return true;
  }

  @Override
  public int getColumnDisplaySize(int column) {
    return 0;
  }

  @Override
  public boolean isReadOnly(int column) {
    return true;
  }

  @Override
  public boolean isWritable(int column) {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) {
    return false;
  }

  @Override
  public String getColumnClassName(int column) {
    return Object.class.getName();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
