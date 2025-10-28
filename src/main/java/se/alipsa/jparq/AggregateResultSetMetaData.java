package se.alipsa.jparq;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.List;
import se.alipsa.jparq.model.ResultSetMetaDataAdapter;

/**
 * Simple metadata for aggregate result sets where the values do not originate
 * from the underlying Avro schema.
 */
public class AggregateResultSetMetaData extends ResultSetMetaDataAdapter {

  private final List<String> labels;
  private final List<Integer> sqlTypes;
  private final String tableName;

  /**
   * Create metadata for an aggregate result set.
   *
   * @param labels column labels exposed to JDBC callers
   * @param sqlTypes SQL types corresponding to each column label
   * @param tableName virtual table name reported to JDBC clients
   */
  public AggregateResultSetMetaData(List<String> labels, List<Integer> sqlTypes, String tableName) {
    this.labels = List.copyOf(labels);
    this.sqlTypes = List.copyOf(sqlTypes);
    this.tableName = tableName;
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
    return getColumnLabel(column);
  }

  @Override
  public String getTableName(int column) {
    return tableName;
  }

  @Override
  public int getColumnType(int column) {
    if (sqlTypes.isEmpty()) {
      return Types.OTHER;
    }
    int idx = column - 1;
    if (idx < 0 || idx >= sqlTypes.size()) {
      return Types.OTHER;
    }
    Integer type = sqlTypes.get(idx);
    return type == null ? Types.OTHER : type;
  }

  @Override
  public String getColumnTypeName(int column) {
    return JDBCType.valueOf(getColumnType(column)).getName();
  }

  @Override
  public int isNullable(int column) {
    return columnNullableUnknown;
  }
}

