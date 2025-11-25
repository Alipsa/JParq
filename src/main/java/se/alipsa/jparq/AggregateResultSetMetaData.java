package se.alipsa.jparq;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
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
  private final String tableSchema;
  private final String tableCatalog;

  /**
   * Create metadata for an aggregate result set.
   *
   * @param labels
   *          column labels exposed to JDBC callers
   * @param sqlTypes
   *          SQL types corresponding to each column label
   * @param tableName
   *          virtual table name reported to JDBC clients
   * @param tableSchema
   *          schema reported to JDBC clients (may be {@code null})
   * @param tableCatalog
   *          catalog reported to JDBC clients (may be {@code null})
   */
  public AggregateResultSetMetaData(List<String> labels, List<Integer> sqlTypes, String tableName, String tableSchema,
      String tableCatalog) {
    this.labels = new ArrayList<>(labels);
    this.sqlTypes = new ArrayList<>(sqlTypes);
    this.tableName = tableName;
    this.tableSchema = tableSchema;
    this.tableCatalog = tableCatalog;
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
  public String getSchemaName(int column) {
    return tableSchema == null ? "" : tableSchema;
  }

  @Override
  public String getCatalogName(int column) {
    return tableCatalog == null ? "" : tableCatalog;
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
