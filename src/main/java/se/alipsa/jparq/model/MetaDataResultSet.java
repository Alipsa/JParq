package se.alipsa.jparq.model;

import java.sql.Types;
import java.util.List;

/** Very small in-memory ResultSet implementation for metadata tables. */
public class MetaDataResultSet extends ResultSetAdapter {
  int idx = -1;
  String[] headers;
  List<Object[]> rows;

  /**
   * Constructor for MetaDataResultSet.
   *
   * @param headers
   *          the column headers
   * @param rows
   *          the data rows
   */
  public MetaDataResultSet(String[] headers, List<Object[]> rows) {
    this.headers = headers;
    this.rows = rows;
  }

  @Override
  public boolean next() {
    idx++;
    return idx < rows.size();
  }

  @Override
  public String getString(String columnLabel) {
    return String.valueOf(getByLabel(columnLabel));
  }

  @Override
  public String getString(int columnIndex) {
    return String.valueOf(rows.get(idx)[columnIndex - 1]);
  }

  @Override
  public Object getObject(String columnLabel) {
    return getByLabel(columnLabel);
  }

  private Object getByLabel(String label) {
    for (int i = 0; i < headers.length; i++) {
      if (headers[i].equalsIgnoreCase(label)) {
        return rows.get(idx)[i];
      }
    }
    return null;
  }

  @Override
  public java.sql.ResultSetMetaData getMetaData() {
    return new java.sql.ResultSetMetaData() {
      @Override
      public int getColumnCount() {
        return headers.length;
      }

      @Override
      public String getColumnLabel(int column) {
        return headers[column - 1];
      }

      @Override
      public String getColumnName(int column) {
        return headers[column - 1];
      }

      @Override
      public int getColumnType(int column) {
        return Types.VARCHAR;
      }

      @Override
      public String getColumnTypeName(int column) {
        return "VARCHAR";
      }

      @Override
      public <T> T unwrap(Class<T> iface) {
        return null;
      }

      @Override
      public boolean isWrapperFor(Class<?> iface) {
        return false;
      }

      @Override
      public String getCatalogName(int column) {
        return "";
      }

      @Override
      public String getColumnClassName(int column) {
        return String.class.getName();
      }

      @Override
      public int getColumnDisplaySize(int column) {
        return 0;
      }

      @Override
      public String getSchemaName(int column) {
        return "";
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
      public boolean isAutoIncrement(int column) {
        return false;
      }

      @Override
      public boolean isCaseSensitive(int column) {
        return true;
      }

      @Override
      public boolean isCurrency(int column) {
        return false;
      }

      @Override
      public boolean isDefinitelyWritable(int column) {
        return false;
      }

      @Override
      public int isNullable(int column) {
        return java.sql.ResultSetMetaData.columnNullableUnknown;
      }

      @Override
      public boolean isReadOnly(int column) {
        return true;
      }

      @Override
      public boolean isSearchable(int column) {
        return true;
      }

      @Override
      public boolean isSigned(int column) {
        return false;
      }

      @Override
      public boolean isWritable(int column) {
        return false;
      }

      @Override
      public String getTableName(int column) {
        return "";
      }
    };
  }

  @Override
  public int findColumn(String columnLabel) {
    for (int i = 0; i < headers.length; i++) {
      if (headers[i].equalsIgnoreCase(columnLabel)) {
        return i + 1;
      }
    }
    return -1;
  }
}
