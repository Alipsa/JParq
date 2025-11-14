package se.alipsa.jparq.model;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;

/** Very small in-memory ResultSet implementation for metadata tables. */
public class MetaDataResultSet extends ResultSetAdapter {
  private int idx = -1;
  private final String[] headers;
  private final List<Object[]> rows;
  private boolean lastWasNull;

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
    Object value = getByLabel(columnLabel);
    return value == null ? null : value.toString();
  }

  @Override
  public String getString(int columnIndex) {
    Object value = getByIndex(columnIndex);
    return value == null ? null : value.toString();
  }

  @Override
  public Object getObject(String columnLabel) {
    return getByLabel(columnLabel);
  }

  @Override
  public Object getObject(int columnIndex) {
    return getByIndex(columnIndex);
  }

  private Object getByLabel(String label) {
    for (int i = 0; i < headers.length; i++) {
      if (headers[i].equalsIgnoreCase(label)) {
        return recordAccess(rows.get(idx)[i]);
      }
    }
    return recordAccess(null);
  }

  private Object getByIndex(int columnIndex) {
    if (idx < 0 || idx >= rows.size()) {
      return recordAccess(null);
    }
    Object[] row = rows.get(idx);
    if (columnIndex <= 0 || columnIndex > row.length) {
      return recordAccess(null);
    }
    return recordAccess(row[columnIndex - 1]);
  }

  private Object recordAccess(Object value) {
    lastWasNull = value == null;
    return value;
  }

  @Override
  public int getInt(String columnLabel) {
    return toInt(getByLabel(columnLabel));
  }

  @Override
  public int getInt(int columnIndex) {
    return toInt(getByIndex(columnIndex));
  }

  private int toInt(Object value) {
    if (value == null) {
      return 0;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value instanceof String str && !str.isEmpty()) {
      return Integer.parseInt(str);
    }
    throw new NumberFormatException("Cannot convert value to int: " + value);
  }

  @Override
  public long getLong(String columnLabel) {
    return toLong(getByLabel(columnLabel));
  }

  @Override
  public long getLong(int columnIndex) {
    return toLong(getByIndex(columnIndex));
  }

  private long toLong(Object value) {
    if (value == null) {
      return 0L;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String str && !str.isEmpty()) {
      return Long.parseLong(str);
    }
    throw new NumberFormatException("Cannot convert value to long: " + value);
  }

  @Override
  public boolean wasNull() {
    return lastWasNull;
  }

  @Override
  public ResultSetMetaData getMetaData() {
    return new ResultSetMetaData() {
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
        return columnNullableUnknown;
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
