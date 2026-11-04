package se.alipsa.jparq;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalValueType;

/**
 * A detached, in-memory JDBC array with one-based slicing and element cursors.
 */
public final class JParqArray implements Array {
  private Object[] elements;
  private final Schema elementSchema;
  private final int baseType;

  /**
   * Copies decoded array elements, retaining their declared element type when
   * available.
   *
   * @param elements
   *          decoded elements, including nulls and nested Object arrays
   * @param elementSchema
   *          declared Avro element schema, or null for value-based inference
   */
  public JParqArray(Object[] elements, Schema elementSchema) {
    this.elements = copy(elements);
    this.elementSchema = elementSchema;
    baseType = elementSchema == null ? inferType(elements) : JdbcTypeMapper.mapSchemaToJdbcType(elementSchema);
  }

  /** {@inheritDoc} */
  @Override
  public String getBaseTypeName() throws SQLException {
    return JDBCType.valueOf(getBaseType()).getName();
  }

  /** {@inheritDoc} */
  @Override
  public int getBaseType() throws SQLException {
    ensureOpen();
    return baseType;
  }

  /** {@inheritDoc} */
  @Override
  public Object getArray() throws SQLException {
    ensureOpen();
    return copy(elements);
  }

  /** {@inheritDoc} */
  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    checkMapping(map);
    return getArray();
  }

  /** {@inheritDoc} */
  @Override
  public Object getArray(long index, int count) throws SQLException {
    int start = sliceStart(index, count);
    return copy(Arrays.copyOfRange(elements, start, start + Math.min(count, elements.length - start)));
  }

  /** {@inheritDoc} */
  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    checkMapping(map);
    return getArray(index, count);
  }

  /** {@inheritDoc} */
  @Override
  public ResultSet getResultSet() throws SQLException {
    ensureOpen();
    return getResultSet(1, elements.length);
  }

  /** {@inheritDoc} */
  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    checkMapping(map);
    return getResultSet();
  }

  /** {@inheritDoc} */
  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    final Object[] slice = (Object[]) getArray(index, count);
    var metadata = new RowSetMetaDataImpl();
    metadata.setColumnCount(2);
    metadata.setColumnName(1, "INDEX");
    metadata.setColumnLabel(1, "INDEX");
    metadata.setColumnType(1, Types.BIGINT);
    metadata.setColumnTypeName(1, "BIGINT");
    metadata.setColumnName(2, "VALUE");
    metadata.setColumnLabel(2, "VALUE");
    metadata.setColumnType(2, baseType);
    metadata.setColumnTypeName(2, getBaseTypeName());
    var rows = RowSetProvider.newFactory().createCachedRowSet();
    try {
      rows.setMetaData(metadata);
      // CachedRowSet inserts before its current row, so build in reverse order.
      for (int i = slice.length - 1; i >= 0; i--) {
        rows.moveToInsertRow();
        rows.updateLong(1, index + i);
        Object value = slice[i];
        if (value instanceof Object[] nested) {
          Schema declaration = JdbcTypeMapper.nonNullSchema(elementSchema);
          value = new JParqArray(nested,
              declaration != null && declaration.getType() == Schema.Type.ARRAY ? declaration.getElementType() : null);
        }
        rows.updateObject(2, value);
        rows.insertRow();
        rows.moveToCurrentRow();
      }
      rows.beforeFirst();
      return rows;
    } catch (SQLException exception) {
      rows.close();
      throw exception;
    }
  }

  /** {@inheritDoc} */
  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
    checkMapping(map);
    return getResultSet(index, count);
  }

  /** {@inheritDoc} */
  @Override
  public void free() {
    elements = null;
  }

  /**
   * Rejects access after free.
   *
   * @throws SQLException
   *           if the array has been freed
   */
  private void ensureOpen() throws SQLException {
    if (elements == null) {
      throw new SQLException("Array has been freed", "HY010");
    }
  }

  /**
   * Validates one-based slice bounds without narrowing or overflowing the index.
   *
   * @param index
   *          first requested index
   * @param count
   *          maximum number of elements
   * @return zero-based starting position
   * @throws SQLException
   *           if the array was freed or the slice is invalid
   */
  private int sliceStart(long index, int count) throws SQLException {
    ensureOpen();
    if (index < 1 || index > (long) elements.length + 1 || count < 0) {
      throw new SQLException("Invalid ARRAY slice: index=" + index + ", count=" + count, "2202E");
    }
    return (int) (index - 1);
  }

  /**
   * Uses standard mappings unless a caller requests unsupported custom struct
   * mapping.
   *
   * @param map
   *          optional custom mappings
   * @throws SQLException
   *           if freed or a matching custom mapping is requested
   */
  private void checkMapping(Map<String, Class<?>> map) throws SQLException {
    ensureOpen();
    Schema declaration = JdbcTypeMapper.nonNullSchema(elementSchema);
    if (baseType == Types.STRUCT && map != null
        && (map.containsKey(getBaseTypeName()) || declaration != null && map.containsKey(declaration.getFullName()))) {
      throw new SQLFeatureNotSupportedException("Custom ARRAY element mappings are not supported", "0A000");
    }
  }

  /**
   * Copies array containers recursively so callers cannot replace retained
   * elements.
   *
   * @param values
   *          source array
   * @return independent array containers
   */
  private static Object[] copy(Object[] values) {
    Object[] result = values.clone();
    for (int i = 0; i < result.length; i++) {
      if (result[i] instanceof Object[] nested) {
        result[i] = copy(nested);
      }
    }
    return result;
  }

  /**
   * Infers homogeneous computed element types, retaining OTHER for unknown or
   * mixed values.
   *
   * @param values
   *          decoded elements
   * @return JDBC element type
   */
  private static int inferType(Object[] values) {
    Integer type = null;
    for (Object value : values) {
      if (value != null) {
        TemporalValueType temporal = TemporalValueType.fromValue(value);
        int candidate = temporal != null
            ? temporal.jdbcType()
            : value instanceof GenericRecord
                ? Types.STRUCT
                : JdbcTypeMapper.mapJavaClassNameToJdbcType(value.getClass().getName());
        if (type != null && type != candidate) {
          return Types.OTHER;
        }
        type = candidate;
      }
    }
    return type == null ? Types.OTHER : type;
  }
}
