package se.alipsa.jparq;

import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.rowset.RowSetMetaDataImpl;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
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
    ensureOpen();
    Schema declaration = JdbcTypeMapper.nonNullSchema(elementSchema);
    if (declaration != null && declaration.getType() == Schema.Type.RECORD) {
      return declaration.getFullName();
    }
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
    metadata.setNullable(1, ResultSetMetaData.columnNoNulls);
    metadata.setColumnName(2, "VALUE");
    metadata.setColumnLabel(2, "VALUE");
    metadata.setColumnType(2, baseType);
    metadata.setColumnTypeName(2, getBaseTypeName());
    metadata.setNullable(2, ResultSetMetaData.columnNullable);
    List<List<Object>> rows = new ArrayList<>(slice.length);
    for (int i = 0; i < slice.length; i++) {
      // ArrayList appends in constant time and permits nullable element values.
      rows.add(Arrays.asList(index + i, slice[i]));
    }
    return new JParqArrayResultSet(rows, metadata, elementSchema);
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
    rejectUnsupportedTypeMap(map, baseType, getBaseTypeName(), JdbcTypeMapper.nonNullSchema(elementSchema));
  }

  /**
   * Uses the standard mapping unless the map requests a custom STRUCT mapping,
   * which is not supported.
   *
   * @param map
   *          optional custom mappings, null means the standard mapping
   * @param sqlType
   *          JDBC type of the value
   * @param typeName
   *          SQL type name of the value
   * @param declaration
   *          optional Avro schema declaration of the value
   * @throws SQLFeatureNotSupportedException
   *           if the map names the STRUCT type being read
   */
  static void rejectUnsupportedTypeMap(Map<String, Class<?>> map, int sqlType, String typeName, Schema declaration)
      throws SQLException {
    if (sqlType == Types.STRUCT && map != null
        && (map.containsKey(typeName) || declaration != null && map.containsKey(declaration.getFullName()))) {
      throw new SQLFeatureNotSupportedException("Custom type mappings are not supported", "0A000");
    }
  }

  /**
   * Copies decoded mutable values recursively so callers cannot change retained
   * elements.
   *
   * @param values
   *          source array
   * @return independent decoded values
   */
  private static Object[] copy(Object[] values) {
    Object[] result = values.clone();
    for (int i = 0; i < result.length; i++) {
      result[i] = copyValue(result[i]);
    }
    return result;
  }

  /**
   * Copies mutable decoded ARRAY, MAP, STRUCT, binary, and datetime values.
   *
   * @param value
   *          decoded value
   * @return independent value, or the original immutable value
   */
  private static Object copyValue(Object value) {
    if (value instanceof Object[] nested) {
      return copy(nested);
    }
    if (value instanceof byte[] bytes) {
      return bytes.clone();
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer duplicate = buffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return ByteBuffer.wrap(bytes);
    }
    if (value instanceof GenericFixed fixed) {
      return new GenericData.Fixed(fixed.getSchema(), fixed.bytes().clone());
    }
    if (value instanceof GenericRecord record) {
      GenericRecord result = new GenericData.Record(record.getSchema());
      for (Schema.Field field : record.getSchema().getFields()) {
        result.put(field.pos(), copyValue(record.get(field.pos())));
      }
      return result;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> result = new LinkedHashMap<>();
      map.forEach((key, item) -> result.put(copyValue(key), copyValue(item)));
      return result;
    }
    if (value instanceof List<?> list) {
      List<Object> result = new ArrayList<>(list.size());
      list.forEach(item -> result.add(copyValue(item)));
      return result;
    }
    if (value instanceof java.util.Date date) {
      return date.clone();
    }
    if (value instanceof org.apache.avro.util.Utf8 text) {
      return new org.apache.avro.util.Utf8(text);
    }
    return value;
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
