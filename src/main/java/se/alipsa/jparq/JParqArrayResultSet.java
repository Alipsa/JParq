package se.alipsa.jparq;

import java.sql.Array;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.apache.avro.Schema;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/** A detached, read-only cursor over an array's indexes and element values. */
final class JParqArrayResultSet extends JParqResultSet {
  private final ResultSetMetaData metadata;
  private final Schema elementSchema;

  /**
   * Creates a cursor with the array's declared element metadata.
   *
   * @param rows
   *          index and value pairs in array order
   * @param metadata
   *          column types and nullability
   * @param elementSchema
   *          declared element schema, possibly null
   */
  JParqArrayResultSet(List<List<Object>> rows, ResultSetMetaData metadata, Schema elementSchema) {
    super("ARRAY", null, null, List.of("INDEX", "VALUE"), List.of(Types.BIGINT, Types.OTHER), rows);
    this.metadata = metadata;
    this.elementSchema = elementSchema;
  }

  /** {@inheritDoc} */
  @Override
  public ResultSetMetaData getMetaData() {
    return metadata;
  }

  /** {@inheritDoc} */
  @Override
  public Array getArray(int columnIndex) throws SQLException {
    Object value = getObject(columnIndex);
    if (value instanceof Object[] nested) {
      Schema declaration = JdbcTypeMapper.nonNullSchema(elementSchema);
      return new JParqArray(nested,
          declaration != null && declaration.getType() == Schema.Type.ARRAY ? declaration.getElementType() : null);
    }
    return super.getArray(columnIndex);
  }
}
