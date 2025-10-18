package se.alipsa.jparq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.QueryProcessor;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.model.ResultSetAdapter;

/** An implementation of the java.sql.ResultSet interface. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqResultSet extends ResultSetAdapter {

  private final QueryProcessor qp;
  private GenericRecord current;
  private final List<String> columnOrder = new ArrayList<>();
  private final String tableName;
  private boolean closed = false;
  private int rowNum = 0;

  /**
   * Constructor for JParqResultSet.
   *
   * @param reader
   *          ParquetReader to read from
   * @param select
   *          the parsed select statement
   * @param tableName
   *          the name of the table
   * @throws SQLException
   *           if reading fails
   */
  public JParqResultSet(ParquetReader<GenericRecord> reader, SqlParser.Select select, String tableName)
      throws SQLException {
    this.tableName = tableName;
    try {
      // prime first matching row to discover schema, then compute projection
      GenericRecord first = reader.read();
      if (first == null) {
        this.qp = new QueryProcessor(reader, List.of(), select.where(), select.limit(), null, 0);
        this.current = null;
        return;
      }
      var schema = first.getSchema();
      var tempEvaluator = new ExpressionEvaluator(schema);
      boolean match = (select.where() == null) || tempEvaluator.eval(select.where(), first);

      List<String> proj = QueryProcessor.computeProjection(select.columns(), schema);
      // If first matched, count it toward LIMIT by starting emitted at 1.
      // If not, start at 0 and let the processor find the first match.
      int initialEmitted = match ? 1 : 0;
      this.qp = new QueryProcessor(reader, proj, select.where(), select.limit(), schema, initialEmitted);

      columnOrder.addAll(proj);
      this.current = match ? first : qp.nextMatching();
    } catch (Exception e) {
      throw new SQLException("Failed reading first parquet record", e);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (closed) {
      throw new SQLException("ResultSet closed");
    }
    try {
      if (rowNum == 0) {
        if (current == null) {
          return false;
        }
        rowNum = 1;
        return true;
      }
      current = qp.nextMatching();
      if (current == null) {
        return false;
      }
      rowNum++;
      return true;
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  // --- getters use AvroCoercions.unwrap like before ---
  private Object value(int idx) throws SQLException {
    if (current == null) {
      throw new SQLException("Call next() before getting values");
    }
    String name = columnOrder.get(idx - 1);
    var field = current.getSchema().getField(name);
    return AvroCoercions.unwrap(current.get(name), field.schema());
  }

  @Override
  public int findColumn(String label) throws SQLException {
    for (int i = 0; i < columnOrder.size(); i++) {
      if (columnOrder.get(i).equals(label)) {
        return i + 1;
      }
    }
    throw new SQLException("Unknown column: " + label);
  }

  @Override
  public ResultSetMetaData getMetaData() {
    var schema = (current == null) ? null : current.getSchema();
    return new JParqResultSetMetaData(schema, columnOrder, tableName);
  }

  @Override
  public void close() throws SQLException {
    closed = true;
    try {
      qp.close();
    } catch (Exception ignore) {
      // intentionally ignored:
    }
  }

  @Override
  public boolean wasNull() {
    return false;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? null : v.toString();
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v != null && ((Boolean) v);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0 : ((Number) v).byteValue();
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0 : ((Number) v).shortValue();
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0 : ((Number) v).intValue();
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0 : ((Number) v).longValue();
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0f : ((Number) v).floatValue();
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0d : ((Number) v).doubleValue();
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return (BigDecimal) v;
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return (byte[]) v;
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return (Date) v;
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    if (v instanceof Timestamp) {
      return new Time(((Timestamp) v).getTime());
    }
    return (Time) v;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return (Timestamp) v;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return value(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public boolean isBeforeFirst() {
    return rowNum == 0;
  }

  @Override
  public boolean isFirst() {
    return rowNum == 1;
  }

  @Override
  public boolean isLast() {
    return false;
  }

  @Override
  public void beforeFirst() {
  }

  @Override
  public void afterLast() {
  }

  @Override
  public boolean first() {
    return false;
  }

  @Override
  public boolean last() {
    return false;
  }

  @Override
  public int getRow() {
    return rowNum;
  }

  @Override
  public boolean absolute(int row) {
    return false;
  }

  @Override
  public boolean relative(int rows) {
    return false;
  }

  @Override
  public boolean previous() {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) {
  }

  @Override
  public int getFetchDirection() {
    return FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) {
  }

  @Override
  public int getFetchSize() {
    return 0;
  }

  @Override
  public int getType() {
    return TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() {
    return CONCUR_READ_ONLY;
  }
}
