package se.alipsa.jparq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import se.alipsa.jparq.engine.AvroCoercions;
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
  private boolean lastWasNull = false;

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
  public JParqResultSet(ParquetReader<GenericRecord> reader, SqlParser.Select select, String tableName,
      net.sf.jsqlparser.expression.Expression residual) throws SQLException {
    this.tableName = tableName;
    try {
      GenericRecord first = reader.read();
      if (first == null) {
        // No rows produced by the reader (after pushdown). Still expose metadata
        // based on the SELECT list when it is explicit (not "*").
        List<String> req = select.columns(); // e.g., ["id","value"] or ["*"]
        if (!req.isEmpty() && !req.contains("*")) {
          columnOrder.addAll(req);
        }
        this.qp = new QueryProcessor(reader, columnOrder, /* where */ residual, select.limit(), null, 0);
        this.current = null;
        this.rowNum = 0;
        return;
      }

      var schema = first.getSchema();
      var evaluator = new se.alipsa.jparq.engine.ExpressionEvaluator(schema);

      boolean match = (residual == null) || evaluator.eval(residual, first);

      List<String> proj = QueryProcessor.computeProjection(select.columns(), schema);
      columnOrder.addAll(proj);

      var order = select.orderBy();
      if (order == null || order.isEmpty()) {
        int initialEmitted = match ? 1 : 0;
        this.qp = new QueryProcessor(reader, proj, residual, select.limit(), schema, initialEmitted);
        this.current = match ? first : qp.nextMatching();
      } else {
        this.qp = new QueryProcessor(reader, proj, residual, select.limit(), schema, 0, order, first);
        this.current = qp.nextMatching();
      }
      this.rowNum = 0;
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
    if (field == null) {
      throw new SQLException("Unknown column in current schema: " + name);
    }
    Object raw = AvroCoercions.unwrap(current.get(name), field.schema());
    lastWasNull = (raw == null);
    return raw;
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

  @SuppressWarnings("PMD.EmptyCatchBlock")
  @Override
  public void close() throws SQLException {
    closed = true;
    try {
      qp.close();
    } catch (Exception ignore) {
      // intentionally ignored
    }
  }

  @Override
  public boolean wasNull() {
    return lastWasNull;
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
    if (v == null) {
      return null;
    }
    if (v instanceof BigDecimal bd) {
      return bd;
    }
    if (v instanceof Number n) {
      return new BigDecimal(n.toString());
    }
    return new BigDecimal(v.toString());
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return (byte[]) v;
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    if (v == null) {
      return null;
    }
    if (v instanceof Date) {
      return (Date) v;
    }
    if (v instanceof Timestamp ts) {
      return new Date(ts.getTime());
    }
    if (v instanceof String s) {
      return Date.valueOf(s);
    }
    if (v instanceof Long l) {
      return new Date(l);
    }
    if (v instanceof Double d) {
      return new Date(d.longValue());
    }
    if (v instanceof LocalDateTime l) {
      return Date.valueOf(l.toLocalDate());
    }
    if (v instanceof LocalDate) {
      return Date.valueOf((LocalDate) v);
    }
    throw new SQLException("Unsupported date type: " + v.getClass().getName());
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
    if (v == null) {
      return null;
    }
    if (v instanceof Timestamp t) {
      return t;
    }
    if (v instanceof Date d) {
      return new Timestamp(d.getTime());
    }
    if (v instanceof String s) {
      return Timestamp.valueOf(s);
    }
    if (v instanceof Long l) {
      return new Timestamp(l);
    }
    if (v instanceof Double d) {
      return new Timestamp(d.longValue());
    }
    if (v instanceof LocalDateTime l) {
      return Timestamp.valueOf(l);
    }
    throw new SQLException("Unsupported timestamp type: " + v.getClass().getName());
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
  public int getRow() {
    return rowNum;
  }

  @Override
  public int getFetchDirection() {
    return FETCH_FORWARD;
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
