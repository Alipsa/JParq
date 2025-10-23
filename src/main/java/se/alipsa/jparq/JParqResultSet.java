package se.alipsa.jparq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import se.alipsa.jparq.engine.QueryProcessor;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.model.ResultSetAdapter;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqResultSet extends ResultSetAdapter {

  private final List<String> physicalColumnOrder; // may be null
  private final QueryProcessor qp;
  private final List<String> columnOrder; // output labels
  private final String tableName;
  private boolean closed = false;
  private int rowNum = 0;
  private boolean lastWasNull = false;

  // Result metadata schema (may be null)
  private final Schema schema;

  // Current projected row
  private Object[] currentRow;

  public JParqResultSet(ParquetReader<GenericRecord> reader, SqlParser.Select select, String tableName,
      Expression residualWhere, List<String> selectLabels, // kept for signature compatibility; not used
      List<String> physicalColumnOrder) throws SQLException {
    this.tableName = tableName;
    this.physicalColumnOrder = physicalColumnOrder;

    try {
      // Prefetch one record to detect schema and avoid losing it
      GenericRecord first = reader.read();
      Schema detectedSchema = (first != null) ? first.getSchema() : null;

      // Source columns from the parsed select (e.g., ["model", "mpg"] or ["*"])
      List<String> sourceColumns = select.columns();

      // Effective output labels (aliases visible to JDBC); fall back to source names
      // if no aliases
      List<String> labels = select.labels();
      if (labels == null || labels.isEmpty()) {
        labels = sourceColumns;
      }

      // Prefer residual pushdown, but fall back to select WHERE when residual is null
      Expression whereExpr = (residualWhere != null) ? residualWhere : select.where();

      // Build processor; it will expand '\*' as soon as schema is known
      this.qp = new QueryProcessor(reader, sourceColumns, labels, // pass effective output labels
          whereExpr, select.limit(), detectedSchema, 0, select.orderBy(), select.distinct(), first);

      // Expose the processor's final projection (handles aliases and '\*' expansion)
      this.columnOrder = qp.projection();
      this.schema = (detectedSchema != null) ? detectedSchema : (first != null ? first.getSchema() : null);
    } catch (Exception e) {
      throw new SQLException("Failed initializing result set", e);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (closed) {
      throw new SQLException("ResultSet closed");
    }
    try {
      currentRow = qp.nextRow();
      if (currentRow == null) {
        return false;
      }
      rowNum++;
      return true;
    } catch (Exception e) {
      throw new SQLException("Failed reading next row", e);
    }
  }

  private Object value(int idx) throws SQLException {
    if (currentRow == null) {
      throw new SQLException("Call next() before getting values");
    }
    if (idx < 1 || idx > currentRow.length) {
      throw new SQLException("Column index out of range: " + idx);
    }
    Object v = currentRow[idx - 1];
    lastWasNull = (v == null);
    return v;
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
    if (physicalColumnOrder != null) {
      return new JParqResultSetMetaData(schema, columnOrder, physicalColumnOrder, tableName);
    }
    return new JParqResultSetMetaData(schema, columnOrder, tableName);
  }

  @Override
  public void close() throws SQLException {
    closed = true;
    try {
      qp.close();
    } catch (Exception ignore) {
      // ignore
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
    return v == null ? (byte) 0 : ((Number) v).byteValue();
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? (short) 0 : ((Number) v).shortValue();
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
    return v == null ? 0L : ((Number) v).longValue();
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
    if (v instanceof Date d) {
      return d;
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
    if (v instanceof LocalDateTime ldt) {
      return Date.valueOf(ldt.toLocalDate());
    }
    if (v instanceof LocalDate ld) {
      return Date.valueOf(ld);
    }
    throw new SQLException("Unsupported date type: " + v.getClass().getName());
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    if (v instanceof Timestamp ts) {
      return new Time(ts.getTime());
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
    if (v instanceof LocalDateTime ldt) {
      return Timestamp.valueOf(ldt);
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
