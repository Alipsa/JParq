package se.alipsa.jparq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.QueryProcessor;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.model.ResultSetAdapter;

/** An implementation of the java.sql.ResultSet interface. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqResultSet extends ResultSetAdapter {

  private final List<String> physicalColumnOrder; // may be null
  private final QueryProcessor qp;
  private GenericRecord current;
  private final List<String> columnOrder;
  private final String tableName;
  private final List<Expression> selectExpressions;
  private ValueExpressionEvaluator projectionEvaluator;
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
   * @param residual
   *          the residual WHERE expression (may be null)
   * @param columnOrder
   *          the projection column labels (aliases) or null
   * @param physicalColumnOrder
   *          the physical column names (may be null)
   * @throws SQLException
   *           if reading fails
   */
  public JParqResultSet(ParquetReader<GenericRecord> reader, se.alipsa.jparq.engine.SqlParser.Select select,
      String tableName, Expression residual, List<String> columnOrder, // projection labels (aliases) or null
      List<String> physicalColumnOrder) // physical names (may be null)
      throws SQLException {
    this.tableName = tableName;
    this.selectExpressions = List.copyOf(select.expressions());
    // Always use a mutable list to avoid UOE from List.of(...)
    this.columnOrder = (columnOrder != null ? new ArrayList<>(columnOrder) : new ArrayList<>());
    // physical names aren’t mutated; store as-is (null allowed)
    this.physicalColumnOrder = physicalColumnOrder;

    try {
      GenericRecord first = reader.read();
      if (first == null) {
        // No rows emitted after pushdown; still build metadata if explicit projection
        List<String> req = select.columns(); // e.g., ["id","value"] or ["*"]
        if (this.columnOrder.isEmpty() && !req.isEmpty() && !req.contains("*")) {
          this.columnOrder.addAll(req); // mutable, safe
        }
        this.qp = new QueryProcessor(reader, this.columnOrder, /* where */ residual, select.limit(), null, 0,
            select.distinct(), null);
        this.current = null;
        this.rowNum = 0;
        return;
      }

      var schema = first.getSchema();
      var evaluator = new se.alipsa.jparq.engine.ExpressionEvaluator(schema);
      boolean match = (residual == null) || evaluator.eval(residual, first);

      // Compute physical projection from schema; only add if we don’t already have
      // labels
      List<String> proj = QueryProcessor.computeProjection(select.columns(), schema);
      if (this.columnOrder.isEmpty()) {
        this.columnOrder.addAll(proj); // keep mutable
      }

      var order = select.orderBy();
      if (order == null || order.isEmpty()) {
        int initialEmitted = match ? 1 : 0;
        GenericRecord firstForDistinct = match ? first : null;
        this.qp = new QueryProcessor(reader, proj, residual, select.limit(), schema, initialEmitted, select.distinct(),
            firstForDistinct);
        this.current = match ? first : qp.nextMatching();
      } else {
        this.qp = new QueryProcessor(reader, proj, residual, select.limit(), schema, 0, select.distinct(), order,
            first);
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

  private Object value(int idx) throws SQLException {
    if (current == null) {
      throw new SQLException("Call next() before getting values");
    }

    Expression projectionExpr = projectionExpression(idx);
    if (projectionExpr != null && !(projectionExpr instanceof Column)) {
      ensureProjectionEvaluator(current);
      Object computed = projectionEvaluator == null ? null : projectionEvaluator.eval(projectionExpr, current);
      lastWasNull = (computed == null);
      return computed;
    }

    // projection name (may be an alias/label)
    String projectedName = columnOrder.get(idx - 1);

    // Try alias (label) directly first (covers engines that rewrap records by
    // label)
    String lookupName = projectedName;
    var field = current.getSchema().getField(lookupName);

    // If not found, fall back to the physical column name from metadata
    if (field == null) {
      // Contract: ResultSetMetaData#getColumnName(idx) should return the *physical*
      // name.
      String physical = getMetaData().getColumnName(idx);
      if (physical != null && !physical.equals(lookupName)) {
        lookupName = physical;
        field = current.getSchema().getField(lookupName);
      }
    }

    if (field == null) {
      throw new SQLException("Unknown column in current schema: " + projectedName);
    }

    Object raw = AvroCoercions.unwrap(current.get(lookupName), field.schema());
    lastWasNull = (raw == null);
    return raw;
  }

  private Expression projectionExpression(int idx) {
    if (selectExpressions.isEmpty()) {
      return null;
    }
    int i = idx - 1;
    if (i >= 0 && i < selectExpressions.size()) {
      return selectExpressions.get(i);
    }
    return null;
  }

  private void ensureProjectionEvaluator(GenericRecord record) {
    if (projectionEvaluator == null && record != null && !selectExpressions.isEmpty()) {
      projectionEvaluator = new ValueExpressionEvaluator(record.getSchema());
    }
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
    // columnOrder = projection labels (aliases if present)
    // physicalColumnOrder = underlying physical column names (null for computed
    // exprs or when unknown)

    return new JParqResultSetMetaData(schema, columnOrder, physicalColumnOrder, tableName);

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
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    if (v == null) {
      return null;
    }
    if (v instanceof Timestamp) {
      return new Time(((Timestamp) v).getTime());
    }
    if (v instanceof Time) {
      return (Time) v;
    }
    if (v instanceof LocalTime lt) {
      return Time.valueOf(lt);
    }
    if (v instanceof String s) {
      return Time.valueOf(s);
    }
    if (v instanceof Long l) {
      return new Time(l);
    }
    if (v instanceof Double d) {
      return new Time(d.longValue());
    }
    throw new SQLException("Unsupported time type: " + v.getClass().getName());
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel));
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
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
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
