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
import se.alipsa.jparq.engine.AggregateFunctions;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.QueryProcessor;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.model.ResultSetAdapter;

/** An implementation of the java.sql.ResultSet interface. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqResultSet extends ResultSetAdapter {

  private final List<String> physicalColumnOrder; // may be null
  private QueryProcessor qp;
  private GenericRecord current;
  private final List<String> columnOrder;
  private final String tableName;
  private final List<Expression> selectExpressions;
  private final SubqueryExecutor subqueryExecutor;
  private ValueExpressionEvaluator projectionEvaluator;
  private final boolean aggregateQuery;
  private List<Object> aggregateValues;
  private List<Integer> aggregateSqlTypes;
  private boolean aggregateDelivered = false;
  private boolean aggregateOnRow = false;
  private boolean aggregateHasRow = false;
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
   * @param subqueryExecutor
   *          executor used to evaluate subqueries during row materialization
   * @throws SQLException
   *           if reading fails
   */
  public JParqResultSet(ParquetReader<GenericRecord> reader, se.alipsa.jparq.engine.SqlParser.Select select,
      String tableName, Expression residual, List<String> columnOrder, // projection labels (aliases) or null
      List<String> physicalColumnOrder, SubqueryExecutor subqueryExecutor) // physical names (may be null)
      throws SQLException {
    this.tableName = tableName;
    this.selectExpressions = List.copyOf(select.expressions());
    this.subqueryExecutor = subqueryExecutor;
    List<String> labels = (columnOrder != null ? new ArrayList<>(columnOrder) : new ArrayList<>());
    List<String> physical = physicalColumnOrder;

    AggregateFunctions.AggregatePlan aggregatePlan = AggregateFunctions.plan(select);
    if (aggregatePlan != null) {
      labels = new ArrayList<>(aggregatePlan.labels());
      physical = null;
      try {
        AggregateFunctions.AggregateResult result = AggregateFunctions.evaluate(reader, aggregatePlan, residual,
            select.having(), subqueryExecutor);
        this.aggregateValues = new ArrayList<>(result.values());
        this.aggregateSqlTypes = result.sqlTypes();
        this.aggregateHasRow = result.hasRow();
      } catch (Exception e) {
        throw new SQLException("Failed to compute aggregate query", e);
      }
      this.columnOrder = labels;
      this.physicalColumnOrder = physical;
      this.aggregateQuery = true;
      this.aggregateDelivered = false;
      this.aggregateOnRow = false;
      this.qp = null;
      this.current = null;
      this.rowNum = 0;
      return;
    }

    this.columnOrder = labels;
    this.physicalColumnOrder = physical;
    this.aggregateQuery = false;
    this.aggregateValues = null;
    this.aggregateSqlTypes = null;
    this.aggregateHasRow = false;

    try {
      GenericRecord first = reader.read();
      if (first == null) {
        // No rows emitted after pushdown; still build metadata if explicit projection
        List<String> req = select.columns(); // e.g., ["id","value"] or ["*"]
        if (this.columnOrder.isEmpty() && !req.isEmpty() && !req.contains("*")) {
          this.columnOrder.addAll(req); // mutable, safe
        }
        QueryProcessor.Options options = QueryProcessor.Options.builder().distinct(select.distinct())
            .subqueryExecutor(subqueryExecutor).preLimit(select.preLimit()).preOrderBy(select.preOrderBy());
        this.qp = new QueryProcessor(reader, this.columnOrder, /* where */ residual, select.limit(), options);
        this.current = null;
        this.rowNum = 0;
        return;
      }

      var schema = first.getSchema();
      var evaluator = new se.alipsa.jparq.engine.ExpressionEvaluator(schema, subqueryExecutor);
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
        QueryProcessor.Options options = QueryProcessor.Options.builder().schema(schema).initialEmitted(initialEmitted)
            .distinct(select.distinct()).firstAlreadyRead(firstForDistinct).subqueryExecutor(subqueryExecutor)
            .preLimit(select.preLimit()).preOrderBy(select.preOrderBy());
        this.qp = new QueryProcessor(reader, proj, residual, select.limit(), options);
        this.current = match ? first : qp.nextMatching();
      } else {
        QueryProcessor.Options options = QueryProcessor.Options.builder().schema(schema).distinct(select.distinct())
            .orderBy(order).firstAlreadyRead(first).subqueryExecutor(subqueryExecutor).preLimit(select.preLimit())
            .preOrderBy(select.preOrderBy());
        this.qp = new QueryProcessor(reader, proj, residual, select.limit(), options);
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
    if (aggregateQuery) {
      if (!aggregateHasRow) {
        aggregateOnRow = false;
        return false;
      }
      if (aggregateDelivered) {
        aggregateOnRow = false;
        return false;
      }
      aggregateDelivered = true;
      aggregateOnRow = true;
      rowNum = 1;
      return true;
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
    if (aggregateQuery) {
      if (!aggregateDelivered || !aggregateOnRow) {
        throw new SQLException("Call next() before getting values");
      }
      if (aggregateValues == null || idx < 1 || idx > aggregateValues.size()) {
        throw new SQLException("Unknown column index: " + idx);
      }
      Object v = aggregateValues.get(idx - 1);
      lastWasNull = (v == null);
      return v;
    }
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
      projectionEvaluator = new ValueExpressionEvaluator(record.getSchema(), subqueryExecutor);
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
    if (aggregateQuery) {
      List<Integer> types = aggregateSqlTypes == null ? List.of() : aggregateSqlTypes;
      return new AggregateResultSetMetaData(columnOrder, types, tableName);
    }
    var schema = (current == null) ? null : current.getSchema();
    return new JParqResultSetMetaData(schema, columnOrder, physicalColumnOrder, tableName);

  }

  @SuppressWarnings("PMD.EmptyCatchBlock")
  @Override
  public void close() throws SQLException {
    closed = true;
    try {
      if (qp != null) {
        qp.close();
      }
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
