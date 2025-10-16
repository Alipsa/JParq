package se.alipsa.jparq;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JParqResultSet implements ResultSet {

  private final ParquetReader<GenericRecord> reader;
  private final JParqSqlParser.Select select;
  private final String tableName;
  private GenericRecord current;
  private boolean closed = false;
  private int rowNum = 0;
  private final List<String> columnOrder = new ArrayList<>();
  private final Schema schema;

  public JParqResultSet(ParquetReader<GenericRecord> reader,
                        JParqSqlParser.Select select,
                        String tableName) throws SQLException {
    this.reader = Objects.requireNonNull(reader);
    this.select = Objects.requireNonNull(select);
    this.tableName = tableName;
    try {
      this.current = readNextMatching();                 // may be null
      this.schema  = (this.current != null) ? this.current.getSchema() : null;  // <-- assign on all paths
      if (current != null) {
        if (select.columns().isEmpty() || select.columns().contains("*")) {
          for (Schema.Field f : schema.getFields()) columnOrder.add(f.name());
        } else {
          columnOrder.addAll(select.columns());
        }
      }
    } catch (Exception e) {
      throw new SQLException("Failed reading first parquet record", e);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (closed) throw new SQLException("ResultSet closed");
    try {
      if (rowNum == 0) {
        if (current == null) return false;
        rowNum = 1;
        return true;
      }
      if (select.limit() >= 0 && rowNum >= select.limit()) return false;
      current = readNextMatching();
      if (current == null) return false;
      rowNum++;
      return true;
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  private GenericRecord readNextMatching() throws Exception {
    GenericRecord rec = reader.read();           // always read next
    while (rec != null) {
      if (matches(rec)) return rec;
      rec = reader.read();
    }
    return null;
  }

  private boolean matches(GenericRecord rec) {
    Expression where = select.where();
    if (where == null) return true;
    return eval(where, rec);
  }

  private boolean eval(Expression expr, GenericRecord rec) {
    if (expr instanceof Parenthesis p) {
      return eval(p.getExpression(), rec);
    }
    if (expr instanceof AndExpression and) {
      return eval(and.getLeftExpression(), rec) && eval(and.getRightExpression(), rec);
    }
    if (expr instanceof OrExpression or) {
      return eval(or.getLeftExpression(), rec) || eval(or.getRightExpression(), rec);
    }
    if (expr instanceof EqualsTo e)            return compare(e.getLeftExpression(), e.getRightExpression(), rec) == 0;
    if (expr instanceof GreaterThan gt)        return compare(gt.getLeftExpression(), gt.getRightExpression(), rec) > 0;
    if (expr instanceof MinorThan lt)          return compare(lt.getLeftExpression(), lt.getRightExpression(), rec) < 0;
    if (expr instanceof GreaterThanEquals ge)  return compare(ge.getLeftExpression(), ge.getRightExpression(), rec) >= 0;
    if (expr instanceof MinorThanEquals le)    return compare(le.getLeftExpression(), le.getRightExpression(), rec) <= 0;

    throw new IllegalArgumentException("Unsupported WHERE expression: " + expr);
  }

  private static final class Operand {
    final Object value;
    final Schema schemaOrNull;
    Operand(Object value, Schema schemaOrNull) {
      this.value = value;
      this.schemaOrNull = schemaOrNull;
    }
  }

  private int compare(Expression lExpr, Expression rExpr, GenericRecord rec) {
    Operand L = operand(lExpr, rec);
    Operand R = operand(rExpr, rec);

    Object l = L.value;
    Object r = R.value;

    // If one side is a column (has schema) and the other is a literal, coerce literal to column type
    if (L.schemaOrNull != null && R.schemaOrNull == null) {
      r = coerceLiteral(r, L.schemaOrNull);
    } else if (R.schemaOrNull != null && L.schemaOrNull == null) {
      l = coerceLiteral(l, R.schemaOrNull);
    }

    if (l == null || r == null) return -1; // nulls don't match in this minimal impl

    try {
      if (l instanceof Number && r instanceof Number) {
        return new BigDecimal(l.toString()).compareTo(new BigDecimal(r.toString()));
      }
      if (l instanceof Boolean && r instanceof Boolean) {
        return Boolean.compare((Boolean) l, (Boolean) r);
      }
      if (l instanceof java.sql.Timestamp && r instanceof java.sql.Timestamp) {
        return Long.compare(((Timestamp) l).getTime(), ((Timestamp) r).getTime());
      }
      if (l instanceof java.sql.Date && r instanceof java.sql.Date) {
        return Long.compare(((Date) l).getTime(), ((Date) r).getTime());
      }
      // fallback: string compare
      return l.toString().compareTo(r.toString());
    } catch (Exception e) {
      return -1;
    }
  }

  private Operand operand(Expression e, GenericRecord rec) {
    if (e instanceof Column c) {
      String name = c.getColumnName();
      Schema.Field f = rec.getSchema().getField(name);
      if (f == null) return new Operand(null, null);
      Object v = unwrapAvro(rec.get(name), f.schema());
      return new Operand(v, f.schema());
    }
    // literal
    return new Operand(literal(e), null);
  }

  private static Object literal(Expression e) {
    if (e instanceof NullValue)         return null;
    if (e instanceof StringValue sv)    return sv.getValue();
    if (e instanceof LongValue lv)      return lv.getBigIntegerValue().longValue();
    if (e instanceof DoubleValue dv)    return new BigDecimal(dv.getValue());
    if (e instanceof SignedExpression se) {
      Object inner = literal(se.getExpression());
      if (inner instanceof Number n) {
        var bd = new BigDecimal(n.toString());
        return se.getSign() == '-' ? bd.negate() : bd;
      }
      if (inner instanceof String s) {
        try {
          var bd = new BigDecimal(s);
          return se.getSign() == '-' ? bd.negate() : bd;
        } catch (Exception ignore) {}
      }
      return (se.getSign() == '-') ? ("-" + inner) : inner;
    }
    if (e instanceof BooleanValue bv)   return bv.getValue();
    if (e instanceof DateValue dv)      return dv.getValue();        // java.sql.Date
    if (e instanceof TimestampValue tv) return tv.getValue();        // java.sql.Timestamp
    try { return new BigDecimal(e.toString()); } catch (Exception ignore) { }
    return e.toString();
  }

  private Object coerceLiteral(Object lit, Schema s) {
    if (lit == null) return null;
    Schema effective = nonNullSchema(s);
    switch (effective.getType()) {
      case STRING, ENUM:
        return lit.toString();

      case INT: {
        if (LogicalTypes.date().equals(effective.getLogicalType())) {
          if (lit instanceof java.sql.Date) return lit;
          try { return java.sql.Date.valueOf(lit.toString()); } catch (Exception ignore) {}
          try { return Integer.parseInt(lit.toString()); } catch (Exception ignore) {}
          return lit.toString();
        }
        try { return Integer.parseInt(lit.toString()); } catch (Exception ignore) { return lit; }
      }

      case LONG: {
        if (effective.getLogicalType() instanceof LogicalTypes.TimestampMillis
            || effective.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
          if (lit instanceof Timestamp) return lit;
          try { return Timestamp.valueOf(lit.toString()); } catch (Exception ignore) {}
          try { return new Timestamp(Long.parseLong(lit.toString())); } catch (Exception ignore) {}
          return lit.toString();
        }
        try { return Long.parseLong(lit.toString()); } catch (Exception ignore) { return lit; }
      }

      case FLOAT, DOUBLE:
        try { return new BigDecimal(lit.toString()); } catch (Exception ignore) { return lit; }

      case BOOLEAN:
        if (lit instanceof Boolean) return lit;
        return Boolean.parseBoolean(lit.toString());

      default:
        return lit; // bytes/arrays/etc. not coerced for WHERE in this minimal impl
    }
  }

  private static Schema nonNullSchema(Schema s) {
    if (s.getType() == Schema.Type.UNION) {
      for (Schema t : s.getTypes()) if (t.getType() != Schema.Type.NULL) return t;
    }
    return s;
  }

  private static Object unwrapAvro(Object v, Schema s) {
    if (v == null) return null;
    Schema effective = nonNullSchema(s);
    switch (effective.getType()) {
      case STRING: return v.toString();
      case INT:
        if (LogicalTypes.date().equals(effective.getLogicalType())) {
          int days = (Integer) v;
          return java.sql.Date.valueOf(LocalDate.ofEpochDay(days));
        }
        return ((Number) v).intValue();
      case LONG:
        if (effective.getLogicalType() instanceof LogicalTypes.TimestampMillis
            || effective.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
          long epoch = ((Number) v).longValue();
          if (effective.getLogicalType() instanceof LogicalTypes.TimestampMicros) epoch /= 1000L;
          return Timestamp.from(Instant.ofEpochMilli(epoch));
        }
        return ((Number) v).longValue();
      case FLOAT: return ((Number) v).floatValue();
      case DOUBLE: return ((Number) v).doubleValue();
      case BOOLEAN: return v;
      case BYTES:
        if (effective.getLogicalType() instanceof LogicalTypes.Decimal dec) {
          ByteBuffer bb = ((ByteBuffer) v).duplicate();
          byte[] bytes = new byte[bb.remaining()]; bb.get(bytes);
          java.math.BigInteger bi = new java.math.BigInteger(bytes);
          return new BigDecimal(bi, dec.getScale());
        }
        if (v instanceof ByteBuffer) {
          ByteBuffer bb = ((ByteBuffer) v).duplicate();
          byte[] b = new byte[bb.remaining()]; bb.get(b); return b;
        }
        return v;
      case RECORD: return v.toString();
      case ENUM: return v.toString();
      case ARRAY:
        if (v instanceof GenericData.Array) return ((GenericData.Array<?>) v).toArray();
        return v;
      case FIXED:
        if (effective.getLogicalType() instanceof LogicalTypes.Decimal) {
          LogicalTypes.Decimal dec = (LogicalTypes.Decimal) effective.getLogicalType();
          byte[] bytes = ((GenericData.Fixed) v).bytes();
          java.math.BigInteger bi = new java.math.BigInteger(bytes);
          return new BigDecimal(bi, dec.getScale());
        }
        return ((GenericData.Fixed) v).bytes();
      default: return v;
    }
  }

  @Override public void close() throws SQLException { closed = true; try { reader.close(); } catch (Exception ignored) {} }
  @Override public boolean wasNull() { return false; }

  private Object value(int columnIndex) throws SQLException {
    if (current == null) throw new SQLException("Call next() before getting values");
    String name = columnOrder.get(columnIndex - 1);
    Object v = current.get(name);
    return unwrapAvro(v, current.getSchema().getField(name).schema());
  }

  // --- getters ---
  @Override public String getString(int columnIndex) throws SQLException { Object v = value(columnIndex); return v == null ? null : v.toString(); }
  @Override public boolean getBoolean(int columnIndex) throws SQLException { Object v = value(columnIndex); return v != null && ((Boolean) v); }
  @Override public byte getByte(int columnIndex) throws SQLException { Object v = value(columnIndex); return v == null ? 0 : ((Number) v).byteValue(); }
  @Override public short getShort(int columnIndex) throws SQLException { Object v = value(columnIndex); return v == null ? 0 : ((Number) v).shortValue(); }
  @Override public int getInt(int columnIndex) throws SQLException { Object v = value(columnIndex); return v == null ? 0 : ((Number) v).intValue(); }
  @Override public long getLong(int columnIndex) throws SQLException { Object v = value(columnIndex); return v == null ? 0 : ((Number) v).longValue(); }
  @Override public float getFloat(int columnIndex) throws SQLException { Object v = value(columnIndex); return v == null ? 0f : ((Number) v).floatValue(); }
  @Override public double getDouble(int columnIndex) throws SQLException { Object v = value(columnIndex); return v == null ? 0d : ((Number) v).doubleValue(); }
  @Override public BigDecimal getBigDecimal(int columnIndex) throws SQLException { Object v = value(columnIndex); return (BigDecimal) v; }
  @Override public byte[] getBytes(int columnIndex) throws SQLException { Object v = value(columnIndex); return (byte[]) v; }
  @Override public Date getDate(int columnIndex) throws SQLException { Object v = value(columnIndex); return (Date) v; }
  @Override public Time getTime(int columnIndex) throws SQLException { Object v = value(columnIndex); if (v instanceof Timestamp) return new Time(((Timestamp) v).getTime()); return (Time) v; }
  @Override public Timestamp getTimestamp(int columnIndex) throws SQLException { Object v = value(columnIndex); return (Timestamp) v; }
  @Override public Object getObject(int columnIndex) throws SQLException { return value(columnIndex); }

  @Override public ResultSetMetaData getMetaData() { return new JParqResultSetMetaData(schema, columnOrder, tableName); }

  // label-based getters
  @Override public String getString(String columnLabel) throws SQLException { return getString(findColumn(columnLabel)); }
  @Override public boolean getBoolean(String columnLabel) throws SQLException { return getBoolean(findColumn(columnLabel)); }
  @Override public int getInt(String columnLabel) throws SQLException { return getInt(findColumn(columnLabel)); }
  @Override public long getLong(String columnLabel) throws SQLException { return getLong(findColumn(columnLabel)); }
  @Override public double getDouble(String columnLabel) throws SQLException { return getDouble(findColumn(columnLabel)); }
  @Override public Object getObject(String columnLabel) throws SQLException { return getObject(findColumn(columnLabel)); }

  @Override public int findColumn(String columnLabel) throws SQLException {
    for (int i = 0; i < columnOrder.size(); i++) if (columnOrder.get(i).equals(columnLabel)) return i + 1;
    throw new SQLException("Unknown column: " + columnLabel);
  }

  // Lots of other ResultSet methods can be left as defaults or no-op for a minimal driver
  @Override public boolean isBeforeFirst(){return rowNum==0;} @Override public boolean isAfterLast(){return false;} @Override public boolean isFirst(){return rowNum==1;} @Override public boolean isLast(){return false;} @Override public void beforeFirst(){} @Override public void afterLast(){} @Override public boolean first(){return false;} @Override public boolean last(){return false;} @Override public int getRow(){return rowNum;} @Override public boolean absolute(int row){return false;} @Override public boolean relative(int rows){return false;} @Override public boolean previous(){return false;} @Override public void setFetchDirection(int direction){} @Override public int getFetchDirection(){return FETCH_FORWARD;} @Override public void setFetchSize(int rows){} @Override public int getFetchSize(){return 0;} @Override public int getType(){return TYPE_FORWARD_ONLY;} @Override public int getConcurrency(){return CONCUR_READ_ONLY;}

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return null;
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return new byte[0];
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public String getCursorName() throws SQLException {
    return "";
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {

  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {

  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {

  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {

  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {

  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {

  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {

  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {

  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {

  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {

  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {

  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {

  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {

  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {

  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {

  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {

  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {

  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {

  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {

  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {

  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {

  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {

  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {

  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {

  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {

  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {

  }

  @Override
  public void insertRow() throws SQLException {

  }

  @Override
  public void updateRow() throws SQLException {

  }

  @Override
  public void deleteRow() throws SQLException {

  }

  @Override
  public void refreshRow() throws SQLException {

  }

  @Override
  public void cancelRowUpdates() throws SQLException {

  }

  @Override
  public void moveToInsertRow() throws SQLException {

  }

  @Override
  public void moveToCurrentRow() throws SQLException {

  }

  @Override
  public Statement getStatement() throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {

  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {

  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {

  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {

  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {

  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {

  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {

  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {

  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {

  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {

  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {

  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {

  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return "";
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return "";
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {

  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return null;
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}