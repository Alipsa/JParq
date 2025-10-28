package se.alipsa.jparq;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import net.sf.jsqlparser.expression.Expression;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import se.alipsa.jparq.engine.AggregateFunctions;
import se.alipsa.jparq.engine.AvroProjections;
import se.alipsa.jparq.engine.ColumnsUsed;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.ParquetSchemas;
import se.alipsa.jparq.engine.ProjectionFields;
import se.alipsa.jparq.engine.SqlParser;

/** An implementation of the java.sql.PreparedStatement interface. */
@SuppressWarnings({
    "checkstyle:AbbreviationAsWordInName", "checkstyle:OverloadMethodsDeclarationOrder",
    "PMD.AvoidCatchingGenericException"
})
class JParqPreparedStatement implements PreparedStatement {
  private final JParqStatement stmt;

  // --- Query Plan Fields (calculated in constructor) ---
  private final SqlParser.Select parsedSelect;
  private final Configuration conf;
  private final Schema fileAvro;
  private final Optional<FilterPredicate> parquetPredicate;
  private final Expression residualExpression;
  private final Path path;
  private final File file;

  JParqPreparedStatement(JParqStatement stmt, String sql) throws SQLException {
    this.stmt = stmt;

    // --- QUERY PLANNING PHASE (Expensive CPU Work) ---
    try {
      // 1. Parse SQL
      this.parsedSelect = SqlParser.parseSelect(sql);
      this.file = stmt.getConn().tableFile(parsedSelect.table());
      var aggregatePlan = AggregateFunctions.plan(parsedSelect);

      // 2. Setup Configuration
      this.conf = new Configuration(false);
      conf.setBoolean("parquet.filter.statistics.enabled", true);
      conf.setBoolean("parquet.read.filter.columnindex.enabled", true);
      conf.setBoolean("parquet.filter.dictionary.enabled", true);
      this.path = new Path(file.toURI());

      // 3. Read Schema and Setup Projection/Filter
      Schema avro;
      try {
        avro = ParquetSchemas.readAvroSchema(path, conf);
      } catch (IOException ignore) {
        /* No schema -> no pushdown */
        avro = null;
      }

      this.fileAvro = avro;

      // 4. Projection Pushdown (SELECT U WHERE columns)
      configureProjectionPushdown(fileAvro, aggregatePlan);

      // 5. Filter Pushdown & Residual Calculation
      if (!parsedSelect.distinct() && fileAvro != null && parsedSelect.where() != null) {
        this.parquetPredicate = ParquetFilterBuilder.build(fileAvro, parsedSelect.where());
        this.residualExpression = ParquetFilterBuilder.residual(fileAvro, parsedSelect.where());
      } else {
        this.parquetPredicate = Optional.empty();
        this.residualExpression = parsedSelect.where();
      }

    } catch (Exception e) {
      throw new SQLException("Failed to prepare query: " + sql, e);
    }
  }

  /**
   * Executes the query using the pre-calculated plan. This method only focuses on
   * I/O (building and reading the ParquetReader).
   */
  @SuppressWarnings("PMD.CloseResource")
  @Override
  public ResultSet executeQuery() throws SQLException {
    ParquetReader<GenericRecord> reader;
    try {
      ParquetReader.Builder<GenericRecord> builder = ParquetReader.<GenericRecord>builder(new AvroReadSupport<>(), path)
          .withConf(conf);

      if (parquetPredicate.isPresent()) {
        builder = builder.withFilter(FilterCompat.get(parquetPredicate.get()));
      }

      reader = builder.build();
    } catch (Exception e) {
      throw new SQLException("Failed to open parquet file: " + file, e);
    }

    // Derive label/physical lists from the parsed SELECT
    // Convention from SqlParser: labels() empty => SELECT *
    final List<String> labels = parsedSelect.labels().isEmpty() ? null : parsedSelect.labels();
    final List<String> physical = parsedSelect.labels().isEmpty() ? null : parsedSelect.columnNames();
    // Create the result set, passing reader, select plan, residual filter,
    // plus projection labels and physical names (for metadata & value lookups)
    JParqResultSet rs = new JParqResultSet(reader, parsedSelect, file.getName(), residualExpression, labels, physical);

    stmt.setCurrentRs(rs);
    return rs;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "A PreparedStatement cannot execute a new query string. Use the no-arg executeQuery().");
  }

  @Override
  public void close() {
    // No-op for now, as PreparedStatement cleanup is minimal in this read-only
    // context.
  }

  private void configureProjectionPushdown(Schema schema, AggregateFunctions.AggregatePlan aggregatePlan) {
    if (schema == null) {
      return;
    }

    java.util.Set<String> selectColumns = ProjectionFields.fromSelect(parsedSelect);
    boolean selectAll = selectColumns == null && aggregatePlan == null;
    if (selectAll) {
      return;
    }

    java.util.Set<String> needed = new java.util.LinkedHashSet<>();
    addColumns(needed, selectColumns);
    addColumns(needed, ColumnsUsed.inWhere(parsedSelect.where()));
    for (SqlParser.OrderKey key : parsedSelect.orderBy()) {
      addColumn(needed, key.column());
    }
    if (aggregatePlan != null) {
      for (AggregateFunctions.AggregateSpec spec : aggregatePlan.specs()) {
        if (!spec.countStar()) {
          addColumns(needed, ColumnsUsed.inWhere(spec.argument()));
        }
      }
    }

    if (!needed.isEmpty()) {
      Schema avroProjection = AvroProjections.project(schema, needed);
      AvroReadSupport.setRequestedProjection(conf, avroProjection);
      AvroReadSupport.setAvroReadSchema(conf, avroProjection);
    }
  }

  private static void addColumns(java.util.Set<String> target, java.util.Set<String> source) {
    if (source == null || source.isEmpty()) {
      return;
    }
    for (String column : source) {
      addColumn(target, column);
    }
  }

  private static void addColumn(java.util.Set<String> target, String column) {
    if (column != null) {
      target.add(column);
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    return stmt.getConnection();
  }

  // --- Parameter Setters (No-ops for read-only, non-parameterized driver) ---
  @Override
  public void setString(int parameterIndex, String x) {
  }
  @Override
  public void setInt(int parameterIndex, int x) {
  }
  @Override
  public void setObject(int parameterIndex, Object x) {
  }
  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) {
  }
  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {
  }
  @Override
  public void clearParameters() {
  }
  @Override
  public void setNull(int parameterIndex, int sqlType) {
  }
  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) {
  }
  @Override
  public void setBoolean(int parameterIndex, boolean x) {
  }
  @Override
  public void setByte(int parameterIndex, byte x) {
  }
  @Override
  public void setShort(int parameterIndex, short x) {
  }
  @Override
  public void setLong(int parameterIndex, long x) {
  }
  @Override
  public void setFloat(int parameterIndex, float x) {
  }
  @Override
  public void setDouble(int parameterIndex, double x) {
  }
  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) {
  }
  @Override
  public void setBytes(int parameterIndex, byte[] x) {
  }
  @Override
  public void setDate(int parameterIndex, Date x) {
  }
  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) {
  }
  @Override
  public void setTime(int parameterIndex, Time x) {
  }
  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) {
  }
  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) {
  }
  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
  }
  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) {
  }
  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) {
  }
  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) {
  }
  @SuppressWarnings("deprecation")
  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) {
  }
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) {
  }
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) {
  }
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) {
  }
  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) {
  }
  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) {
  }
  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) {
  }
  @Override
  public void setRef(int parameterIndex, Ref x) {
  }
  @Override
  public void setBlob(int parameterIndex, Blob x) {
  }
  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) {
  }
  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) {
  }
  @Override
  public void setClob(int parameterIndex, Clob x) {
  }
  @Override
  public void setClob(int parameterIndex, Reader reader, long length) {
  }
  @Override
  public void setClob(int parameterIndex, Reader reader) {
  }
  @Override
  public void setArray(int parameterIndex, Array x) {
  }
  @Override
  public void setURL(int parameterIndex, URL x) {
  }
  @Override
  public void setRowId(int parameterIndex, RowId x) {
  }
  @Override
  public void setNString(int parameterIndex, String value) {
  }
  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) {
  }
  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) {
  }
  @Override
  public void setNClob(int parameterIndex, NClob value) {
  }
  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) {
  }
  @Override
  public void setNClob(int parameterIndex, Reader reader) {
  }
  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) {
  }

  // --- Batching methods (Unsupported) ---

  @Override
  public void addBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Batch operations are not supported by this read-only driver.");
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Batch operations are not supported by this read-only driver.");
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Batch operations are not supported by this read-only driver.");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Batch operations are not supported by this read-only driver.");
  }

  // --- Boilerplate (Standard PreparedStatement methods) ---

  @Override
  public ResultSet getResultSet() {
    return stmt.getResultSet();
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public boolean isPoolable() {
    return false;
  }

  @Override
  public void setPoolable(boolean poolable) {
    // Do nothing
  }

  @Override
  public void closeOnCompletion() {
    // Do nothing
  }

  @Override
  public boolean isCloseOnCompletion() {
    return false;
  }

  // Unused PreparedStatement-specific methods (meta, etc.)
  @Override
  public ResultSetMetaData getMetaData() {
    return null;
  }
  @Override
  public ParameterMetaData getParameterMetaData() {
    return null;
  }
  @Override
  public int executeUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute() throws SQLException {
    executeQuery();
    return true;
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
  @Override
  public int getMaxFieldSize() {
    return 0;
  }
  @Override
  public void setMaxFieldSize(int max) {
  }
  @Override
  public int getMaxRows() {
    return 0;
  }
  @Override
  public void setMaxRows(int max) {
  }
  @Override
  public void setEscapeProcessing(boolean enable) {
  }
  @Override
  public int getQueryTimeout() {
    return 0;
  }
  @Override
  public void setQueryTimeout(int seconds) {
  }
  @Override
  public void cancel() {
  }
  @Override
  public SQLWarning getWarnings() {
    return null;
  }
  @Override
  public void clearWarnings() {
  }
  @Override
  public void setCursorName(String name) {
  }
  @Override
  public int getUpdateCount() {
    return 0;
  }
  @Override
  public boolean getMoreResults() {
    return false;
  }
  @Override
  public boolean getMoreResults(int current) {
    return false;
  }
  @Override
  public void setFetchDirection(int direction) {
  }
  @Override
  public int getFetchDirection() {
    return ResultSet.FETCH_FORWARD;
  }
  @Override
  public void setFetchSize(int rows) {
  }
  @Override
  public int getFetchSize() {
    return 0;
  }
  @Override
  public int getResultSetConcurrency() {
    return ResultSet.CONCUR_READ_ONLY;
  }
  @Override
  public int getResultSetType() {
    return ResultSet.TYPE_FORWARD_ONLY;
  }
  @Override
  public ResultSet getGeneratedKeys() {
    return null;
  }
  @Override
  public int getResultSetHoldability() {
    return 0;
  }
  @Override
  public <T> T unwrap(Class<T> iface) {
    return null;
  }
  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }
}
