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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
import se.alipsa.jparq.engine.JoinRecordReader;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.ParquetRecordReaderAdapter;
import se.alipsa.jparq.engine.ParquetSchemas;
import se.alipsa.jparq.engine.ProjectionFields;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SubqueryExecutor;

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
  private final boolean joinQuery;
  private final List<SqlParser.TableReference> tableReferences;

  JParqPreparedStatement(JParqStatement stmt, String sql) throws SQLException {
    this.stmt = stmt;

    // --- QUERY PLANNING PHASE (Expensive CPU Work) ---
    try {
      // 1. Parse SQL
      this.parsedSelect = SqlParser.parseSelect(sql);
      this.tableReferences = parsedSelect.tableReferences();
      this.joinQuery = parsedSelect.hasMultipleTables();
      final var aggregatePlan = AggregateFunctions.plan(parsedSelect);

      // 2. Setup Configuration
      this.conf = new Configuration(false);
      conf.setBoolean("parquet.filter.statistics.enabled", true);
      conf.setBoolean("parquet.read.filter.columnindex.enabled", true);
      conf.setBoolean("parquet.filter.dictionary.enabled", true);

      if (joinQuery) {
        this.file = null;
        this.path = null;
        this.fileAvro = null;
        this.parquetPredicate = Optional.empty();
        this.residualExpression = parsedSelect.where();
      } else {
        this.file = stmt.getConn().tableFile(parsedSelect.table());
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
    RecordReader reader;
    String resultTableName;
    try {
      if (joinQuery) {
        JoinRecordReader joinReader = buildJoinReader();
        reader = joinReader;
        resultTableName = buildJoinTableName();
      } else {
        ParquetReader.Builder<GenericRecord> builder = ParquetReader
            .<GenericRecord>builder(new AvroReadSupport<>(), path).withConf(conf);

        if (parquetPredicate.isPresent()) {
          builder = builder.withFilter(FilterCompat.get(parquetPredicate.get()));
        }

        ParquetReader<GenericRecord> parquetReader = builder.build();
        reader = new ParquetRecordReaderAdapter(parquetReader);
        resultTableName = file.getName();
      }
    } catch (Exception e) {
      throw new SQLException("Failed to open parquet data source", e);
    }

    // Derive label/physical lists from the parsed SELECT
    // Convention from SqlParser: labels() empty => SELECT *
    final List<String> labels = parsedSelect.labels().isEmpty() ? null : parsedSelect.labels();
    final List<String> physical = parsedSelect.labels().isEmpty() ? null : parsedSelect.columnNames();
    // Create the result set, passing reader, select plan, residual filter,
    // plus projection labels and physical names (for metadata & value lookups)
    SubqueryExecutor subqueryExecutor = new SubqueryExecutor(stmt.getConn());
    JParqResultSet rs = new JParqResultSet(reader, parsedSelect, resultTableName, residualExpression, labels, physical,
        subqueryExecutor);

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

  private JoinRecordReader buildJoinReader() throws SQLException {
    List<JoinRecordReader.JoinTable> tables = new ArrayList<>();
    for (SqlParser.TableReference ref : tableReferences) {
      if (ref.subquery() != null) {
        tables.add(materializeSubqueryTable(ref));
        continue;
      }
      String tableName = ref.tableName();
      if (tableName == null || tableName.isBlank()) {
        throw new SQLException("JOIN requires explicit table names");
      }
      File tableFile = stmt.getConn().tableFile(tableName);
      Path tablePath = new Path(tableFile.toURI());
      Configuration tableConf = new Configuration(false);
      tableConf.setBoolean("parquet.filter.statistics.enabled", true);
      tableConf.setBoolean("parquet.read.filter.columnindex.enabled", true);
      tableConf.setBoolean("parquet.filter.dictionary.enabled", true);
      Schema tableSchema;
      try {
        tableSchema = ParquetSchemas.readAvroSchema(tablePath, tableConf);
      } catch (IOException e) {
        throw new SQLException("Failed to read schema for table " + tableName, e);
      }
      List<GenericRecord> rows = new ArrayList<>();
      try (ParquetReader<GenericRecord> tableReader = ParquetReader
          .<GenericRecord>builder(new AvroReadSupport<>(), tablePath).withConf(tableConf).build()) {
        GenericRecord record = tableReader.read();
        while (record != null) {
          rows.add(record);
          record = tableReader.read();
        }
      } catch (Exception e) {
        throw new SQLException("Failed to read data for table " + tableName, e);
      }
      tables.add(new JoinRecordReader.JoinTable(tableName, ref.tableAlias(), tableSchema, rows, ref.joinType(),
          ref.joinCondition()));
    }
    try {
      return new JoinRecordReader(tables);
    } catch (IllegalArgumentException e) {
      throw new SQLException("Failed to build join reader", e);
    }
  }

  /**
   * Materialize a derived table used in a JOIN by executing the associated
   * subquery and adapting the result into {@link GenericRecord} instances.
   *
   * @param ref
   *          table reference describing the derived table
   * @return a {@link JoinRecordReader.JoinTable} backed by the subquery result
   * @throws SQLException
   *           if the subquery fails to execute or cannot be converted into an
   *           Avro representation
   */
  private JoinRecordReader.JoinTable materializeSubqueryTable(SqlParser.TableReference ref) throws SQLException {
    String sql = ref.subquerySql();
    if (sql == null || sql.isBlank()) {
      throw new SQLException("Missing subquery SQL for derived table");
    }
    try (PreparedStatement subStmt = stmt.getConn().prepareStatement(sql);
        ResultSet rs = subStmt.executeQuery()) {
      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();
      List<String> fieldNames = new ArrayList<>(columnCount);
      List<Schema> valueSchemas = new ArrayList<>(columnCount);
      Schema schema = buildSubquerySchema(meta, ref, fieldNames, valueSchemas);
      List<GenericRecord> rows = new ArrayList<>();
      while (rs.next()) {
        GenericData.Record record = new GenericData.Record(schema);
        for (int i = 1; i <= columnCount; i++) {
          Object raw = rs.getObject(i);
          Schema fieldSchema = valueSchemas.get(i - 1);
          record.put(fieldNames.get(i - 1), toAvroValue(raw, fieldSchema));
        }
        rows.add(record);
      }
      String tableName = ref.tableAlias() != null ? ref.tableAlias() : ref.tableName();
      return new JoinRecordReader.JoinTable(tableName, ref.tableAlias(), schema, rows, ref.joinType(),
          ref.joinCondition());
    } catch (SQLException e) {
      throw new SQLException("Failed to execute subquery for join: " + sql, e);
    }
  }

  /**
   * Build an Avro schema describing the result of a derived table subquery.
   *
   * @param meta
   *          metadata describing the subquery result set
   * @param ref
   *          table reference owning the subquery
   * @param fieldNames
   *          output list capturing the field names in column order
   * @param valueSchemas
   *          output list receiving the non-null Avro schema for each column
   * @return the constructed Avro schema for the derived table
   * @throws SQLException
   *           if metadata cannot be interrogated
   */
  private Schema buildSubquerySchema(ResultSetMetaData meta, SqlParser.TableReference ref, List<String> fieldNames,
      List<Schema> valueSchemas) throws SQLException {
    int columnCount = meta.getColumnCount();
    List<Schema.Field> fields = new ArrayList<>(columnCount);
    for (int i = 1; i <= columnCount; i++) {
      String columnName = columnLabel(meta, i);
      Schema valueSchema = columnSchema(meta, i);
      Schema union = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), valueSchema));
      Schema.Field field = new Schema.Field(columnName, union, null, Schema.Field.NULL_DEFAULT_VALUE);
      fields.add(field);
      fieldNames.add(columnName);
      valueSchemas.add(valueSchema);
    }
    String recordName = ref.tableAlias() != null && !ref.tableAlias().isBlank() ? ref.tableAlias() : ref.tableName();
    Schema schema = Schema.createRecord(recordName == null ? "derived" : recordName, null, null, false);
    schema.setFields(fields);
    return schema;
  }

  /**
   * Determine the Avro schema for a subquery column based on its JDBC type.
   *
   * @param meta
   *          metadata describing the column
   * @param column
   *          one-based column index
   * @return Avro schema representing the column type (excluding nullability)
   * @throws SQLException
   *           if column metadata cannot be accessed
   */
  private Schema columnSchema(ResultSetMetaData meta, int column) throws SQLException {
    int type = meta.getColumnType(column);
    return switch (type) {
      case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> Schema.create(Schema.Type.BOOLEAN);
      case java.sql.Types.TINYINT, java.sql.Types.SMALLINT, java.sql.Types.INTEGER -> Schema.create(Schema.Type.INT);
      case java.sql.Types.BIGINT -> Schema.create(Schema.Type.LONG);
      case java.sql.Types.FLOAT, java.sql.Types.REAL -> Schema.create(Schema.Type.FLOAT);
      case java.sql.Types.DOUBLE -> Schema.create(Schema.Type.DOUBLE);
      case java.sql.Types.NUMERIC, java.sql.Types.DECIMAL -> decimalSchema(meta, column);
      case java.sql.Types.DATE -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
      case java.sql.Types.TIME -> LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
      case java.sql.Types.TIMESTAMP -> LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
      case java.sql.Types.BINARY, java.sql.Types.VARBINARY, java.sql.Types.LONGVARBINARY -> Schema
          .create(Schema.Type.BYTES);
      default -> Schema.create(Schema.Type.STRING);
    };
  }

  private Schema decimalSchema(ResultSetMetaData meta, int column) throws SQLException {
    int precision = meta.getPrecision(column);
    int scale = meta.getScale(column);
    LogicalType decimal = LogicalTypes.decimal(Math.max(precision, 1), Math.max(scale, 0));
    return decimal.addToSchema(Schema.create(Schema.Type.BYTES));
  }

  /**
   * Convert a JDBC value to an Avro-compatible representation honouring
   * registered logical types.
   *
   * @param value
   *          raw JDBC value (possibly {@code null})
   * @param schema
   *          Avro schema describing the non-null type
   * @return value coerced into the representation expected by Avro
   * @throws SQLException
   *           if coercion fails
   */
  private Object toAvroValue(Object value, Schema schema) throws SQLException {
    if (value == null) {
      return null;
    }
    LogicalType logical = schema.getLogicalType();
    if (logical instanceof LogicalTypes.Date) {
      Date date = value instanceof Date d ? d : Date.valueOf(value.toString());
      return toEpochDays(date);
    }
    if (logical instanceof LogicalTypes.TimeMillis) {
      Time time = value instanceof Time t ? t : Time.valueOf(value.toString());
      return toMillisOfDay(time);
    }
    if (logical instanceof LogicalTypes.TimestampMillis) {
      Timestamp ts = value instanceof Timestamp t ? t : Timestamp.valueOf(value.toString());
      return toEpochMillis(ts);
    }
    if (logical instanceof LogicalTypes.Decimal decimal) {
      return toDecimalBytes(value, decimal);
    }
    return switch (schema.getType()) {
      case BOOLEAN -> asBoolean(value);
      case INT -> asNumber(value).intValue();
      case LONG -> asNumber(value).longValue();
      case FLOAT -> asNumber(value).floatValue();
      case DOUBLE -> asNumber(value).doubleValue();
      case BYTES -> toBinary(value);
      case STRING -> value.toString();
      default -> value;
    };
  }

  private String columnLabel(ResultSetMetaData meta, int column) throws SQLException {
    String label = meta.getColumnLabel(column);
    if (label == null || label.isBlank()) {
      label = meta.getColumnName(column);
    }
    if (label == null || label.isBlank()) {
      label = "column_" + column;
    }
    return label;
  }

  private Number asNumber(Object value) throws SQLException {
    if (value instanceof Number num) {
      return num;
    }
    try {
      return new BigDecimal(value.toString());
    } catch (NumberFormatException e) {
      throw new SQLException("Value '" + value + "' cannot be coerced to a number", e);
    }
  }

  private Boolean asBoolean(Object value) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    if (value instanceof Number num) {
      return num.intValue() != 0;
    }
    return Boolean.parseBoolean(value.toString());
  }

  private Object toBinary(Object value) {
    if (value instanceof byte[] bytes) {
      return ByteBuffer.wrap(bytes);
    }
    if (value instanceof ByteBuffer buffer) {
      return buffer;
    }
    if (value instanceof String str) {
      return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
    }
    return ByteBuffer.wrap(value.toString().getBytes(StandardCharsets.UTF_8));
  }

  private Object toDecimalBytes(Object value, LogicalTypes.Decimal decimal) throws SQLException {
    BigDecimal number;
    if (value instanceof BigDecimal bigDecimal) {
      number = bigDecimal;
    } else {
      try {
        number = new BigDecimal(value.toString());
      } catch (NumberFormatException e) {
        throw new SQLException("Value '" + value + "' cannot be coerced to a decimal", e);
      }
    }
    number = number.setScale(decimal.getScale(), java.math.RoundingMode.HALF_UP);
    return ByteBuffer.wrap(number.unscaledValue().toByteArray());
  }

  private int toEpochDays(Date date) {
    return (int) date.toLocalDate().toEpochDay();
  }

  private int toMillisOfDay(Time time) {
    return (int) (time.toLocalTime().toNanoOfDay() / 1_000_000L);
  }

  private long toEpochMillis(Timestamp timestamp) {
    return timestamp.toInstant().toEpochMilli();
  }

  private String buildJoinTableName() {
    if (tableReferences == null || tableReferences.isEmpty()) {
      return "join";
    }
    List<String> names = new ArrayList<>();
    for (SqlParser.TableReference ref : tableReferences) {
      String candidate = ref.tableAlias();
      if (candidate == null || candidate.isBlank()) {
        candidate = ref.tableName();
      }
      if (candidate != null && !candidate.isBlank() && !names.contains(candidate)) {
        names.add(candidate);
      }
    }
    if (names.isEmpty()) {
      return "join";
    }
    return String.join("_", names);
  }

  private void configureProjectionPushdown(Schema schema, AggregateFunctions.AggregatePlan aggregatePlan) {
    if (schema == null) {
      return;
    }

    if (parsedSelect.hasMultipleTables()) {
      return;
    }

    Set<String> selectColumns = ProjectionFields.fromSelect(parsedSelect);
    boolean selectAll = selectColumns == null && aggregatePlan == null;
    if (selectAll) {
      return;
    }

    Set<String> needed = new LinkedHashSet<>();
    addColumns(needed, selectColumns);
    if (!parsedSelect.innerDistinctColumns().isEmpty()) {
      addColumns(needed, new LinkedHashSet<>(parsedSelect.innerDistinctColumns()));
    }
    addColumns(needed, ColumnsUsed.inWhere(parsedSelect.where()));
    List<String> qualifiers = new ArrayList<>();
    if (parsedSelect.tableReferences() != null) {
      for (SqlParser.TableReference ref : parsedSelect.tableReferences()) {
        if (ref.tableName() != null && !ref.tableName().isBlank()) {
          qualifiers.add(ref.tableName());
        }
        if (ref.tableAlias() != null && !ref.tableAlias().isBlank()) {
          qualifiers.add(ref.tableAlias());
        }
      }
    }
    for (Expression expression : parsedSelect.expressions()) {
      addColumns(needed, SqlParser.collectQualifiedColumns(expression, qualifiers));
    }
    addColumns(needed, SqlParser.collectQualifiedColumns(parsedSelect.where(), qualifiers));
    for (SqlParser.OrderKey key : parsedSelect.orderBy()) {
      addColumn(needed, key.column());
    }
    for (SqlParser.OrderKey key : parsedSelect.preOrderBy()) {
      addColumn(needed, key.column());
    }
    if (aggregatePlan != null) {
      for (AggregateFunctions.AggregateSpec spec : aggregatePlan.specs()) {
        if (!spec.countStar()) {
          for (Expression arg : spec.arguments()) {
            addColumns(needed, ColumnsUsed.inWhere(arg));
          }
        }
      }
      for (AggregateFunctions.GroupExpression groupExpr : aggregatePlan.groupExpressions()) {
        addColumns(needed, ColumnsUsed.inWhere(groupExpr.expression()));
      }
    }

    if (!needed.isEmpty()) {
      Schema avroProjection = AvroProjections.project(schema, needed);
      AvroReadSupport.setRequestedProjection(conf, avroProjection);
      AvroReadSupport.setAvroReadSchema(conf, avroProjection);
    }
  }

  private static void addColumns(Set<String> target, Set<String> source) {
    if (source == null || source.isEmpty()) {
      return;
    }
    for (String column : source) {
      addColumn(target, column);
    }
  }

  private static void addColumn(Set<String> target, String column) {
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
