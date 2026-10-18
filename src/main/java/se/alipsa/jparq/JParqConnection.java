package se.alipsa.jparq;

import java.io.File;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import se.alipsa.jparq.engine.HfTableDiscovery;
import se.alipsa.jparq.engine.Identifier;
import se.alipsa.jparq.helper.JParqUtil;

/** An implementation of the java.sql.Connection interface for parquet files. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqConnection implements Connection {

  private final File baseDir;
  private final boolean caseSensitive;
  private final boolean predicatePushdown;
  private final boolean projectionPushdown;
  private final boolean hfLayout;
  private boolean closed = false;
  /** Default schema used when no explicit schema is provided. */
  public static final String DEFAULT_SCHEMA = "PUBLIC";
  private final String url;

  /**
   * Create a new JParq connection for the supplied JDBC URL.
   *
   * @param url
   *          the JDBC URL that points to the base directory containing Parquet
   *          files
   * @param props
   *          optional connection properties that refine behaviour
   * @throws SQLException
   *           if the path is invalid or the connection cannot be initialized
   */
  public JParqConnection(String url, Properties props) throws SQLException {
    Objects.requireNonNull(url, "url");
    String path = url.substring(JParqDriver.URL_PREFIX.length());
    int q = path.indexOf('?');
    Properties qprops = new Properties();
    if (q >= 0) {
      String qs = path.substring(q + 1);
      qprops.putAll(JParqUtil.parseUrlQuery(qs));
      path = path.substring(0, q);
    }
    if (props != null) {
      qprops.putAll(props);
    }

    this.caseSensitive = Boolean.parseBoolean(qprops.getProperty("caseSensitive", "false"));
    this.predicatePushdown = Boolean.parseBoolean(qprops.getProperty("predicatePushdown", "true"));
    this.projectionPushdown = Boolean.parseBoolean(qprops.getProperty("projectionPushdown", "true"));

    String layout = qprops.getProperty("datasetLayout", "none");
    if (!"none".equalsIgnoreCase(layout) && !"hf".equalsIgnoreCase(layout)) {
      throw new SQLException("Invalid datasetLayout '" + layout + "'; allowed values: none, hf");
    }
    hfLayout = "hf".equalsIgnoreCase(layout);

    if (path.startsWith("file://")) {
      path = path.substring("file://".length());
    }
    this.baseDir = new File(path);
    if (!baseDir.isDirectory()) {
      throw new SQLException("Not a directory: " + baseDir);
    }
    this.url = url;
  }

  /**
   * Resolve a table reference to its ordered physical files without reading
   * footers.
   *
   * @param schemaName
   *          schema containing the table; {@value #DEFAULT_SCHEMA} is used when
   *          omitted
   * @param tableName
   *          logical table name
   * @return immutable {@link TableLocation} describing the table
   * @throws SQLException
   *           if the table cannot be found or the provided table name is blank
   */
  public TableLocation resolveTable(String schemaName, String tableName) throws SQLException {
    String effectiveSchema = (schemaName == null || schemaName.isBlank()) ? DEFAULT_SCHEMA : schemaName;
    String normalizedSchema = normalize(effectiveSchema);
    String normalizedTable = normalize(validateTableName(tableName));
    for (TableLocation location : listTables()) {
      if (normalize(location.schemaName()).equals(normalizedSchema)
          && normalize(location.tableName()).equals(normalizedTable)) {
        return location;
      }
    }
    throw new SQLException("Table not found: " + effectiveSchema + "." + tableName);
  }

  /**
   * Discover all Parquet tables available under the base directory, mapping the
   * root directory and immediate children to schemas in the default layout. With
   * datasetLayout=hf, named shards and eligible numeric directories form logical
   * tables; discovery validates the complete catalog without footer I/O.
   *
   * @return immutable list of available table locations
   * @throws SQLException
   *           if a directory cannot be listed or HF discovery is invalid
   */
  public List<TableLocation> listTables() throws SQLException {
    if (hfLayout) {
      return HfTableDiscovery.discover(baseDir, caseSensitive).tables();
    }
    Map<String, TableLocation> discovered = new LinkedHashMap<>();
    addTablesFromDirectory(baseDir, DEFAULT_SCHEMA, discovered);
    File[] children = baseDir.listFiles(File::isDirectory);
    if (children == null) {
      throw new SQLException("Failed to list directory: " + baseDir);
    }
    for (File dir : children) {
      if (!dir.isDirectory()) {
        continue;
      }
      String schemaName = dir.getName();
      String effectiveSchema = DEFAULT_SCHEMA.equalsIgnoreCase(schemaName) ? DEFAULT_SCHEMA : schemaName;
      addTablesFromDirectory(dir, effectiveSchema, discovered);
    }
    return List.copyOf(discovered.values());
  }

  /**
   * Discover all schemas available under the base directory without footer I/O.
   * HF mode classifies directory tables before schemas and validates the whole
   * catalog.
   *
   * @return immutable list of available schema names
   * @throws SQLException
   *           if a directory cannot be listed
   */
  public List<String> listSchemas() throws SQLException {
    if (hfLayout) {
      return HfTableDiscovery.discover(baseDir, caseSensitive).schemas();
    }
    Set<String> schemas = new LinkedHashSet<>();
    schemas.add(DEFAULT_SCHEMA);
    File[] children = baseDir.listFiles(File::isDirectory);
    if (children == null) {
      throw new SQLException("Failed to list directory: " + baseDir);
    }
    for (File dir : children) {
      if (!dir.isDirectory()) {
        continue;
      }
      String schemaName = dir.getName();
      String effectiveSchema = DEFAULT_SCHEMA.equalsIgnoreCase(schemaName) ? DEFAULT_SCHEMA : schemaName;
      schemas.add(effectiveSchema);
    }
    return List.copyOf(schemas);
  }

  private void addTablesFromDirectory(File dir, String schemaName, Map<String, TableLocation> discovered)
      throws SQLException {
    File[] files = dir.listFiles((d, n) -> n.toLowerCase(Locale.ROOT).endsWith(".parquet"));
    if (files == null) {
      throw new SQLException("Failed to list directory: " + dir);
    }
    for (File f : files) {
      if (!f.isFile()) {
        continue;
      }
      String base = f.getName();
      int dot = base.lastIndexOf('.');
      if (dot > 0) {
        base = base.substring(0, dot);
      }
      TableLocation location = new TableLocation(schemaName, base, f);
      String key = normalize(location.schemaName()) + "|" + normalize(location.tableName());
      discovered.putIfAbsent(key, location);
    }
  }

  private String normalize(String name) {
    return Identifier.normalize(name, caseSensitive);
  }

  private String validateTableName(String tableName) throws SQLException {
    if (tableName == null || tableName.isBlank()) {
      throw new SQLException("Table name must not be blank");
    }
    return tableName;
  }

  /**
   * Immutable logical table. Record components expose files rather than a single
   * file.
   * 
   * @param schemaName
   *          owning schema
   * @param tableName
   *          logical name
   * @param files
   *          nonempty physical files in scan order
   */
  public record TableLocation(String schemaName, String tableName, List<File> files) {
    /**
     * Copies and validates the physical file list.
     * 
     * @param schemaName
     *          owning schema
     * @param tableName
     *          logical name
     * @param files
     *          nonempty ordered files without null entries
     */
    public TableLocation {
      Objects.requireNonNull(schemaName, "schemaName");
      Objects.requireNonNull(tableName, "tableName");
      files = List.copyOf(files);
      if (files.isEmpty()) {
        throw new IllegalArgumentException("A table requires at least one file");
      }
    }

    /**
     * Creates a single-file table, preserving the original constructor signature.
     * 
     * @param schemaName
     *          owning schema
     * @param tableName
     *          logical name
     * @param file
     *          physical file
     */
    public TableLocation(String schemaName, String tableName, File file) {
      this(schemaName, tableName, List.of(file));
    }

    /**
     * Returns the first file for compatibility; execution must use all files.
     * 
     * @return first physical file
     */
    public File file() {
      return files.getFirst();
    }
  }

  /**
   * Get the base directory for this connection.
   *
   * @return the base directory
   */
  public File getBaseDir() {
    return baseDir;
  }

  /**
   * Check if the connection is case sensitive.
   *
   * @return true if case sensitive, false otherwise
   */
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  /**
   * Reports whether Parquet predicate filtering is enabled.
   * 
   * @return true if predicate pushdown is enabled
   */
  public boolean isPredicatePushdown() {
    return predicatePushdown;
  }

  /**
   * Reports whether Parquet column projection is enabled.
   * 
   * @return true if projection pushdown is enabled
   */
  public boolean isProjectionPushdown() {
    return projectionPushdown;
  }

  // --- Connection API ---
  @Override
  public Statement createStatement() {
    return new JParqStatement(this);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    if (resultSetType == ResultSet.TYPE_FORWARD_ONLY && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY) {
      return createStatement();
    }
    throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY/CONCUR_READ_ONLY result sets are supported.");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLFeatureNotSupportedException("Only CLOSE_CURSORS_AT_COMMIT holdability is supported.");
    }
    return createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public String nativeSQL(String sql) {
    return sql;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) {
    /* read-only */
  }

  @Override
  public boolean getAutoCommit() {
    return true;
  }

  @Override
  public void commit() {
    /* read-only */
  }

  @Override
  public void rollback() {
    /* read-only */
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public DatabaseMetaData getMetaData() {
    return new JParqDatabaseMetaData(this, url);
  }

  @Override
  public void setReadOnly(boolean readOnly) {
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public void setCatalog(String catalog) {
  }

  @Override
  public String getCatalog() {
    return baseDir.getName();
  }

  @Override
  public void setTransactionIsolation(int level) {
  }

  @Override
  public int getTransactionIsolation() {
    return TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() {
    return null;
  }

  @Override
  public void clearWarnings() {
  }

  @Override
  public JParqPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (resultSetType == ResultSet.TYPE_FORWARD_ONLY && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY) {
      return prepareStatement(sql);
    }
    throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY/CONCUR_READ_ONLY result sets are supported.");
  }

  @Override
  public JParqPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLFeatureNotSupportedException("Only CLOSE_CURSORS_AT_COMMIT holdability is supported.");
    }
    return prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Generated keys are not supported by this read-only driver.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Generated keys are not supported by this read-only driver.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Generated keys are not supported by this read-only driver.");
  }

  @Override
  public JParqPreparedStatement prepareStatement(String sql) throws SQLException {
    return new JParqPreparedStatement(new JParqStatement(this), sql, true);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return Map.of();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLFeatureNotSupportedException("Only CLOSE_CURSORS_AT_COMMIT holdability is supported.");
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return null;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return null;
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Clob is not supported.");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Blob is not supported.");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("NClob is not supported.");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException("SQLXML is not supported.");
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new SQLException("Timeout must be >= 0.");
    }
    return !isClosed();
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return "";
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return new Properties();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new SQLFeatureNotSupportedException("Array is not supported.");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Struct is not supported.");
  }

  @Override
  public void setSchema(String schema) throws SQLException {
  }

  @Override
  public String getSchema() throws SQLException {
    return "";
  }

  @Override
  public void abort(Executor executor) throws SQLException {
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface != null && iface.isInstance(this)) {
      return iface.cast(this);
    }
    String typeName = iface == null ? "null" : iface.getName();
    throw new SQLException("Cannot unwrap to " + typeName);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface != null && iface.isInstance(this);
  }
}
