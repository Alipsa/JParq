package se.alipsa.jparq;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/** An implementation of the java.sql.Connection interface for parquet files. */
public class JParqConnection implements Connection {

  private final File baseDir;
  private final boolean caseSensitive;
  private boolean closed = false;

  JParqConnection(String url, Properties props) throws SQLException {
    Objects.requireNonNull(url, "url");
    String path = url.substring(JParqDriver.URL_PREFIX.length());
    int q = path.indexOf('?');
    Properties qprops = new Properties();
    if (q >= 0) {
      String qs = path.substring(q + 1);
      qprops.putAll(JParqUtil.parseQuery(qs));
      path = path.substring(0, q);
    }
    if (props != null) {
      qprops.putAll(props);
    }

    this.caseSensitive = Boolean.parseBoolean(qprops.getProperty("caseSensitive", "false"));

    if (path.startsWith("file://")) {
      path = path.substring("file://".length());
    }
    this.baseDir = new File(path);
    if (!baseDir.isDirectory()) {
      throw new SQLException("Not a directory: " + baseDir);
    }
  }

  File tableFile(String tableName) throws SQLException {
    String name = caseSensitive ? tableName : tableName.toLowerCase(Locale.ROOT);
    File[] files = baseDir.listFiles((dir, n) -> n.toLowerCase(Locale.ROOT).endsWith(".parquet"));
    if (files == null) {
      throw new SQLException("Failed to list directory: " + baseDir);
    }
    for (File f : files) {
      String base = f.getName();
      int dot = base.lastIndexOf('.');
      if (dot > 0) {
        base = base.substring(0, dot);
      }
      String candidate = caseSensitive ? base : base.toLowerCase(Locale.ROOT);
      if (candidate.equals(name)) {
        return f;
      }
    }
    throw new SQLException("Table not found: " + tableName);
  }

  public File getBaseDir() {
    return baseDir;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  // --- Connection API ---
  @Override
  public Statement createStatement() {
    return new JParqStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new JParqStatement(this).prepare(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
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
  public void close() {
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public DatabaseMetaData getMetaData() {
    return new JParqDatabaseMetaData(this);
  }

  @Override
  public void setReadOnly(boolean readOnly) {}

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public void setCatalog(String catalog) {}

  @Override
  public String getCatalog() {
    return baseDir.getName();
  }

  @Override
  public void setTransactionIsolation(int level) {}

  @Override
  public int getTransactionIsolation() {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() {
    return null;
  }

  @Override
  public void clearWarnings() {}

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return Map.of();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {}

  @Override
  public void setHoldability(int holdability) throws SQLException {}

  @Override
  public int getHoldability() throws SQLException {
    return 0;
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
  public void rollback(Savepoint savepoint) throws SQLException {}

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {}

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return null;
  }

  @Override
  public Clob createClob() throws SQLException {
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return false;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {}

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {}

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
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {}

  @Override
  public String getSchema() throws SQLException {
    return "";
  }

  @Override
  public void abort(Executor executor) throws SQLException {}

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {}

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
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
