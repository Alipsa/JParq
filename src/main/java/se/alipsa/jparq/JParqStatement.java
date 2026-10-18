package se.alipsa.jparq;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import se.alipsa.jparq.engine.QueryCancellation;

/** An implementation of the java.sql.Statement interface. */
@SuppressWarnings({
    "checkstyle:AbbreviationAsWordInName", "checkstyle:OverloadMethodsDeclarationOrder"
})
public class JParqStatement implements Statement {

  private final JParqConnection conn;
  private String currentSql;
  private volatile JParqResultSet currentRs;
  private boolean closed = false;
  private QueryCancellation activeExecution;
  private Thread executionOwner;
  private int executionDepth;

  /**
   * Constructor for JParqStatement.
   *
   * @param conn
   *          the JParqConnection
   */
  public JParqStatement(JParqConnection conn) {
    this.conn = conn;
  }

  /**
   * Prepares a SQL statement.
   *
   * @param sql
   *          the SQL query
   * @return a JParqPreparedStatement
   * @throws SQLException
   *           if a database access error occurs
   */
  // Used by JParqConnection
  public JParqPreparedStatement prepare(String sql) throws SQLException {
    this.currentSql = sql;
    return new JParqPreparedStatement(this, sql);
  }

  /**
   * Gets the associated JParqConnection.
   *
   * @return the JParqConnection
   */
  public JParqConnection getConn() {
    return conn;
  }

  /**
   * Gets the current SQL query.
   *
   * @return the current SQL query
   */
  public String getCurrentSql() {
    return currentSql;
  }

  /**
   * Gets the current result set.
   *
   * @return the JParqResultSet
   */
  public JParqResultSet getCurrentRs() {
    return currentRs;
  }

  /**
   * Sets the current result set.
   *
   * @param rs
   *          the JParqResultSet
   */
  public void setCurrentRs(JParqResultSet rs) {
    this.currentRs = rs;
  }

  /**
   * Executes the query by delegating the work to a temporary PreparedStatement.
   * This ensures the query planning logic is centralized in the prepared
   * statement class.
   */
  @SuppressWarnings({
      "PMD.CloseResource", "PMD.AvoidCatchingGenericException", "PMD.EmptyCatchBlock"
  })
  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    this.currentSql = sql;
    // Delegation: Create a temporary prepared statement, execute it, and close it.
    // The JParqPreparedStatement constructor will perform the expensive planning
    // phase.
    try (ExecutionScope execution = beginExecution()) {
      try (JParqPreparedStatement pstmt = new JParqPreparedStatement(this, sql)) {
        execution.cancellation().checkCanceled();
        this.currentRs = pstmt.executeQuery();
        return currentRs;
      } catch (SQLException | RuntimeException exception) {
        execution.cancellation().checkCanceled();
        throw exception;
      }
    }
  }

  /**
   * Starts execution before planning or materialization, sharing nested scopes.
   *
   * @return scope owning one nesting level of this statement's execution
   * @throws SQLException
   *           if another thread is executing this statement
   */
  ExecutionScope beginExecution() throws SQLException {
    return beginExecution(null);
  }

  /**
   * Starts nested work using the outer result's cancellation state during
   * iteration.
   *
   * @param inherited
   *          outer cancellation state, or null for a fresh execution
   * @return execution scope
   * @throws SQLException
   *           if another thread is executing this statement
   */
  synchronized ExecutionScope beginExecution(QueryCancellation inherited) throws SQLException {
    if (executionDepth == 0) {
      activeExecution = inherited == null ? new QueryCancellation() : inherited;
      executionOwner = Thread.currentThread();
    } else if (executionOwner != Thread.currentThread()) {
      throw new SQLException("Statement is already executing on another thread");
    }
    executionDepth++;
    return new ExecutionScope(activeExecution);
  }

  /** Holds the cancellation state until an execution or nested query returns. */
  final class ExecutionScope implements AutoCloseable {
    private final QueryCancellation cancellation;

    /**
     * Captures the shared execution state.
     *
     * @param cancellation
     *          execution cancellation token
     */
    private ExecutionScope(QueryCancellation cancellation) {
      this.cancellation = cancellation;
    }

    /**
     * Returns the cancellation state shared with nested executions.
     *
     * @return shared token
     */
    QueryCancellation cancellation() {
      return cancellation;
    }

    @Override
    public void close() {
      synchronized (JParqStatement.this) {
        executionDepth--;
        if (executionDepth == 0) {
          executionOwner = null;
          activeExecution = null;
        }
      }
    }
  }

  // --- Unsupported Write/Batch methods for read-only driver ---
  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Write operations are not supported by this read-only driver.");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Write operations are not supported by this read-only driver.");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Write operations are not supported by this read-only driver.");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Write operations are not supported by this read-only driver.");
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

  // --- Boilerplate ---
  @Override
  public void close() {
    try {
      if (currentRs != null) {
        currentRs.close();
      }
    } catch (SQLException ignored) {
      // Ignore
    }
    closed = true;
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

  /**
   * Cancels execution, materialization, or the currently returned result.
   *
   * @throws SQLException
   *           if closing the current result fails
   */
  @Override
  public void cancel() throws SQLException {
    QueryCancellation execution;
    synchronized (this) {
      execution = executionDepth > 0 ? activeExecution : null;
    }
    if (execution != null) {
      execution.cancel();
    } else {
      JParqResultSet result = currentRs;
      if (result != null) {
        result.cancel();
      }
    }
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
  public boolean execute(String sql) throws SQLException {
    executeQuery(sql);
    return true;
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
  public ResultSet getResultSet() {
    return currentRs;
  }

  @Override
  public int getUpdateCount() {
    return -1;
  }

  @Override
  public boolean getMoreResults() {
    return false;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) {
    // Ignore
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
  public Connection getConnection() throws SQLException {
    return conn;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return null;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed || conn.isClosed();
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
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
