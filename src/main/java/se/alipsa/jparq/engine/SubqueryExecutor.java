package se.alipsa.jparq.engine;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import se.alipsa.jparq.JParqConnection;

/**
 * Utility for evaluating sub queries using the same {@link JParqConnection} as
 * the parent query.
 */
public final class SubqueryExecutor {

  private final JParqConnection connection;
  private final Map<String, SubqueryResult> cache = new ConcurrentHashMap<>();
  private final StatementFactory statementFactory;

  /**
   * Create a new executor bound to the provided connection.
   *
   * @param connection
   *          the active connection
   */
  public SubqueryExecutor(JParqConnection connection) {
    this(connection, null);
  }

  /**
   * Create a new executor bound to the provided connection using a custom
   * statement factory.
   *
   * @param connection
   *          the active connection
   * @param statementFactory
   *          factory used to create prepared statements aware of the caller's
   *          execution context
   */
  public SubqueryExecutor(JParqConnection connection, StatementFactory statementFactory) {
    this.connection = Objects.requireNonNull(connection, "connection");
    this.statementFactory = statementFactory;
  }

  /**
   * Execute the supplied {@link net.sf.jsqlparser.statement.select.Select} and
   * capture all rows eagerly.
   *
   * @param subSelect
   *          the sub select to execute
   * @return the {@link SubqueryResult} containing rows and column labels
   */
  public SubqueryResult execute(net.sf.jsqlparser.statement.select.Select subSelect) {
    Objects.requireNonNull(subSelect, "subSelect");
    String sql = normalize(subSelect.toString());
    return cache.computeIfAbsent(sql, this::runQuery);
  }

  /**
   * Execute the provided SQL text without caching the result. This is used for
   * correlated sub queries where the SQL string changes for each outer row.
   *
   * @param sql
   *          the SQL text to execute
   * @return the {@link SubqueryResult}
   */
  public SubqueryResult executeRaw(String sql) {
    Objects.requireNonNull(sql, "sql");
    return runQuery(normalize(sql));
  }

  private SubqueryResult runQuery(String sql) {
    try (PreparedStatement stmt = prepareStatement(sql); ResultSet rs = stmt.executeQuery()) {
      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();
      List<String> columnLabels = new ArrayList<>(columnCount);
      for (int i = 1; i <= columnCount; i++) {
        String label = meta.getColumnLabel(i);
        if (label == null || label.isBlank()) {
          label = meta.getColumnName(i);
        }
        columnLabels.add(label);
      }

      List<List<Object>> rows = new ArrayList<>();
      while (rs.next()) {
        List<Object> row = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
          row.add(rs.getObject(i));
        }
        rows.add(row);
      }
      return new SubqueryResult(List.copyOf(columnLabels), List.copyOf(rows));
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to execute sub query: " + sql, e);
    }
  }

  private PreparedStatement prepareStatement(String sql) throws SQLException {
    if (statementFactory != null) {
      return statementFactory.prepare(sql);
    }
    return connection.prepareStatement(sql);
  }

  private static String normalize(String sql) {
    String trimmed = sql.trim();
    if (trimmed.startsWith("(") && trimmed.endsWith(")")) {
      return trimmed.substring(1, trimmed.length() - 1);
    }
    return trimmed;
  }

  /**
   * Immutable result set snapshot for a sub query.
   *
   * @param columnLabels
   *          list of column labels
   * @param rows
   *          list of rows, each row is a list of column values
   */
  public record SubqueryResult(List<String> columnLabels, List<List<Object>> rows) {

    /**
     * Canonical constructor verifying arguments.
     *
     * @param columnLabels
     *          column labels
     * @param rows
     *          rows
     */
    public SubqueryResult {
      Objects.requireNonNull(columnLabels, "columnLabels");
      Objects.requireNonNull(rows, "rows");
    }

    /**
     * Convenience accessor returning the first column of each row.
     *
     * @return list of values from the first column
     */
    public List<Object> firstColumnValues() {
      List<Object> values = new ArrayList<>(rows.size());
      for (List<Object> row : rows) {
        values.add(row.isEmpty() ? null : row.getFirst());
      }
      return values;
    }
  }

  /**
   * Factory for creating prepared statements tailored to the caller's execution
   * context.
   */
  @FunctionalInterface
  public interface StatementFactory {

    /**
     * Create a prepared statement for the supplied SQL text.
     *
     * @param sql
     *          SQL text to prepare
     * @return a prepared statement ready for execution
     * @throws SQLException
     *           if the statement cannot be created
     */
    PreparedStatement prepare(String sql) throws SQLException;
  }
}
