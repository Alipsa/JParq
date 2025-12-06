package se.alipsa.jparq.cli;

import java.io.Closeable;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqDriver;

/**
 * A simple command processor that executes JParq queries and CLI commands.
 * Handles connection lifecycle, SQL execution, and formatting of result sets.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqCliSession implements Closeable {

  private static final String UNKNOWN_VERSION = "DEV";
  /**
   * ANSI color used for the CLI prompt to keep it subtly visible.
   */
  public static final String PROMPT_COLOR = "\u001B[2;37m"; // dim light gray for subtle contrast
  /**
   * ANSI color used to render user input a bit brighter than the prompt.
   */
  public static final String USER_INPUT = "\u001B[0;97m"; // brighter white to separate input from output
  /**
   * ANSI code that resets styling after colored segments.
   */
  public static final String ANSI_RESET = "\u001B[0m";
  private Connection connection;
  private String jdbcUrl;
  private String connectedDirectory;
  private final PrintWriter out;
  private final PrintWriter err;

  /**
   * Create a new CLI session.
   *
   * @param out
   *          writer used for standard output
   * @param err
   *          writer used for error output
   */
  public JParqCliSession(PrintWriter out, PrintWriter err) {
    this.out = Objects.requireNonNull(out, "out");
    this.err = Objects.requireNonNull(err, "err");
  }

  /**
   * Compute the prompt to display to the user.
   *
   * @return the current prompt string
   */
  public String prompt() {
    String dirName = currentDirectoryName();
    if (dirName == null || dirName.isBlank()) {
      return PROMPT_COLOR + "jparq>" + ANSI_RESET + " ";
    }
    return PROMPT_COLOR + "jparq(" + dirName + ")>" + ANSI_RESET + " ";
  }

  /**
   * Handle a single line of input. Lines starting with "/" are treated as CLI
   * commands, all others are executed as SQL.
   *
   * @param line
   *          the input line
   * @return {@code false} if the session should terminate, {@code true} otherwise
   */
  public boolean handleLine(String line) {
    if (line == null) {
      return false;
    }
    String trimmed = line.trim();
    if (trimmed.isEmpty()) {
      return true;
    }
    if (trimmed.startsWith("/")) {
      return handleCommand(trimmed);
    }
    executeSql(trimmed);
    return true;
  }

  private boolean handleCommand(String command) {
    String lower = command.toLowerCase(Locale.ROOT);
    if ("/exit".equals(lower)) {
      close();
      return false;
    }
    if ("/help".equals(lower)) {
      printHelp();
      return true;
    }
    if ("/close".equals(lower)) {
      boolean wasOpen = connection != null;
      try {
        wasOpen = wasOpen && !connection.isClosed();
      } catch (SQLException e) {
        err.println("Failed to check connection state: " + e.getMessage());
        err.flush();
      }
      closeConnection();
      if (wasOpen) {
        out.println("Connection closed.");
      } else {
        out.println("No active connection to close.");
      }
      out.flush();
      return true;
    }
    if (lower.startsWith("/connect")) {
      String baseDir = command.substring("/connect".length()).trim();
      connectDirectory(baseDir);
      return true;
    }
    if ("/list".equals(lower)) {
      listTables();
      return true;
    }
    if (lower.startsWith("/describe")) {
      String argument = command.substring("/describe".length()).trim();
      if (argument.isEmpty()) {
        err.println("Usage: /describe <table name>");
        err.flush();
        return true;
      }
      describe(argument);
      return true;
    }
    if ("/info".equals(lower)) {
      printInfo();
      return true;
    }
    err.println("Unknown command: " + command + ". Use /help to list commands.");
    err.flush();
    return true;
  }

  /**
   * Connect to a base directory containing Parquet files.
   *
   * @param baseDir
   *          the directory path to connect to; will be normalized to an absolute
   *          path
   */
  public void connectDirectory(String baseDir) {
    if (baseDir == null || baseDir.trim().isEmpty()) {
      err.println("Usage: /connect <base directory path>");
      err.flush();
      return;
    }
    try {
      Path path = Paths.get(baseDir.trim()).toAbsolutePath().normalize();
      connect(path.toString());
    } catch (Exception e) {
      err.println("Failed to resolve path: " + e.getMessage());
      err.flush();
    }
  }

  private void connect(String baseDir) {
    closeConnection();
    jdbcUrl = JParqDriver.URL_PREFIX + baseDir;
    try {
      connection = DriverManager.getConnection(jdbcUrl);
      if (connection instanceof JParqConnection jparqConn) {
        connectedDirectory = jparqConn.getBaseDir().getAbsolutePath();
      } else {
        connectedDirectory = baseDir;
      }
      out.println(PROMPT_COLOR + "Connected to " + baseDir + ANSI_RESET);
      out.flush();
    } catch (SQLException e) {
      jdbcUrl = null;
      connectedDirectory = null;
      err.println("Failed to connect: " + e.getMessage());
      err.flush();
    }
  }

  private void listTables() {
    if (!ensureConnected()) {
      return;
    }
    try {
      if (connection instanceof JParqConnection jparqConn) {
        List<JParqConnection.TableLocation> tables = new ArrayList<>(jparqConn.listTables());
        tables.sort(Comparator.comparing(JParqConnection.TableLocation::schemaName)
            .thenComparing(JParqConnection.TableLocation::tableName));
        if (tables.isEmpty()) {
          out.println("No tables found.");
        } else {
          tables.forEach(t -> out.println(t.schemaName() + "." + t.tableName()));
        }
      } else {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(null, null, "%", new String[]{
            "TABLE"
        })) {
          boolean found = false;
          while (rs.next()) {
            found = true;
            String schema = rs.getString("TABLE_SCHEM");
            String table = rs.getString("TABLE_NAME");
            if (schema == null || schema.isBlank()) {
              out.println(table);
            } else {
              out.println(schema + "." + table);
            }
          }
          if (!found) {
            out.println("No tables found.");
          }
        }
      }
      out.flush();
    } catch (SQLException e) {
      err.println("Failed to list tables: " + e.getMessage());
      err.flush();
    }
  }

  private void describe(String tableReference) {
    if (!ensureConnected()) {
      return;
    }
    String schema = null;
    String table = tableReference;
    int dot = tableReference.indexOf('.');
    if (dot > 0 && dot < tableReference.length() - 1) {
      schema = tableReference.substring(0, dot);
      table = tableReference.substring(dot + 1);
    }
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      List<ColumnMeta> columns = new ArrayList<>();
      try (ResultSet rs = metaData.getColumns(null, schema, table, "%")) {
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          String typeName = rs.getString("TYPE_NAME");
          int size = rs.getInt("COLUMN_SIZE");
          int nullable = rs.getInt("NULLABLE");
          int ordinal = rs.getInt("ORDINAL_POSITION");
          columns.add(new ColumnMeta(columnName, typeName, size, nullable == DatabaseMetaData.columnNullable, ordinal));
        }
      }
      if (columns.isEmpty()) {
        out.println("No columns found for table " + tableReference + ".");
        out.flush();
        return;
      }
      columns.sort(Comparator.comparing(ColumnMeta::ordinal));
      int nameWidth = Math.max("COLUMN".length(), columns.stream().mapToInt(c -> c.column().length()).max().orElse(0));
      int typeWidth = Math.max("TYPE".length(),
          columns.stream().map(c -> c.type() + "(" + c.size() + ")").mapToInt(String::length).max().orElse(0));
      out.println(
          "COLUMN" + pad(nameWidth - "COLUMN".length()) + " | TYPE" + pad(typeWidth - "TYPE".length()) + " | NULLABLE");
      out.println(repeat('-', nameWidth) + "-+-" + repeat('-', typeWidth) + "-+-" + repeat('-', 8));
      for (ColumnMeta column : columns) {
        String typeText = column.type() + "(" + column.size() + ")";
        out.println(column.column() + pad(nameWidth - column.column().length()) + " | " + typeText
            + pad(typeWidth - typeText.length()) + " | " + (column.nullable() ? "YES" : "NO"));
      }
      out.flush();
    } catch (SQLException e) {
      err.println("Failed to describe table: " + e.getMessage());
      err.flush();
    }
  }

  private boolean ensureConnected() {
    if (connection == null) {
      err.println("Not connected. Use /connect <base directory path> first.");
      err.flush();
      return false;
    }
    try {
      if (connection.isClosed()) {
        err.println("The connection is closed. Use /connect <base directory path> to open a new connection.");
        err.flush();
        connection = null;
        connectedDirectory = null;
        jdbcUrl = null;
        return false;
      }
    } catch (SQLException e) {
      err.println("Failed to verify connection state: " + e.getMessage());
      err.flush();
      return false;
    }
    return true;
  }

  private void executeSql(String sql) {
    if (!ensureConnected()) {
      return;
    }
    try (Statement stmt = connection.createStatement()) {
      boolean hasResult = stmt.execute(sql);
      if (hasResult) {
        try (ResultSet rs = stmt.getResultSet()) {
          printResultSet(rs);
        }
      } else {
        out.println("Statement executed.");
      }
      out.flush();
    } catch (SQLException e) {
      err.println("SQL execution failed: " + e.getMessage());
      err.flush();
    }
  }

  private void printResultSet(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int columnCount = meta.getColumnCount();
    if (columnCount == 0) {
      out.println("Query executed, no columns returned.");
      return;
    }
    List<String> headers = new ArrayList<>();
    List<Integer> widths = new ArrayList<>();
    for (int i = 1; i <= columnCount; i++) {
      String label = meta.getColumnLabel(i);
      headers.add(label);
      widths.add(label.length());
    }
    List<List<String>> rows = new ArrayList<>();
    while (rs.next()) {
      List<String> row = new ArrayList<>();
      for (int i = 1; i <= columnCount; i++) {
        Object value = rs.getObject(i);
        String text = value == null ? "NULL" : value.toString();
        row.add(text);
        widths.set(i - 1, Math.max(widths.get(i - 1), text.length()));
      }
      rows.add(row);
    }
    printRow(headers, widths);
    out.println(buildSeparator(widths));
    for (List<String> row : rows) {
      printRow(row, widths);
    }
  }

  private void printRow(List<String> columns, List<Integer> widths) {
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) {
        line.append(" | ");
      }
      String value = String.valueOf(columns.get(i));
      line.append(value);
      line.append(pad(widths.get(i) - value.length()));
    }
    out.println(line.toString());
  }

  private String buildSeparator(List<Integer> widths) {
    return widths.stream().map(w -> repeat('-', w)).collect(Collectors.joining("-+-"));
  }

  private void printHelp() {
    out.println("Available commands:");
    out.println("/connect <base directory path> - Connect to a Parquet database directory");
    out.println("/close - Close the current connection");
    out.println("/list - List available tables");
    out.println("/describe <table name> - Show column definitions for a table");
    out.println("/info - Show information about the current connection");
    out.println("/help - Display this help text");
    out.println("/exit - Exit the CLI");
    out.flush();
  }

  private void printInfo() {
    if (!ensureConnected()) {
      return;
    }
    out.println("Version: " + cliVersion());
    out.println("JDBC URL: " + jdbcUrl);
    if (connection instanceof JParqConnection jparqConn) {
      out.println("Base directory: " + jparqConn.getBaseDir().getAbsolutePath());
      out.println("Case sensitive: " + jparqConn.isCaseSensitive());
    }
    try {
      out.println("Connection closed: " + connection.isClosed());
    } catch (SQLException e) {
      out.println("Connection state could not be determined: " + e.getMessage());
    }
    out.flush();
  }

  private void closeConnection() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        err.println("Failed to close connection: " + e.getMessage());
        err.flush();
      }
    }
    connection = null;
    connectedDirectory = null;
    jdbcUrl = null;
  }

  private String pad(int count) {
    return count <= 0 ? "" : repeat(' ', count);
  }

  private String repeat(char ch, int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(ch);
    }
    return sb.toString();
  }

  @Override
  public void close() {
    closeConnection();
  }

  /**
   * Resolve the CLI version from the package manifest.
   *
   * @return the implementation version, or {@value #UNKNOWN_VERSION} when not
   *         available
   */
  public static String cliVersion() {
    Package pkg = JParqCliSession.class.getPackage();
    if (pkg != null) {
      String implementationVersion = pkg.getImplementationVersion();
      if (implementationVersion != null && !implementationVersion.isBlank()) {
        return implementationVersion;
      }
    }
    String sysVersion = System.getProperty("jparq.version");
    if (sysVersion != null && !sysVersion.isBlank()) {
      return sysVersion;
    }
    return UNKNOWN_VERSION;
  }

  private String currentDirectoryName() {
    if (connectedDirectory == null || connectedDirectory.isBlank()) {
      return null;
    }
    Path path = Paths.get(connectedDirectory);
    Path name = path.getFileName();
    return name == null ? connectedDirectory : name.toString();
  }

  /**
   * Immutable column description for describe output.
   */
  private static final class ColumnMeta {

    private final String column;
    private final String type;
    private final int size;
    private final boolean nullable;
    private final int ordinal;

    /**
     * Create a column description.
     *
     * @param column
     *          the column name
     * @param type
     *          the column type
     * @param size
     *          the column size
     * @param nullable
     *          whether the column is nullable
     * @param ordinal
     *          the ordinal position of the column
     */
    ColumnMeta(String column, String type, int size, boolean nullable, int ordinal) {
      this.column = Objects.requireNonNull(column, "column");
      this.type = Objects.requireNonNull(type, "type");
      this.size = size;
      this.nullable = nullable;
      this.ordinal = ordinal;
    }

    /**
     * Get the column name.
     *
     * @return the column name
     */
    String column() {
      return column;
    }

    /**
     * Get the column type.
     *
     * @return the column type
     */
    String type() {
      return type;
    }

    /**
     * Get the column size.
     *
     * @return the column size
     */
    int size() {
      return size;
    }

    /**
     * Check whether the column is nullable.
     *
     * @return {@code true} if nullable, otherwise {@code false}
     */
    boolean nullable() {
      return nullable;
    }

    /**
     * Get the ordinal position.
     *
     * @return the ordinal position
     */
    int ordinal() {
      return ordinal;
    }
  }
}
