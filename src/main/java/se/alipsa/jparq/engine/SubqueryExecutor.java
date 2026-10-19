package se.alipsa.jparq.engine;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.meta.InformationSchemaColumns;
import se.alipsa.jparq.meta.InformationSchemaTables;

/**
 * Utility for evaluating sub queries using the same {@link JParqConnection} as
 * the parent query.
 */
public final class SubqueryExecutor {

  private final JParqConnection connection;
  private final Map<String, SubqueryResult> cache = new ConcurrentHashMap<>();
  private final StatementFactory statementFactory;
  private final Function<Table, Set<String>> tableColumns;
  private final Map<PlainSelect, Optional<Set<String>>> scopeColumns = new IdentityHashMap<>();
  private final Map<String, Optional<Set<String>>> sourceColumns = new ConcurrentHashMap<>();

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
    this(connection, statementFactory, null);
  }

  /**
   * Creates an executor with access to the prepared plan's source schemas.
   *
   * @param connection
   *          active connection
   * @param statementFactory
   *          nested statement factory, or null
   * @param tableColumns
   *          source column resolver returning null for unknown inputs, or null to
   *          resolve physical tables
   */
  public SubqueryExecutor(JParqConnection connection, StatementFactory statementFactory,
      Function<Table, Set<String>> tableColumns) {
    this.connection = Objects.requireNonNull(connection, "connection");
    this.statementFactory = statementFactory;
    this.tableColumns = tableColumns == null ? this::physicalColumns : tableColumns;
  }

  /**
   * Execute the supplied {@link Select} and capture all rows eagerly.
   *
   * @param subSelect
   *          the sub select to execute
   * @return the {@link SubqueryResult} containing rows and column labels
   */
  public SubqueryResult execute(Select subSelect) {
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

  /**
   * Resolves correlations using cached inner-scope columns before outer names.
   *
   * @param select
   *          subquery to rewrite
   * @param qualifiers
   *          outer qualifiers
   * @param columns
   *          candidate unqualified outer names
   * @param context
   *          outer qualifier mappings
   * @param values
   *          values in the current outer row
   * @return rewritten SQL and correlation references
   */
  public CorrelatedSubqueryRewriter.Result rewrite(Select select, Collection<String> qualifiers, Set<String> columns,
      Map<String, Map<String, String>> context, BiFunction<String, String, Object> values) {
    return SubqueryCorrelatedFiltersIsolator.isolate(select, qualifiers, columns, context, values, this::localColumns);
  }

  /**
   * Resolves fixed virtual-table columns without scanning the catalog.
   *
   * @param table
   *          table reference
   * @return raw field names, or null when this is not a supported virtual table
   */
  public static Set<String> virtualColumns(Table table) {
    org.apache.avro.Schema schema;
    if (InformationSchemaTables.matchesQualifiedName(table.getSchemaName(), table.getName(),
        table.getFullyQualifiedName())) {
      schema = InformationSchemaTables.schema();
    } else if (InformationSchemaColumns.matchesQualifiedName(table.getSchemaName(), table.getName(),
        table.getFullyQualifiedName())) {
      schema = InformationSchemaColumns.schema();
    } else {
      return null;
    }
    return schema.getFields().stream().map(org.apache.avro.Schema.Field::name)
        .collect(java.util.stream.Collectors.toSet());
  }

  /**
   * Collects normalized names from all local inputs once per query scope.
   *
   * @param select
   *          inner query scope
   * @return columns bound by its FROM inputs, or null if any input is unknown
   */
  private Set<String> localColumns(PlainSelect select) {
    return scopeColumns.computeIfAbsent(select, scope -> joinedColumns(scope.getFromItem(), scope.getJoins(), scope))
        .orElse(null);
  }

  /**
   * Combines input names while retaining uncertainty from any join operand.
   *
   * @param first
   *          leading input
   * @param joins
   *          subsequent inputs, or null
   * @param scope
   *          declaring query scope
   * @return known names, or empty when any source is unknown
   */
  private Optional<Set<String>> joinedColumns(FromItem first, List<Join> joins, PlainSelect scope) {
    Optional<Set<String>> initial = inputColumns(first, scope);
    if (initial.isEmpty()) {
      return Optional.empty();
    }
    Set<String> names = new LinkedHashSet<>(initial.orElseThrow());
    if (joins != null) {
      for (Join join : joins) {
        Optional<Set<String>> next = inputColumns(join.getRightItem(), scope);
        if (next.isEmpty()) {
          return Optional.empty();
        }
        names.addAll(next.orElseThrow());
      }
    }
    return Optional.of(Set.copyOf(names));
  }

  /**
   * Finds a source's visible names without executing its predicates.
   *
   * @param input
   *          local FROM input
   * @param scope
   *          query declaring that input
   * @return known normalized names, or empty when the input cannot be resolved
   */
  private Optional<Set<String>> inputColumns(FromItem input, PlainSelect scope) {
    if (input == null) {
      return Optional.of(Set.of());
    }
    if (input.getAlias() != null && input.getAlias().getAliasColumns() != null
        && !input.getAlias().getAliasColumns().isEmpty()) {
      return Optional.of(input.getAlias().getAliasColumns().stream().map(alias -> Identifier.lookupKey(alias.name))
          .collect(java.util.stream.Collectors.toSet()));
    }
    if (input instanceof Table table) {
      if (scope.getWithItemsList() != null) {
        for (var cte : scope.getWithItemsList()) {
          if (Identifier.lookupKey(cte.getAliasName()).equals(Identifier.lookupKey(table.getName()))) {
            return cteColumns(cte);
          }
        }
      }
      return sourceColumns.computeIfAbsent(table.getFullyQualifiedName(),
          name -> Optional.ofNullable(tableColumns.apply(table)).map(
              columns -> columns.stream().map(Identifier::lookupKey).collect(java.util.stream.Collectors.toSet())));
    }
    if (input instanceof ParenthesedSelect derived) {
      return outputColumns(derived.getSelect());
    }
    if (input instanceof ParenthesedFromItem group) {
      return joinedColumns(group.getFromItem(), group.getJoins(), scope);
    }
    return Optional.empty();
  }

  /**
   * Resolves explicit CTE column aliases before its projected output names.
   *
   * @param cte
   *          CTE declaration
   * @return known exported names, or empty when its output is unknown
   */
  private Optional<Set<String>> cteColumns(WithItem<?> cte) {
    if (cte.getWithItemList() != null && !cte.getWithItemList().isEmpty()) {
      return Optional.of(cte.getWithItemList().stream().map(item -> Identifier.lookupKey(item.toString()))
          .collect(java.util.stream.Collectors.toSet()));
    }
    return outputColumns(cte.getSelect());
  }

  /**
   * Finds columns exported by a derived query, respecting projection aliases.
   *
   * @param select
   *          derived query
   * @return known output names, or empty when they cannot all be determined
   */
  private Optional<Set<String>> outputColumns(Select select) {
    if (select instanceof ParenthesedSelect nested) {
      return outputColumns(nested.getSelect());
    }
    if (select instanceof SetOperationList set) {
      return outputColumns(set.getSelects().getFirst());
    }
    if (!(select instanceof PlainSelect plain)) {
      return Optional.empty();
    }
    Set<String> names = new LinkedHashSet<>();
    for (var item : plain.getSelectItems()) {
      if (item.getAlias() != null) {
        names.add(Identifier.lookupKey(item.getAlias().getName()));
      } else if (item.getExpression() instanceof Column column) {
        names.add(Identifier.lookupKey(column.getColumnName()));
      } else if (item.getExpression() instanceof AllColumns) {
        Set<String> inputs = localColumns(plain);
        if (inputs == null) {
          return Optional.empty();
        }
        names.addAll(inputs);
      } else {
        names.add(Identifier.lookupKey(item.getExpression().toString()));
      }
    }
    return Optional.of(Set.copyOf(names));
  }

  /**
   * Resolves virtual and physical sources without an injected schema resolver.
   *
   * @param table
   *          source table reference
   * @return source field names, or null when the source cannot be resolved
   */
  private Set<String> physicalColumns(Table table) {
    Set<String> virtual = virtualColumns(table);
    if (virtual != null) {
      return virtual;
    }
    try {
      var location = connection.resolveTable(table.getSchemaName(), table.getName());
      return ParquetTable.validate(location, HadoopConfigurations.create()).schema().getFields().stream()
          .map(org.apache.avro.Schema.Field::name).collect(java.util.stream.Collectors.toSet());
    } catch (SQLException exception) {
      if (QueryCancellation.findCancellation(exception).isPresent()) {
        throw new IllegalStateException("Canceled while resolving subquery columns", exception);
      }
      return null;
    }
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
