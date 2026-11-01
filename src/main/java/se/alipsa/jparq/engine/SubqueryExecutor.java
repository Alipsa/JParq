package se.alipsa.jparq.engine;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqPreparedStatement;
import se.alipsa.jparq.meta.InformationSchemaColumns;
import se.alipsa.jparq.meta.InformationSchemaTables;

/**
 * Utility for evaluating sub queries using the same {@link JParqConnection} as
 * the parent query.
 */
public final class SubqueryExecutor implements AutoCloseable {

  private final Map<String, PreparedStatement> preparedStatements = new LinkedHashMap<>();
  private final Map<String, Optional<org.apache.avro.Schema>> projectionSchemas = new LinkedHashMap<>();

  private final ExecutionPlans executionPlans;
  private final JParqConnection connection;
  private final Map<String, SubqueryResult> cache = new ConcurrentHashMap<>();
  private final Map<String, SubqueryResult> existenceCache = new ConcurrentHashMap<>();
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
    this(connection, statementFactory, tableColumns, new ExecutionPlans());
  }

  /**
   * Creates an executor sharing parsed shapes within one owning execution.
   *
   * @param connection
   *          active connection
   * @param statementFactory
   *          factory using the same execution plans, or null
   * @param tableColumns
   *          source column resolver, or null
   * @param executionPlans
   *          shape cache belonging only to this execution
   */
  public SubqueryExecutor(JParqConnection connection, StatementFactory statementFactory,
      Function<Table, Set<String>> tableColumns, ExecutionPlans executionPlans) {
    this.executionPlans = Objects.requireNonNull(executionPlans, "executionPlans");
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
   * correlated subqueries with stable placeholders and per-row binding frames.
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
   * Checks whether a correlated or uncorrelated subquery returns any rows,
   * without reading its unused projected values.
   *
   * @param select
   *          subquery node
   * @param qualifiers
   *          outer qualifiers
   * @param columns
   *          candidate unqualified outer columns
   * @param context
   *          outer qualifier mappings
   * @param values
   *          current outer value resolver
   * @return whether at least one row exists
   */
  public boolean existsCorrelated(Select select, Collection<String> qualifiers, Set<String> columns,
      Map<String, Map<String, String>> context, BiFunction<String, String, Object> values) {
    return !executeCorrelated(select, qualifiers, columns, context, values, true).rows().isEmpty();
  }

  /**
   * Executes a subquery using one analyzed shape and the current typed bindings.
   * Uncorrelated results retain their separate result cache. Each Select node
   * belongs to one lexical outer scope within this execution: qualifiers,
   * columns, and context must remain unchanged across calls; only resolved values
   * vary.
   *
   * @param select
   *          subquery node
   * @param qualifiers
   *          outer qualifiers
   * @param columns
   *          candidate unqualified outer columns
   * @param context
   *          outer qualifier mappings
   * @param values
   *          current outer value resolver
   * @return a fresh correlated result or cached uncorrelated result
   */
  public SubqueryResult executeCorrelated(Select select, Collection<String> qualifiers, Set<String> columns,
      Map<String, Map<String, String>> context, BiFunction<String, String, Object> values) {
    return executeCorrelated(select, qualifiers, columns, context, values, false);
  }

  /**
   * Executes a bound shape in value or row-existence mode, keeping their caches
   * separate so an existence probe cannot replace a later scalar result.
   *
   * @param select
   *          subquery node
   * @param qualifiers
   *          outer qualifiers
   * @param columns
   *          candidate outer columns
   * @param context
   *          outer qualifier mappings
   * @param values
   *          current outer value resolver
   * @param existenceOnly
   *          whether to stop after the first row without reading columns
   * @return materialized values or a zero-column row-existence result
   */
  private SubqueryResult executeCorrelated(Select select, Collection<String> qualifiers, Set<String> columns,
      Map<String, Map<String, String>> context, BiFunction<String, String, Object> values, boolean existenceOnly) {
    BoundQuery query = executionPlans.correlations.get(select);
    if (query == null) {
      String key = UUID.randomUUID().toString();
      var references = new LinkedHashMap<CorrelatedSubqueryRewriter.QualifiedColumn, Integer>();
      var rewritten = rewrite(select, qualifiers, columns, context, (qualifier, column) -> {
        var reference = new CorrelatedSubqueryRewriter.QualifiedColumn(qualifier, column);
        int index = references.computeIfAbsent(reference, ignored -> references.size());
        return new CorrelatedBindings.Reference(key, index);
      });
      query = new BoundQuery(rewritten.sql(), key, List.copyOf(references.keySet()));
      executionPlans.correlations.put(select, query);
    }
    if (query.references().isEmpty()) {
      if (existenceOnly) {
        String sql = normalize(select.toString());
        return existenceCache.computeIfAbsent(sql, key -> runQuery(key, () -> key, true));
      }
      return execute(select);
    }
    List<Object> bindings = new ArrayList<>(query.references().size());
    for (var reference : query.references()) {
      bindings.add(values.apply(reference.qualifier(), reference.column()));
    }
    var frame = new CorrelatedBindings(query.key(), bindings);
    try (frame) {
      var references = query.references();
      return runQuery(normalize(query.sql()), () -> {
        var namedBindings = new StringJoiner(", ");
        for (int index = 0; index < bindings.size(); index++) {
          var reference = references.get(index);
          String qualifier = reference.qualifier() == null
              ? ""
              : reference.qualifier().replaceFirst("^[UQ]:", "") + ".";
          namedBindings.add(qualifier + reference.column() + "=" + bindings.get(index));
        }
        return select + " [outer bindings: " + namedBindings + "]";
      }, existenceOnly);
    }
  }

  /**
   * An analyzed query whose outer references occupy stable typed slots.
   *
   * @param sql
   *          stable SQL containing binding placeholders
   * @param key
   *          binding-frame identity
   * @param references
   *          outer references in slot order
   */
  private record BoundQuery(String sql, String key, List<CorrelatedSubqueryRewriter.QualifiedColumn> references) {
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
        Map<Identifier, WithItem<?>> declarations = new LinkedHashMap<>();
        for (var cte : scope.getWithItemsList()) {
          declarations.put(Identifier.of(cte.getAliasName()), cte);
        }
        Identifier name = Identifier.of(table.getName());
        WithItem<?> declaration = name == null ? null : name.resolve(declarations);
        if (declaration != null) {
          return cteColumns(declaration);
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
    return runQuery(sql, () -> sql, false);
  }

  /**
   * Executes a stable shape with a separate user-facing diagnostic description.
   *
   * @param sql
   *          executable SQL shape
   * @param description
   *          diagnostic text computed only on failure
   * @param existenceOnly
   *          whether to check for a row without accessing projected values
   * @return materialized subquery result
   */
  private SubqueryResult runQuery(String sql, java.util.function.Supplier<String> description, boolean existenceOnly) {
    try {
      PreparedStatement prepared = preparedStatements.get(sql);
      if (prepared == null) {
        prepared = prepareStatement(sql);
        if (prepared instanceof JParqPreparedStatement nested && nested.isReusableSubqueryShape()) {
          preparedStatements.put(sql, prepared);
        }
      }
      // Cached statements are closed by this executor; all results are per-call.
      PreparedStatement perCall = preparedStatements.containsKey(sql) ? null : prepared;
      try (perCall; ResultSet rs = prepared.executeQuery()) {
        if (existenceOnly) {
          return new SubqueryResult(List.of(), rs.next() ? List.of(List.of()) : List.of());
        }
        return consume(rs);
      }
    } catch (SQLException | RuntimeException e) {
      throw new IllegalStateException("Failed to execute sub query: " + description.get(), e);
    }
  }

  /**
   * Captures a per-call result without retaining a live reader.
   *
   * @param rs
   *          nested result owned by the caller
   * @return materialized rows and labels
   * @throws SQLException
   *           if metadata or iteration fails
   */
  private SubqueryResult consume(ResultSet rs) throws SQLException {
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
  }

  /**
   * Closes cached statements after their owning result or join is closed.
   *
   * @throws SQLException
   *           if a cached statement cannot be closed
   */
  @Override
  public void close() throws SQLException {
    SQLException failure = null;
    for (PreparedStatement prepared : preparedStatements.values()) {
      try {
        prepared.close();
      } catch (SQLException exception) {
        if (failure == null) {
          failure = exception;
        } else {
          failure.addSuppressed(exception);
        }
      }
    }
    preparedStatements.clear();
    projectionSchemas.clear();
    cache.clear();
    existenceCache.clear();
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Resolves a scalar interval or datetime declaration in the subquery's own
   * input scope.
   *
   * @param select
   *          scalar subquery to inspect
   * @return its declared interval or datetime schema, or null if it cannot be
   *         established
   */
  public org.apache.avro.Schema projectionSchema(Select select) {
    String sql = select.toString();
    return projectionSchemas.computeIfAbsent(sql, shape -> {
      try (PreparedStatement prepared = prepareStatement(shape)) {
        return Optional
            .ofNullable(prepared instanceof JParqPreparedStatement nested ? nested.declaredProjectionSchema() : null);
      } catch (SQLException exception) {
        if (QueryCancellation.findCancellation(exception).isPresent()) {
          throw new IllegalStateException("Canceled while resolving scalar projection metadata", exception);
        }
        return Optional.empty();
      }
    }).orElse(null);
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
   * Parsed and schema-validated shapes owned by one nested execution tree. No
   * rows, readers, statements, cancellation state, or CTE results are cached.
   */
  public static final class ExecutionPlans {
    private final Map<String, ExecutionPlans> children = new LinkedHashMap<>();
    private final Map<JParqConnection.TableLocation, ParquetTable> tables = new LinkedHashMap<>();
    private final Map<Select, BoundQuery> correlations = new IdentityHashMap<>();
    private final Map<SqlParser.TableReference, LateralQuery> laterals = new IdentityHashMap<>();
    private SqlParser.Query query;

    /** Creates an empty execution-local shape cache. */
    public ExecutionPlans() {
    }

    /**
     * Obtains a child shape scoped to its declaring query and SQL text.
     *
     * @param sql
     *          stable nested SQL
     * @return the child cache
     */
    public ExecutionPlans child(String sql) {
      return children.computeIfAbsent(sql, ignored -> new ExecutionPlans());
    }

    /**
     * Reuses one binding identity and rewritten SQL per lexical LATERAL input.
     * Values and schemas remain owned by each current materialization.
     *
     * @param reference
     *          parsed LATERAL input in this query shape
     * @param qualifiers
     *          preceding table qualifiers
     * @return stable binding slots and SQL for this input
     * @throws SQLException
     *           if the LATERAL query cannot be parsed
     */
    public LateralQuery lateral(SqlParser.TableReference reference, Collection<String> qualifiers) throws SQLException {
      LateralQuery cached = laterals.get(reference);
      if (cached != null) {
        return cached;
      }
      Select parsed;
      try {
        parsed = (Select) JSqlParserSupport.parseStatement(reference.subquerySql());
      } catch (Exception exception) {
        throw new SQLException("Failed to parse LATERAL subquery", exception);
      }
      String key = UUID.randomUUID().toString();
      var references = new LinkedHashMap<CorrelatedSubqueryRewriter.QualifiedColumn, Integer>();
      var rewritten = CorrelatedSubqueryRewriter.rewrite(parsed, qualifiers, (qualifier, column) -> {
        var outer = new CorrelatedSubqueryRewriter.QualifiedColumn(qualifier, column);
        int index = references.computeIfAbsent(outer, ignored -> references.size());
        return new CorrelatedBindings.Reference(key, index);
      });
      var result = new LateralQuery(rewritten.sql(), key, List.copyOf(references.keySet()));
      laterals.put(reference, result);
      return result;
    }

    /**
     * Parses this shape once; callers supply the same SQL throughout its life.
     *
     * @param sql
     *          stable SQL identifying this shape
     * @return parsed query
     */
    public SqlParser.Query query(String sql) {
      if (query == null) {
        query = SqlParser.parseQuery(sql);
      }
      return query;
    }

    /**
     * Validates a physical input once within this query shape.
     *
     * @param location
     *          physical or logical input
     * @param configuration
     *          footer reader configuration
     * @return validated table
     * @throws SQLException
     *           if a footer cannot be read or schemas disagree
     */
    public ParquetTable table(JParqConnection.TableLocation location,
        org.apache.hadoop.conf.Configuration configuration) throws SQLException {
      ParquetTable table = tables.get(location);
      if (table == null) {
        table = ParquetTable.validate(location, configuration);
        tables.put(location, table);
      }
      return table;
    }
  }

  /**
   * Stable SQL and binding slots for one lexical LATERAL input.
   *
   * @param sql
   *          rewritten SQL
   * @param key
   *          binding frame identity
   * @param references
   *          outer columns in binding-slot order
   */
  public record LateralQuery(String sql, String key, List<CorrelatedSubqueryRewriter.QualifiedColumn> references) {
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
     * Validates scalar cardinality and extracts the value consistently for all
     * expression evaluators, including empty results.
     *
     * @return the scalar value, or null for no rows or a SQL NULL value
     * @throws IllegalArgumentException
     *           carrying SQLState 42000 if the result does not have exactly one
     *           column and at most one row
     */
    public Object scalarValue() {
      requireSingleColumn("Scalar subquery");
      if (rows.size() > 1) {
        throw SqlExceptions.syntaxFailure("Scalar subquery returned more than one row");
      }
      return rows.isEmpty() ? null : rows.getFirst().getFirst();
    }

    /**
     * Validates single-column cardinality and returns the value of each row,
     * including for empty results used by scalar IN and quantified comparisons.
     * Row-value predicates require degree-aware access to {@link #rows()}; this
     * scalar accessor does not implement row-value comparisons (issue 388).
     *
     * @return list of values from the single column
     * @throws IllegalArgumentException
     *           carrying SQLState 42000 if the result does not have exactly one
     *           column
     */
    public List<Object> singleColumnValues() {
      requireSingleColumn("Subquery");
      List<Object> values = new ArrayList<>(rows.size());
      for (List<Object> row : rows) {
        values.add(row.getFirst());
      }
      return values;
    }

    /**
     * Checks the declared column count even when no rows were returned.
     *
     * @param context
     *          subquery description for error diagnostics
     * @throws IllegalArgumentException
     *           carrying SQLState 42000 if the result does not have exactly one
     *           column
     */
    private void requireSingleColumn(String context) {
      if (columnLabels.size() != 1 || !rows.isEmpty() && rows.getFirst().size() != 1) {
        throw SqlExceptions.syntaxFailure(context + " must return exactly one column");
      }
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
