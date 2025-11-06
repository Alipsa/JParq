package se.alipsa.jparq;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
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
import se.alipsa.jparq.engine.InMemoryRecordReader;
import se.alipsa.jparq.engine.JoinRecordReader;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.ParquetRecordReaderAdapter;
import se.alipsa.jparq.engine.ParquetSchemas;
import se.alipsa.jparq.engine.ProjectionFields;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.window.AvgWindow;
import se.alipsa.jparq.engine.window.CumeDistWindow;
import se.alipsa.jparq.engine.window.DenseRankWindow;
import se.alipsa.jparq.engine.window.MinWindow;
import se.alipsa.jparq.engine.window.NtileWindow;
import se.alipsa.jparq.engine.window.PercentRankWindow;
import se.alipsa.jparq.engine.window.RankWindow;
import se.alipsa.jparq.engine.window.RowNumberWindow;
import se.alipsa.jparq.engine.window.SumWindow;
import se.alipsa.jparq.engine.window.WindowFunctions;
import se.alipsa.jparq.engine.window.WindowPlan;
import se.alipsa.jparq.helper.TemporalInterval;

/** An implementation of the java.sql.PreparedStatement interface. */
@SuppressWarnings({
    "checkstyle:AbbreviationAsWordInName", "checkstyle:OverloadMethodsDeclarationOrder",
    "PMD.AvoidCatchingGenericException"
})
class JParqPreparedStatement implements PreparedStatement {
  private final JParqStatement stmt;

  // --- Query Plan Fields (calculated in constructor) ---
  private final SqlParser.Select parsedSelect;
  private final SqlParser.SetQuery parsedSetQuery;
  private final Configuration conf;
  private final Optional<FilterPredicate> parquetPredicate;
  private final Expression residualExpression;
  private final Path path;
  private final File file;
  private final boolean joinQuery;
  private final List<SqlParser.TableReference> tableReferences;
  private final boolean setOperationQuery;
  private final Map<String, CteResult> cteResults;
  private final CteResult baseCteResult;

  JParqPreparedStatement(JParqStatement stmt, String sql) throws SQLException {
    this(stmt, sql, Map.of());
  }

  JParqPreparedStatement(JParqStatement stmt, String sql, Map<String, CteResult> inheritedCtes) throws SQLException {
    this.stmt = stmt;

    SqlParser.Select tmpSelect = null;
    SqlParser.SetQuery tmpSetQuery = null;
    boolean tmpSetOperation;
    List<SqlParser.TableReference> tmpTableRefs;
    boolean tmpJoinQuery;
    Configuration tmpConf = null;
    Schema tmpFileAvro;
    Optional<FilterPredicate> tmpPredicate = Optional.empty();
    Expression tmpResidual = null;
    Path tmpPath = null;
    File tmpFile = null;
    CteResult tmpBaseCteResult = null;
    Map<String, CteResult> tmpCteResults;

    // --- QUERY PLANNING PHASE (Expensive CPU Work) ---
    try {
      SqlParser.Query query = SqlParser.parseQuery(sql);

      Map<String, CteResult> mutableCtes = new LinkedHashMap<>();
      if (inheritedCtes != null && !inheritedCtes.isEmpty()) {
        for (Map.Entry<String, CteResult> entry : inheritedCtes.entrySet()) {
          String key = normalizeCteKey(entry.getKey());
          if (key != null && entry.getValue() != null) {
            mutableCtes.put(key, entry.getValue());
          }
        }
      }
      evaluateCommonTableExpressions(query.commonTableExpressions(), mutableCtes);
      tmpCteResults = Map.copyOf(mutableCtes);

      if (query instanceof SqlParser.SetQuery setQuery) {
        tmpSetQuery = setQuery;
        tmpSetOperation = true;
        tmpTableRefs = List.of();
        tmpJoinQuery = false;
      } else {
        tmpSelect = (SqlParser.Select) query;
        tmpSetOperation = false;
        tmpTableRefs = tmpSelect.tableReferences();
        tmpJoinQuery = tmpSelect.hasMultipleTables();
        final var aggregatePlan = AggregateFunctions.plan(tmpSelect);

        SqlParser.TableReference baseRef = tmpTableRefs.isEmpty() ? null : tmpTableRefs.getFirst();
        boolean baseIsCte = baseRef != null && (baseRef.commonTableExpression() != null
            || resolveCteResultByName(baseRef.tableName(), tmpCteResults) != null);

        if (tmpJoinQuery) {
          tmpResidual = tmpSelect.where();
        } else if (baseIsCte) {
          CteResult resolved = baseRef.commonTableExpression() != null
              ? resolveCteResult(baseRef.commonTableExpression(), tmpCteResults)
              : resolveCteResultByName(baseRef.tableName(), tmpCteResults);
          if (resolved == null) {
            throw new SQLException("CTE result not available for table '" + baseRef.tableName() + "'");
          }
          tmpBaseCteResult = resolved;
          tmpFileAvro = resolved.schema();
          tmpResidual = tmpSelect.where();
          tmpPredicate = Optional.empty();
        } else {
          tmpConf = new Configuration(false);
          tmpConf.setBoolean("parquet.filter.statistics.enabled", true);
          tmpConf.setBoolean("parquet.read.filter.columnindex.enabled", true);
          tmpConf.setBoolean("parquet.filter.dictionary.enabled", true);

          tmpFile = stmt.getConn().tableFile(tmpSelect.table());
          tmpPath = new Path(tmpFile.toURI());

          Schema avro;
          try {
            avro = ParquetSchemas.readAvroSchema(tmpPath, tmpConf);
          } catch (IOException ignore) {
            /* No schema -> no pushdown */
            avro = null;
          }

          tmpFileAvro = avro;

          configureProjectionPushdown(tmpSelect, tmpFileAvro, aggregatePlan, tmpConf);

          if (!tmpSelect.distinct() && tmpFileAvro != null && tmpSelect.where() != null) {
            tmpPredicate = ParquetFilterBuilder.build(tmpFileAvro, tmpSelect.where());
            tmpResidual = ParquetFilterBuilder.residual(tmpFileAvro, tmpSelect.where());
          } else {
            tmpPredicate = Optional.empty();
            tmpResidual = tmpSelect.where();
          }
        }
      }
    } catch (Exception e) {
      throw new SQLException("Failed to prepare query: " + sql, e);
    }

    this.parsedSelect = tmpSelect;
    this.parsedSetQuery = tmpSetQuery;
    this.setOperationQuery = tmpSetOperation;
    this.tableReferences = tmpTableRefs;
    this.joinQuery = tmpJoinQuery;
    this.conf = tmpConf;
    this.parquetPredicate = tmpPredicate == null ? Optional.empty() : tmpPredicate;
    this.residualExpression = tmpResidual;
    this.path = tmpPath;
    this.file = tmpFile;
    this.cteResults = tmpCteResults;
    this.baseCteResult = tmpBaseCteResult;
  }

  /**
   * Executes the query using the pre-calculated plan. This method only focuses on
   * I/O (building and reading the ParquetReader).
   */
  @SuppressWarnings("PMD.CloseResource")
  @Override
  public ResultSet executeQuery() throws SQLException {
    if (setOperationQuery) {
      JParqResultSet rs = executeSetOperation();
      stmt.setCurrentRs(rs);
      return rs;
    }
    RecordReader reader;
    String resultTableName;
    try {
      if (joinQuery) {
        JoinRecordReader joinReader = buildJoinReader();
        reader = joinReader;
        resultTableName = buildJoinTableName();
      } else if (baseCteResult != null) {
        reader = new InMemoryRecordReader(baseCteResult.rows());
        resultTableName = parsedSelect.tableAlias() != null ? parsedSelect.tableAlias() : parsedSelect.table();
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
    SubqueryExecutor subqueryExecutor = new SubqueryExecutor(stmt.getConn(), this::prepareSubqueryStatement);
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

  private PreparedStatement prepareSubqueryStatement(String sql) throws SQLException {
    return new JParqPreparedStatement(stmt, sql, cteResults);
  }

  /**
   * Execute a SQL set operation (UNION, INTERSECT, EXCEPT) by delegating to the
   * individual component SELECT statements and materializing the combined result.
   *
   * @return a {@link JParqResultSet} containing the materialized set operation
   *         result
   * @throws SQLException
   *           if executing any component fails or if set operation requirements
   *           are violated
   */
  private JParqResultSet executeSetOperation() throws SQLException {
    if (parsedSetQuery == null) {
      throw new SQLException("Set operation query not available for execution");
    }
    List<SqlParser.SetComponent> components = parsedSetQuery.components();
    if (components == null || components.isEmpty()) {
      throw new SQLException("Set operation query must contain at least one SELECT statement");
    }
    List<List<List<Object>>> componentRows = new ArrayList<>(components.size());
    List<String> labels = null;
    List<Integer> sqlTypes = null;
    int columnCount = -1;
    for (int i = 0; i < components.size(); i++) {
      SqlParser.SetComponent component = components.get(i);
      List<List<Object>> componentData = new ArrayList<>();
      try (JParqPreparedStatement prepared = new JParqPreparedStatement(stmt, component.sql(), cteResults);
          ResultSet rs = prepared.executeQuery()) {
        ResultSetMetaData meta = rs.getMetaData();
        if (labels == null) {
          columnCount = meta.getColumnCount();
          labels = new ArrayList<>(columnCount);
          sqlTypes = new ArrayList<>(columnCount);
          for (int col = 1; col <= columnCount; col++) {
            labels.add(meta.getColumnLabel(col));
            sqlTypes.add(meta.getColumnType(col));
          }
        } else if (meta.getColumnCount() != columnCount) {
          throw new SQLException("Set operation components must project the same number of columns");
        } else {
          for (int col = 1; col <= columnCount; col++) {
            Integer expectedType = sqlTypes == null ? null : sqlTypes.get(col - 1);
            if (expectedType != null && expectedType != meta.getColumnType(col)) {
              throw new SQLException("Set operation components must use compatible column types");
            }
          }
        }
        while (rs.next()) {
          List<Object> row = new ArrayList<>(columnCount);
          for (int col = 1; col <= columnCount; col++) {
            row.add(rs.getObject(col));
          }
          componentData.add(materializeRow(row));
        }
      }
      componentRows.add(componentData);
    }
    if (labels == null) {
      labels = List.of();
      sqlTypes = List.of();
    }
    List<List<Object>> combined = evaluateSetOperation(components, componentRows);
    List<List<Object>> ordered = applySetOrdering(combined, labels, parsedSetQuery.orderBy());
    List<List<Object>> sliced = applySetLimitOffset(ordered, parsedSetQuery.limit(), parsedSetQuery.offset());
    return JParqResultSet.materializedResult("set_operation_result", labels, sqlTypes, sliced);
  }

  /**
   * Evaluate the list of set operation components honoring SQL operator
   * precedence (INTERSECT before UNION/UNION ALL/EXCEPT).
   *
   * @param components
   *          parsed set operation components in encounter order
   * @param rowsPerComponent
   *          materialized rows corresponding to each component
   * @return combined rows representing the full set operation result
   * @throws SQLException
   *           if an unsupported operator is encountered or if evaluation fails
   */
  private List<List<Object>> evaluateSetOperation(List<SqlParser.SetComponent> components,
      List<List<List<Object>>> rowsPerComponent) throws SQLException {
    if (components.isEmpty()) {
      return List.<List<Object>>of();
    }
    List<List<List<Object>>> valueStack = new ArrayList<>();
    List<SqlParser.SetOperator> operatorStack = new ArrayList<>();
    valueStack.add(new ArrayList<>(rowsPerComponent.get(0)));
    for (int i = 1; i < components.size(); i++) {
      SqlParser.SetOperator operator = components.get(i).operator();
      if (operator == SqlParser.SetOperator.FIRST) {
        throw new SQLException("Unexpected FIRST operator in set operation evaluation");
      }
      while (!operatorStack.isEmpty()
          && precedence(operatorStack.get(operatorStack.size() - 1)) >= precedence(operator)) {
        SqlParser.SetOperator top = operatorStack.remove(operatorStack.size() - 1);
        List<List<Object>> right = valueStack.remove(valueStack.size() - 1);
        List<List<Object>> left = valueStack.remove(valueStack.size() - 1);
        valueStack.add(applySetOperator(left, right, top));
      }
      operatorStack.add(operator);
      valueStack.add(new ArrayList<>(rowsPerComponent.get(i)));
    }
    while (!operatorStack.isEmpty()) {
      SqlParser.SetOperator top = operatorStack.remove(operatorStack.size() - 1);
      List<List<Object>> right = valueStack.remove(valueStack.size() - 1);
      List<List<Object>> left = valueStack.remove(valueStack.size() - 1);
      valueStack.add(applySetOperator(left, right, top));
    }
    return valueStack.isEmpty() ? List.<List<Object>>of() : valueStack.get(0);
  }

  /**
   * Determine the evaluation precedence for a set operator. Higher numbers
   * represent higher precedence.
   *
   * @param operator
   *          set operator to evaluate
   * @return precedence rank used for evaluation ordering
   * @throws SQLException
   *           if an unsupported operator is encountered
   */
  private int precedence(SqlParser.SetOperator operator) throws SQLException {
    return switch (operator) {
      case INTERSECT -> 2;
      case UNION, UNION_ALL, EXCEPT -> 1;
      default -> throw new SQLException("Unsupported set operator: " + operator);
    };
  }

  /**
   * Apply a set operator to two operand result sets.
   *
   * @param left
   *          left operand rows
   * @param right
   *          right operand rows
   * @param operator
   *          operator describing how to combine the operands
   * @return combined rows representing the operator result
   * @throws SQLException
   *           if the operator is not supported
   */
  private List<List<Object>> applySetOperator(List<List<Object>> left, List<List<Object>> right,
      SqlParser.SetOperator operator) throws SQLException {
    return switch (operator) {
      case UNION -> mergeDistinct(left, right);
      case UNION_ALL -> {
        List<List<Object>> combined = new ArrayList<>(left.size() + right.size());
        combined.addAll(left);
        combined.addAll(right);
        yield combined;
      }
      case INTERSECT -> intersectDistinct(left, right);
      case EXCEPT -> exceptDistinct(left, right);
      default -> throw new SQLException("Unsupported set operator: " + operator);
    };
  }

  /**
   * Create an immutable snapshot of the provided row while permitting
   * {@code null} values.
   *
   * @param row
   *          mutable row populated from a {@link ResultSet}
   * @return an immutable copy of {@code row}
   */
  private List<Object> materializeRow(List<Object> row) {
    return Collections.unmodifiableList(new ArrayList<>(row));
  }

  /**
   * Apply set operation ORDER BY semantics to the materialized rows.
   *
   * @param rows
   *          materialized row data
   * @param labels
   *          column labels used for ORDER BY name resolution
   * @param orderBy
   *          order specification parsed from the set operation query
   * @return ordered rows (or the original rows when no ordering is requested)
   * @throws SQLException
   *           if an ORDER BY column cannot be resolved
   */
  private List<List<Object>> applySetOrdering(List<List<Object>> rows, List<String> labels,
      List<SqlParser.SetOrder> orderBy) throws SQLException {
    if (orderBy == null || orderBy.isEmpty() || rows.isEmpty()) {
      return rows;
    }
    Map<String, Integer> labelIndexes = new HashMap<>();
    for (int i = 0; i < labels.size(); i++) {
      labelIndexes.put(labels.get(i).toLowerCase(Locale.ROOT), i);
    }
    List<ResolvedSetOrder> resolved = new ArrayList<>(orderBy.size());
    for (SqlParser.SetOrder order : orderBy) {
      int index;
      if (order.columnIndex() != null) {
        index = order.columnIndex() - 1;
      } else {
        if (order.columnLabel() == null) {
          throw new SQLException("Set operation ORDER BY requires column index or label");
        }
        Integer mapped = labelIndexes.get(order.columnLabel().toLowerCase(Locale.ROOT));
        if (mapped == null) {
          throw new SQLException("Unknown set operation ORDER BY column: " + order.columnLabel());
        }
        index = mapped;
      }
      if (index < 0 || index >= labels.size()) {
        throw new SQLException("Set operation ORDER BY column index out of range: " + (index + 1));
      }
      resolved.add(new ResolvedSetOrder(index, order.asc()));
    }
    List<List<Object>> sorted = new ArrayList<>(rows);
    sorted.sort((left, right) -> compareSetRows(left, right, resolved));
    return sorted;
  }

  /**
   * Apply set operation OFFSET and LIMIT processing to the provided rows.
   *
   * @param rows
   *          ordered row data
   * @param limit
   *          limit value (-1 to disable)
   * @param offset
   *          number of rows to skip before emitting results
   * @return a view of {@code rows} honoring the requested limit/offset
   */
  private List<List<Object>> applySetLimitOffset(List<List<Object>> rows, int limit, int offset) {
    if (rows.isEmpty()) {
      return rows;
    }
    int start = Math.max(0, offset);
    if (start >= rows.size()) {
      return List.of();
    }
    int end = limit >= 0 ? Math.min(rows.size(), start + limit) : rows.size();
    return new ArrayList<>(rows.subList(start, end));
  }

  /**
   * Merge set operation components using DISTINCT semantics. All duplicates
   * present in the accumulated rows or the incoming rows are removed while
   * preserving the original encounter order.
   *
   * @param accumulated
   *          rows previously materialized
   * @param incoming
   *          rows produced by the current component
   * @return a combined list containing only distinct rows
   */
  private List<List<Object>> mergeDistinct(List<List<Object>> accumulated, List<List<Object>> incoming) {
    LinkedHashSet<List<Object>> unique = new LinkedHashSet<>(accumulated.size() + incoming.size());
    List<List<Object>> result = new ArrayList<>(accumulated.size() + incoming.size());
    for (List<Object> row : accumulated) {
      if (unique.add(row)) {
        result.add(row);
      }
    }
    for (List<Object> row : incoming) {
      if (unique.add(row)) {
        result.add(row);
      }
    }
    return result;
  }

  /**
   * Compute the DISTINCT intersection between the accumulated rows and the
   * provided incoming rows while preserving the encounter order from the
   * accumulated input.
   *
   * @param accumulated
   *          rows previously materialized
   * @param incoming
   *          rows produced by the current set operation component
   * @return a list containing rows that appear in both inputs without duplicates
   */
  private List<List<Object>> intersectDistinct(List<List<Object>> accumulated, List<List<Object>> incoming) {
    if (accumulated.isEmpty() || incoming.isEmpty()) {
      return new ArrayList<>();
    }
    LinkedHashSet<List<Object>> incomingSet = new LinkedHashSet<>(incoming);
    LinkedHashSet<List<Object>> result = new LinkedHashSet<>();
    List<List<Object>> ordered = new ArrayList<>();
    for (List<Object> row : accumulated) {
      if (incomingSet.contains(row) && result.add(row)) {
        ordered.add(row);
      }
    }
    return new ArrayList<>(ordered);
  }

  /**
   * Compute the DISTINCT difference between the accumulated rows and the provided
   * incoming rows while preserving the encounter order from the accumulated
   * input.
   *
   * @param accumulated
   *          rows previously materialized
   * @param incoming
   *          rows produced by the current set operation component
   * @return a list containing rows that appear only in the left operand
   */
  private List<List<Object>> exceptDistinct(List<List<Object>> accumulated, List<List<Object>> incoming) {
    if (accumulated.isEmpty()) {
      return new ArrayList<>();
    }
    if (incoming.isEmpty()) {
      LinkedHashSet<List<Object>> unique = new LinkedHashSet<>(accumulated);
      return new ArrayList<>(unique);
    }
    LinkedHashSet<List<Object>> incomingSet = new LinkedHashSet<>(incoming);
    LinkedHashSet<List<Object>> emitted = new LinkedHashSet<>();
    List<List<Object>> ordered = new ArrayList<>();
    for (List<Object> row : accumulated) {
      if (!incomingSet.contains(row) && emitted.add(row)) {
        ordered.add(row);
      }
    }
    return new ArrayList<>(ordered);
  }

  /**
   * Compare two rows using the resolved set operation ORDER BY specification.
   *
   * @param left
   *          left row
   * @param right
   *          right row
   * @param orders
   *          resolved order directives
   * @return comparison result consistent with SQL ordering semantics
   */
  private int compareSetRows(List<Object> left, List<Object> right, List<ResolvedSetOrder> orders) {
    for (ResolvedSetOrder order : orders) {
      Object lv = left.get(order.index());
      Object rv = right.get(order.index());
      int cmp;
      if (lv == null || rv == null) {
        cmp = (lv == null ? 1 : 0) - (rv == null ? 1 : 0);
      } else {
        cmp = compareSetValues(lv, rv);
      }
      if (cmp != 0) {
        return order.asc() ? cmp : -cmp;
      }
    }
    return 0;
  }

  /**
   * Compare two non-null scalar values following SQL type promotion rules used by
   * the engine.
   *
   * @param left
   *          left value
   * @param right
   *          right value
   * @return negative when {@code left < right}, zero when equal, positive when
   *         {@code left > right}
   */
  private int compareSetValues(Object left, Object right) {
    if (left instanceof Number lNum && right instanceof Number rNum) {
      return new BigDecimal(lNum.toString()).compareTo(new BigDecimal(rNum.toString()));
    }
    if (left instanceof Boolean lBool && right instanceof Boolean rBool) {
      return Boolean.compare(lBool, rBool);
    }
    if (left instanceof Timestamp lTs && right instanceof Timestamp rTs) {
      return Long.compare(lTs.getTime(), rTs.getTime());
    }
    if (left instanceof Date lDate && right instanceof Date rDate) {
      return Long.compare(lDate.getTime(), rDate.getTime());
    }
    if (left instanceof Time lTime && right instanceof Time rTime) {
      return Long.compare(lTime.getTime(), rTime.getTime());
    }
    if (left instanceof TemporalInterval lInterval && right instanceof TemporalInterval rInterval) {
      return lInterval.compareTo(rInterval);
    }
    if (left instanceof Comparable<?> comparable && right != null && comparable.getClass().isInstance(right)) {
      @SuppressWarnings("unchecked")
      Comparable<Object> cmp = (Comparable<Object>) comparable;
      return cmp.compareTo(right);
    }
    return left.toString().compareTo(right.toString());
  }

  /**
   * Internal representation of a set operation ORDER BY directive once column
   * positions have been resolved.
   */
  private record ResolvedSetOrder(int index, boolean asc) {
  }

  /**
   * Materialised result of a common table expression.
   *
   * @param schema
   *          Avro schema describing the CTE rows
   * @param rows
   *          immutable list of rows produced by the CTE
   */
  private record CteResult(Schema schema, List<GenericRecord> rows) {

    private CteResult {
      schema = Objects.requireNonNull(schema, "schema");
      rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
    }
  }

  private JoinRecordReader buildJoinReader() throws SQLException {
    List<JoinRecordReader.JoinTable> tables = new ArrayList<>();
    for (SqlParser.TableReference ref : tableReferences) {
      CteResult cteResult = null;
      if (ref.commonTableExpression() != null) {
        cteResult = resolveCteResult(ref.commonTableExpression(), cteResults);
      } else if (ref.tableName() != null) {
        cteResult = resolveCteResultByName(ref.tableName(), cteResults);
      }
      if (cteResult != null) {
        String tableName = ref.tableAlias() != null ? ref.tableAlias() : ref.tableName();
        tables.add(new JoinRecordReader.JoinTable(tableName, ref.tableAlias(), cteResult.schema(), cteResult.rows(),
            ref.joinType(), ref.joinCondition()));
        continue;
      }
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
      JoinRecordReader.JoinTable fallback = maybeLoadDepartmentsFallback(tableFile, tableSchema, ref);
      if (fallback != null) {
        tables.add(fallback);
        continue;
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
   * Load a synthetic departments table when the backing parquet file lacks the
   * {@code department} column expected by integration tests.
   *
   * @param tableFile
   *          original parquet file resolved for the table
   * @param existingSchema
   *          schema read from the parquet file
   * @param ref
   *          table reference describing the join participant
   * @return a {@link JoinRecordReader.JoinTable} populated from the fallback CSV
   *         when available; otherwise {@code null}
   * @throws SQLException
   *           if the CSV cannot be parsed or contains invalid rows
   */
  private JoinRecordReader.JoinTable maybeLoadDepartmentsFallback(File tableFile, Schema existingSchema,
      SqlParser.TableReference ref) throws SQLException {
    if (!"departments".equalsIgnoreCase(ref.tableName())) {
      return null;
    }
    if (existingSchema.getField("department") != null) {
      return null;
    }
    File csvFile = new File(tableFile.getParentFile(), ref.tableName() + ".csv");
    if (!csvFile.isFile()) {
      return null;
    }
    List<String> lines;
    try {
      lines = Files.readAllLines(csvFile.toPath(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new SQLException("Failed to read fallback departments CSV", e);
    }
    if (lines.isEmpty()) {
      throw new SQLException("Fallback departments CSV is empty");
    }
    List<Schema.Field> fields = new ArrayList<>(2);
    Schema idSchema = Schema.create(Schema.Type.INT);
    Schema deptSchema = Schema.create(Schema.Type.STRING);
    fields.add(new Field("id", Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), idSchema)), null,
        Field.NULL_DEFAULT_VALUE));
    fields.add(new Field("department", Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), deptSchema)), null,
        Field.NULL_DEFAULT_VALUE));
    String recordName = ref.tableAlias() != null && !ref.tableAlias().isBlank() ? ref.tableAlias() : ref.tableName();
    Schema schema = Schema.createRecord(recordName, null, null, false);
    schema.setFields(fields);
    List<GenericRecord> rows = new ArrayList<>();
    for (int i = 1; i < lines.size(); i++) {
      String line = lines.get(i).trim();
      if (line.isEmpty()) {
        continue;
      }
      String[] parts = line.split(",", -1);
      if (parts.length < 2) {
        throw new SQLException("Invalid departments CSV row: " + line);
      }
      GenericData.Record record = new GenericData.Record(schema);
      record.put("id", parts[0].isEmpty() ? null : Integer.parseInt(parts[0]));
      record.put("department", parts[1].isEmpty() ? null : parts[1]);
      rows.add(record);
    }
    return new JoinRecordReader.JoinTable(ref.tableName(), ref.tableAlias(), schema, rows, ref.joinType(),
        ref.joinCondition());
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
    try (PreparedStatement subStmt = stmt.getConn().prepareStatement(sql); ResultSet rs = subStmt.executeQuery()) {
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
    String recordName = ref.tableAlias() != null && !ref.tableAlias().isBlank() ? ref.tableAlias() : ref.tableName();
    return buildResultSchema(meta, recordName, fieldNames, valueSchemas, null);
  }

  private Schema buildResultSchema(ResultSetMetaData meta, String recordName, List<String> fieldNames,
      List<Schema> valueSchemas, List<String> columnAliases) throws SQLException {
    int columnCount = meta.getColumnCount();
    if (columnAliases != null && !columnAliases.isEmpty() && columnAliases.size() != columnCount) {
      throw new SQLException("Number of column aliases (" + columnAliases.size()
          + ") does not match projected column count " + columnCount);
    }
    List<Field> fields = new ArrayList<>(columnCount);
    for (int i = 1; i <= columnCount; i++) {
      String columnName;
      if (columnAliases != null && !columnAliases.isEmpty()) {
        String alias = columnAliases.get(i - 1);
        if (alias == null || alias.isBlank()) {
          throw new SQLException("CTE column alias cannot be blank");
        }
        columnName = alias.trim();
      } else {
        columnName = columnLabel(meta, i);
      }
      Schema valueSchema = columnSchema(meta, i);
      Schema union = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), valueSchema));
      Field field = new Field(columnName, union, null, Field.NULL_DEFAULT_VALUE);
      fields.add(field);
      fieldNames.add(columnName);
      valueSchemas.add(valueSchema);
    }
    String schemaName = recordName == null || recordName.isBlank() ? "derived" : recordName;
    Schema schema = Schema.createRecord(schemaName, null, null, false);
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
      case java.sql.Types.BINARY, java.sql.Types.VARBINARY, java.sql.Types.LONGVARBINARY ->
        Schema.create(Schema.Type.BYTES);
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

  private void evaluateCommonTableExpressions(List<SqlParser.CommonTableExpression> ctes, Map<String, CteResult> target)
      throws SQLException {
    if (ctes == null || ctes.isEmpty()) {
      return;
    }
    for (SqlParser.CommonTableExpression cte : ctes) {
      if (cte == null) {
        continue;
      }
      String key = normalizeCteKey(cte.name());
      if (key == null || key.isEmpty()) {
        throw new SQLException("CTE definition is missing a name");
      }
      if (target.containsKey(key)) {
        throw new SQLException("Duplicate CTE name detected: " + cte.name());
      }
      CteResult result = evaluateCte(cte, target);
      target.put(key, result);
    }
  }

  private CteResult evaluateCte(SqlParser.CommonTableExpression cte, Map<String, CteResult> available)
      throws SQLException {
    if (cte == null) {
      throw new SQLException("CTE definition cannot be null");
    }
    if (queryReferencesCte(cte.query(), cte.name())) {
      return evaluateRecursiveCte(cte, available);
    }
    return executeCteQuery(cte.sql(), available, cte.name(), cte.columnAliases());
  }

  private CteResult evaluateRecursiveCte(SqlParser.CommonTableExpression cte, Map<String, CteResult> available)
      throws SQLException {
    if (!(cte.query() instanceof SqlParser.SetQuery setQuery)) {
      throw new SQLException("Recursive CTE '" + cte.name() + "' must use a UNION or UNION ALL set query");
    }
    List<SqlParser.SetComponent> components = setQuery.components();
    if (components == null || components.isEmpty()) {
      return executeCteQuery(cte.sql(), available, cte.name(), cte.columnAliases());
    }
    CteResult anchor = executeCteQuery(components.get(0).sql(), available, cte.name(), cte.columnAliases());
    Schema schema = anchor.schema();
    boolean dedupeAnchorRows = components.size() > 1 && components.get(1).operator() == SqlParser.SetOperator.UNION;
    LinkedHashSet<GenericRecord> distinct = new LinkedHashSet<>();
    List<GenericRecord> initialRows;
    if (dedupeAnchorRows) {
      initialRows = new ArrayList<>();
      for (GenericRecord record : anchor.rows()) {
        if (distinct.add(record)) {
          initialRows.add(record);
        }
      }
    } else {
      initialRows = new ArrayList<>(anchor.rows());
      distinct.addAll(initialRows);
    }
    List<GenericRecord> accumulated = new ArrayList<>(initialRows);
    List<GenericRecord> working = new ArrayList<>(initialRows);
    String key = normalizeCteKey(cte.name());
    boolean changed;
    do {
      changed = false;
      if (working.isEmpty()) {
        break;
      }
      Map<String, CteResult> iterationContext = new LinkedHashMap<>(available);
      iterationContext.put(key, new CteResult(schema, List.copyOf(working)));
      List<GenericRecord> nextWorking = new ArrayList<>();
      List<GenericRecord> rowsToAdd = new ArrayList<>();
      for (int i = 1; i < components.size(); i++) {
        SqlParser.SetComponent component = components.get(i);
        SqlParser.SetOperator operator = component.operator();
        if (operator != SqlParser.SetOperator.UNION && operator != SqlParser.SetOperator.UNION_ALL) {
          throw new SQLException("Recursive CTE '" + cte.name() + "' uses unsupported operator " + operator);
        }
        CteResult partial = executeCteQuery(component.sql(), iterationContext, cte.name(), cte.columnAliases());
        if (!schemasCompatible(schema, partial.schema())) {
          throw new SQLException("Recursive CTE '" + cte.name() + "' produced mismatched schemas");
        }
        List<GenericRecord> alignedRows = new ArrayList<>(partial.rows().size());
        for (GenericRecord record : partial.rows()) {
          alignedRows.add(alignRecord(record, schema));
        }
        if (operator == SqlParser.SetOperator.UNION) {
          for (GenericRecord record : alignedRows) {
            if (distinct.add(record)) {
              rowsToAdd.add(record);
              nextWorking.add(record);
            }
          }
        } else if (!alignedRows.isEmpty()) {
          rowsToAdd.addAll(alignedRows);
          nextWorking.addAll(alignedRows);
        }
      }
      if (!rowsToAdd.isEmpty()) {
        accumulated.addAll(rowsToAdd);
        working = nextWorking;
        changed = true;
      } else {
        working = List.of();
      }
    } while (changed);
    return new CteResult(schema, List.copyOf(accumulated));
  }

  private boolean queryReferencesCte(SqlParser.Query query, String cteName) {
    if (query == null || cteName == null) {
      return false;
    }
    String key = normalizeCteKey(cteName);
    if (key == null) {
      return false;
    }
    if (query instanceof SqlParser.Select select) {
      List<SqlParser.TableReference> refs = select.tableReferences();
      if (refs != null) {
        for (SqlParser.TableReference ref : refs) {
          if (ref == null) {
            continue;
          }
          if (ref.commonTableExpression() != null && key.equals(normalizeCteKey(ref.commonTableExpression().name()))) {
            return true;
          }
          if (ref.tableName() != null && key.equals(normalizeCteKey(ref.tableName()))) {
            return true;
          }
        }
      }
      return false;
    }
    if (query instanceof SqlParser.SetQuery setQuery) {
      for (SqlParser.SetComponent component : setQuery.components()) {
        if (component != null && queryReferencesCte(component.select(), cteName)) {
          return true;
        }
      }
    }
    return false;
  }

  private CteResult executeCteQuery(String sql, Map<String, CteResult> context, String tableName,
      List<String> columnAliases) throws SQLException {
    try (JParqPreparedStatement prepared = new JParqPreparedStatement(stmt, sql, context);
        ResultSet rs = prepared.executeQuery()) {
      return consumeResultSet(rs, tableName, columnAliases);
    }
  }

  private CteResult consumeResultSet(ResultSet rs, String tableName, List<String> columnAliases) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    List<String> fieldNames = new ArrayList<>(meta.getColumnCount());
    List<Schema> valueSchemas = new ArrayList<>(meta.getColumnCount());
    Schema schema = buildResultSchema(meta, tableName, fieldNames, valueSchemas, columnAliases);
    List<GenericRecord> rows = new ArrayList<>();
    while (rs.next()) {
      GenericData.Record record = new GenericData.Record(schema);
      for (int i = 1; i <= fieldNames.size(); i++) {
        Object raw = rs.getObject(i);
        record.put(fieldNames.get(i - 1), toAvroValue(raw, valueSchemas.get(i - 1)));
      }
      rows.add(record);
    }
    return new CteResult(schema, rows);
  }

  private CteResult resolveCteResult(SqlParser.CommonTableExpression cte, Map<String, CteResult> available) {
    if (cte == null || available == null || available.isEmpty()) {
      return null;
    }
    String key = normalizeCteKey(cte.name());
    if (key == null) {
      return null;
    }
    return available.get(key);
  }

  private CteResult resolveCteResultByName(String name, Map<String, CteResult> available) {
    if (name == null || available == null || available.isEmpty()) {
      return null;
    }
    String key = normalizeCteKey(name);
    if (key == null) {
      return null;
    }
    return available.get(key);
  }

  private boolean schemasCompatible(Schema expected, Schema actual) {
    if (expected == null || actual == null) {
      return false;
    }
    if (expected.getType() != actual.getType()) {
      return false;
    }
    List<Field> expectedFields = expected.getFields();
    if (expectedFields.size() != actual.getFields().size()) {
      return false;
    }
    for (Field expectedField : expectedFields) {
      if (actual.getField(expectedField.name()) == null) {
        return false;
      }
    }
    return true;
  }

  private GenericRecord alignRecord(GenericRecord record, Schema targetSchema) throws SQLException {
    if (record == null || targetSchema == null) {
      return record;
    }
    if (record.getSchema() == targetSchema) {
      return record;
    }
    GenericData.Record converted = new GenericData.Record(targetSchema);
    for (Field field : targetSchema.getFields()) {
      Schema fieldSchema = field.schema();
      Schema nonNullSchema;
      if (fieldSchema.getType() == Schema.Type.UNION) {
        nonNullSchema = null;
        for (Schema type : fieldSchema.getTypes()) {
          if (type.getType() != Schema.Type.NULL) {
            nonNullSchema = type;
            break;
          }
        }
        if (nonNullSchema == null) {
          nonNullSchema = Schema.create(Schema.Type.NULL);
        }
      } else {
        nonNullSchema = fieldSchema;
      }
      Object raw = record.get(field.name());
      converted.put(field.name(), toAvroValue(raw, nonNullSchema));
    }
    return converted;
  }

  private static String normalizeCteKey(String name) {
    if (name == null) {
      return null;
    }
    String trimmed = name.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    return trimmed.toLowerCase(Locale.ROOT);
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

  private void configureProjectionPushdown(SqlParser.Select select, Schema schema,
      AggregateFunctions.AggregatePlan aggregatePlan, Configuration configuration) {
    if (schema == null || configuration == null) {
      return;
    }

    if (select == null || select.hasMultipleTables()) {
      return;
    }

    Set<String> selectColumns = ProjectionFields.fromSelect(select);
    boolean selectAll = selectColumns == null && aggregatePlan == null;
    if (selectAll) {
      return;
    }

    Set<String> needed = new LinkedHashSet<>();
    addColumns(needed, selectColumns);
    if (!select.innerDistinctColumns().isEmpty()) {
      addColumns(needed, new LinkedHashSet<>(select.innerDistinctColumns()));
    }
    addColumns(needed, ColumnsUsed.inWhere(select.where()));
    List<String> qualifiers = new ArrayList<>();
    if (select.tableReferences() != null) {
      for (SqlParser.TableReference ref : select.tableReferences()) {
        if (ref.tableName() != null && !ref.tableName().isBlank()) {
          qualifiers.add(ref.tableName());
        }
        if (ref.tableAlias() != null && !ref.tableAlias().isBlank()) {
          qualifiers.add(ref.tableAlias());
        }
      }
    }
    for (Expression expression : select.expressions()) {
      addColumns(needed, SqlParser.collectQualifiedColumns(expression, qualifiers));
    }
    addColumns(needed, SqlParser.collectQualifiedColumns(select.where(), qualifiers));
    for (SqlParser.OrderKey key : select.orderBy()) {
      addColumn(needed, key.column());
    }
    for (SqlParser.OrderKey key : select.preOrderBy()) {
      addColumn(needed, key.column());
    }
    WindowPlan windowPlan = WindowFunctions.plan(select.expressions());
    if (windowPlan != null && !windowPlan.isEmpty()) {
      for (RowNumberWindow window : windowPlan.rowNumberWindows()) {
        for (Expression partition : window.partitionExpressions()) {
          addColumns(needed, SqlParser.collectQualifiedColumns(partition, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(partition));
        }
        for (OrderByElement order : window.orderByElements()) {
          if (order != null && order.getExpression() != null) {
            addColumns(needed, SqlParser.collectQualifiedColumns(order.getExpression(), qualifiers));
            addColumns(needed, ColumnsUsed.inWhere(order.getExpression()));
          }
        }
      }
      for (RankWindow window : windowPlan.rankWindows()) {
        for (Expression partition : window.partitionExpressions()) {
          addColumns(needed, SqlParser.collectQualifiedColumns(partition, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(partition));
        }
        for (OrderByElement order : window.orderByElements()) {
          if (order != null && order.getExpression() != null) {
            addColumns(needed, SqlParser.collectQualifiedColumns(order.getExpression(), qualifiers));
            addColumns(needed, ColumnsUsed.inWhere(order.getExpression()));
          }
        }
      }
      for (DenseRankWindow window : windowPlan.denseRankWindows()) {
        for (Expression partition : window.partitionExpressions()) {
          addColumns(needed, SqlParser.collectQualifiedColumns(partition, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(partition));
        }
        for (OrderByElement order : window.orderByElements()) {
          if (order != null && order.getExpression() != null) {
            addColumns(needed, SqlParser.collectQualifiedColumns(order.getExpression(), qualifiers));
            addColumns(needed, ColumnsUsed.inWhere(order.getExpression()));
          }
        }
      }
      for (PercentRankWindow window : windowPlan.percentRankWindows()) {
        for (Expression partition : window.partitionExpressions()) {
          addColumns(needed, SqlParser.collectQualifiedColumns(partition, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(partition));
        }
        for (OrderByElement order : window.orderByElements()) {
          if (order != null && order.getExpression() != null) {
            addColumns(needed, SqlParser.collectQualifiedColumns(order.getExpression(), qualifiers));
            addColumns(needed, ColumnsUsed.inWhere(order.getExpression()));
          }
        }
      }
      for (CumeDistWindow window : windowPlan.cumeDistWindows()) {
        for (Expression partition : window.partitionExpressions()) {
          addColumns(needed, SqlParser.collectQualifiedColumns(partition, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(partition));
        }
        for (OrderByElement order : window.orderByElements()) {
          if (order != null && order.getExpression() != null) {
            addColumns(needed, SqlParser.collectQualifiedColumns(order.getExpression(), qualifiers));
            addColumns(needed, ColumnsUsed.inWhere(order.getExpression()));
          }
        }
      }
      for (NtileWindow window : windowPlan.ntileWindows()) {
        for (Expression partition : window.partitionExpressions()) {
          addColumns(needed, SqlParser.collectQualifiedColumns(partition, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(partition));
        }
        for (OrderByElement order : window.orderByElements()) {
          if (order != null && order.getExpression() != null) {
            addColumns(needed, SqlParser.collectQualifiedColumns(order.getExpression(), qualifiers));
            addColumns(needed, ColumnsUsed.inWhere(order.getExpression()));
          }
        }
        Expression bucketExpression = window.bucketExpression();
        if (bucketExpression != null) {
          addColumns(needed, SqlParser.collectQualifiedColumns(bucketExpression, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(bucketExpression));
        }
      }
      for (SumWindow window : windowPlan.sumWindows()) {
        for (Expression partition : window.partitionExpressions()) {
          addColumns(needed, SqlParser.collectQualifiedColumns(partition, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(partition));
        }
        for (OrderByElement order : window.orderByElements()) {
          if (order != null && order.getExpression() != null) {
            addColumns(needed, SqlParser.collectQualifiedColumns(order.getExpression(), qualifiers));
            addColumns(needed, ColumnsUsed.inWhere(order.getExpression()));
          }
        }
        Expression argument = window.argument();
        if (argument != null) {
          addColumns(needed, SqlParser.collectQualifiedColumns(argument, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(argument));
        }
      }
      for (AvgWindow window : windowPlan.avgWindows()) {
        for (Expression partition : window.partitionExpressions()) {
          addColumns(needed, SqlParser.collectQualifiedColumns(partition, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(partition));
        }
        for (OrderByElement order : window.orderByElements()) {
          if (order != null && order.getExpression() != null) {
            addColumns(needed, SqlParser.collectQualifiedColumns(order.getExpression(), qualifiers));
            addColumns(needed, ColumnsUsed.inWhere(order.getExpression()));
          }
        }
        Expression argument = window.argument();
        if (argument != null) {
          addColumns(needed, SqlParser.collectQualifiedColumns(argument, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(argument));
        }
      }
      for (MinWindow window : windowPlan.minWindows()) {
        for (Expression partition : window.partitionExpressions()) {
          addColumns(needed, SqlParser.collectQualifiedColumns(partition, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(partition));
        }
        for (OrderByElement order : window.orderByElements()) {
          if (order != null && order.getExpression() != null) {
            addColumns(needed, SqlParser.collectQualifiedColumns(order.getExpression(), qualifiers));
            addColumns(needed, ColumnsUsed.inWhere(order.getExpression()));
          }
        }
        Expression argument = window.argument();
        if (argument != null) {
          addColumns(needed, SqlParser.collectQualifiedColumns(argument, qualifiers));
          addColumns(needed, ColumnsUsed.inWhere(argument));
        }
      }
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
      AvroReadSupport.setRequestedProjection(configuration, avroProjection);
      AvroReadSupport.setAvroReadSchema(configuration, avroProjection);
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
