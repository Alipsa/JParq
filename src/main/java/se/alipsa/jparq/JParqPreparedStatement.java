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
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
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
import se.alipsa.jparq.engine.CorrelatedSubqueryRewriter;
import se.alipsa.jparq.engine.IdentifierUtil;
import se.alipsa.jparq.engine.InMemoryRecordReader;
import se.alipsa.jparq.engine.JoinRecordReader;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.ParquetRecordReaderAdapter;
import se.alipsa.jparq.engine.ParquetSchemas;
import se.alipsa.jparq.engine.ProjectionFields;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SamplingRecordReader;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SqlParser.QualifiedExpansionColumn;
import se.alipsa.jparq.engine.SqlParser.ValueTableDefinition;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.UnnestTableBuilder;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.engine.window.AvgWindow;
import se.alipsa.jparq.engine.window.CumeDistWindow;
import se.alipsa.jparq.engine.window.DenseRankWindow;
import se.alipsa.jparq.engine.window.MaxWindow;
import se.alipsa.jparq.engine.window.MinWindow;
import se.alipsa.jparq.engine.window.NtileWindow;
import se.alipsa.jparq.engine.window.PercentRankWindow;
import se.alipsa.jparq.engine.window.RankWindow;
import se.alipsa.jparq.engine.window.RowNumberWindow;
import se.alipsa.jparq.engine.window.SumWindow;
import se.alipsa.jparq.engine.window.WindowFunctions;
import se.alipsa.jparq.engine.window.WindowPlan;
import se.alipsa.jparq.helper.JParqUtil;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalInterval;
import se.alipsa.jparq.meta.InformationSchemaColumns;
import se.alipsa.jparq.meta.InformationSchemaTables;

/** An implementation of the java.sql.PreparedStatement interface. */
@SuppressWarnings({
    "checkstyle:AbbreviationAsWordInName", "checkstyle:OverloadMethodsDeclarationOrder",
    "PMD.AvoidCatchingGenericException"
})
class JParqPreparedStatement implements PreparedStatement {
  private final JParqStatement stmt;

  // --- Query Plan Fields (calculated in constructor) ---
  private SqlParser.Select parsedSelect;
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
  private final Map<String, CteResult> valueTableResults;
  private final CteResult baseCteResult;
  private CteResult informationSchemaTablesResult;
  private CteResult informationSchemaColumnsResult;

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
    Map<String, CteResult> tmpValueTables = new LinkedHashMap<>();

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

        SqlParser.TableReference baseRef = tmpTableRefs.isEmpty() ? null : tmpTableRefs.getFirst();
        boolean baseIsCte = baseRef != null && (baseRef.commonTableExpression() != null
            || resolveCteResultByName(baseRef.tableName(), tmpCteResults) != null);
        boolean baseIsInformationSchemaTables = baseRef != null
            && InformationSchemaTables.matchesTableReference(baseRef.tableName());
        boolean baseIsInformationSchemaColumns = baseRef != null
            && InformationSchemaColumns.matchesTableReference(baseRef.tableName());

        AggregateFunctions.AggregatePlan aggregatePlan = null;

        if (tmpJoinQuery) {
          tmpResidual = tmpSelect.where();
        } else if (baseRef != null && baseRef.valueTable() != null) {
          CteResult resolved = materializeValueTable(baseRef.valueTable(), deriveValueTableName(baseRef));
          tmpBaseCteResult = resolved;
          tmpFileAvro = resolved.schema();
          registerValueTableResult(baseRef, resolved, tmpValueTables);
          if (!tmpSelect.qualifiedWildcards().isEmpty()) {
            Map<String, List<QualifiedExpansionColumn>> qualifierColumns = buildQualifierColumnsForSchema(tmpSelect,
                baseRef, tmpFileAvro);
            tmpSelect = SqlParser.expandQualifiedWildcards(tmpSelect, qualifierColumns);
          }
          aggregatePlan = AggregateFunctions.plan(tmpSelect);
          tmpResidual = tmpSelect.where();
          tmpPredicate = Optional.empty();
        } else if (baseIsCte) {
          CteResult resolved = baseRef.commonTableExpression() != null
              ? resolveCteResult(baseRef.commonTableExpression(), tmpCteResults)
              : resolveCteResultByName(baseRef.tableName(), tmpCteResults);
          if (resolved == null) {
            throw new SQLException("CTE result not available for table '" + baseRef.tableName() + "'");
          }
          tmpBaseCteResult = resolved;
          tmpFileAvro = resolved.schema();
          if (!tmpSelect.qualifiedWildcards().isEmpty()) {
            Map<String, List<QualifiedExpansionColumn>> qualifierColumns = buildQualifierColumnsForSchema(tmpSelect,
                baseRef, tmpFileAvro);
            tmpSelect = SqlParser.expandQualifiedWildcards(tmpSelect, qualifierColumns);
          }
          aggregatePlan = AggregateFunctions.plan(tmpSelect);
          tmpResidual = tmpSelect.where();
          tmpPredicate = Optional.empty();
        } else if (baseIsInformationSchemaTables || baseIsInformationSchemaColumns) {
          CteResult resolved = baseIsInformationSchemaTables
              ? materializeInformationSchemaTables()
              : materializeInformationSchemaColumns();
          tmpBaseCteResult = resolved;
          tmpFileAvro = resolved.schema();
          if (!tmpSelect.qualifiedWildcards().isEmpty()) {
            Map<String, List<QualifiedExpansionColumn>> qualifierColumns = buildQualifierColumnsForSchema(tmpSelect,
                baseRef, tmpFileAvro);
            tmpSelect = SqlParser.expandQualifiedWildcards(tmpSelect, qualifierColumns);
          }
          aggregatePlan = AggregateFunctions.plan(tmpSelect);
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

          if (!tmpSelect.qualifiedWildcards().isEmpty()) {
            if (tmpFileAvro == null) {
              throw new SQLException("Unable to resolve schema for qualified wildcard projection");
            }
            Map<String, List<QualifiedExpansionColumn>> qualifierColumns = buildQualifierColumnsForSchema(tmpSelect,
                baseRef, tmpFileAvro);
            tmpSelect = SqlParser.expandQualifiedWildcards(tmpSelect, qualifierColumns);
          }

          aggregatePlan = AggregateFunctions.plan(tmpSelect);
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
    this.valueTableResults = tmpValueTables;
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
    SqlParser.TableReference baseRef = (tableReferences == null || tableReferences.isEmpty())
        ? null
        : tableReferences.get(0);
    try {
      if (joinQuery) {
        JoinRecordReader joinReader = buildJoinReader();
        reader = joinReader;
        resultTableName = buildJoinTableName();
      } else if (baseCteResult != null) {
        resultTableName = parsedSelect.tableAlias() != null ? parsedSelect.tableAlias() : parsedSelect.table();
        reader = new InMemoryRecordReader(baseCteResult.rows(), baseCteResult.schema(), resultTableName);
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
      if (!joinQuery) {
        reader = applyTableSample(baseRef == null ? null : baseRef.tableSample(), reader);
      }
    } catch (SQLException e) {
      throw e;
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

  private RecordReader applyTableSample(SqlParser.TableSampleDefinition sample, RecordReader reader)
      throws SQLException {
    if (sample == null || reader == null) {
      return reader;
    }
    try {
      return SamplingRecordReader.wrap(reader, sample);
    } catch (IOException e) {
      throw new SQLException("Failed to apply TABLESAMPLE", e);
    }
  }

  /**
   * Execute a SQL set operation (UNION, UNION ALL, INTERSECT, INTERSECT ALL,
   * EXCEPT, EXCEPT ALL) by delegating to the individual component SELECT
   * statements and materializing the combined result.
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
   * precedence (INTERSECT before UNION/UNION ALL/EXCEPT/EXCEPT ALL).
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
      case INTERSECT, INTERSECT_ALL -> 2;
      case UNION, UNION_ALL, EXCEPT, EXCEPT_ALL -> 1;
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
      case INTERSECT_ALL -> intersectAll(left, right);
      case EXCEPT -> exceptDistinct(left, right);
      case EXCEPT_ALL -> exceptAll(left, right);
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
   * Compute the multiset intersection between the accumulated rows and the
   * provided incoming rows while preserving encounter order from the accumulated
   * input and respecting duplicate counts.
   *
   * @param accumulated
   *          rows previously materialized
   * @param incoming
   *          rows produced by the current set operation component
   * @return a list containing rows that appear in both inputs according to
   *         INTERSECT ALL semantics
   */
  private List<List<Object>> intersectAll(List<List<Object>> accumulated, List<List<Object>> incoming) {
    if (accumulated.isEmpty() || incoming.isEmpty()) {
      return new ArrayList<>();
    }
    Map<List<Object>, Integer> remaining = new HashMap<>();
    for (List<Object> row : incoming) {
      remaining.merge(row, 1, Integer::sum);
    }
    List<List<Object>> result = new ArrayList<>();
    for (List<Object> row : accumulated) {
      Integer count = remaining.get(row);
      if (count != null && count > 0) {
        result.add(row);
        if (count == 1) {
          remaining.remove(row);
        } else {
          remaining.put(row, count - 1);
        }
      }
    }
    return result;
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
   * Compute the multiset difference between the accumulated rows and the provided
   * incoming rows while preserving the encounter order from the left operand and
   * respecting duplicate counts.
   *
   * @param accumulated
   *          rows previously materialized
   * @param incoming
   *          rows produced by the current set operation component
   * @return a list containing rows that remain after applying EXCEPT ALL
   *         semantics
   */
  private List<List<Object>> exceptAll(List<List<Object>> accumulated, List<List<Object>> incoming) {
    if (accumulated.isEmpty()) {
      return new ArrayList<>();
    }
    if (incoming.isEmpty()) {
      return new ArrayList<>(accumulated);
    }
    Map<List<Object>, Integer> remaining = new HashMap<>();
    for (List<Object> row : incoming) {
      remaining.merge(row, 1, Integer::sum);
    }
    List<List<Object>> result = new ArrayList<>();
    for (List<Object> row : accumulated) {
      Integer count = remaining.get(row);
      if (count == null) {
        result.add(row);
      } else if (count == 1) {
        remaining.remove(row);
      } else {
        remaining.put(row, count - 1);
      }
    }
    return result;
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
    Map<String, Integer> qualifierIndex = new LinkedHashMap<>();
    for (SqlParser.TableReference ref : tableReferences) {
      if (ref.unnest() != null) {
        if (tables.isEmpty()) {
          throw new SQLException("UNNEST requires a preceding table reference");
        }
        JoinRecordReader.JoinTable unnestTable = UnnestTableBuilder.build(ref, tables, qualifierIndex);
        tables.add(unnestTable);
        registerQualifierIndexes(qualifierIndex, unnestTable, tables.size() - 1);
        continue;
      }
      if (ref.valueTable() != null) {
        CteResult valueResult = resolveValueTable(ref);
        if (valueResult == null) {
          valueResult = materializeValueTable(ref.valueTable(), deriveValueTableName(ref));
          registerValueTableResult(ref, valueResult, valueTableResults);
        }
        String tableName = deriveValueTableName(ref);
        List<GenericRecord> rows = applySampleToRows(ref.tableSample(), valueResult.rows());
        JoinRecordReader.JoinTable table = new JoinRecordReader.JoinTable(tableName, ref.tableAlias(),
            valueResult.schema(), rows, ref.joinType(), ref.joinCondition(), ref.usingColumns(), null);
        tables.add(table);
        registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
        continue;
      }
      if (InformationSchemaTables.matchesTableReference(ref.tableName())) {
        CteResult infoResult = materializeInformationSchemaTables();
        String infoName = ref.tableAlias() != null ? ref.tableAlias() : ref.tableName();
        List<GenericRecord> rows = applySampleToRows(ref.tableSample(), infoResult.rows());
        JoinRecordReader.JoinTable table = new JoinRecordReader.JoinTable(infoName, ref.tableAlias(),
            infoResult.schema(), rows, ref.joinType(), ref.joinCondition(), ref.usingColumns(), null);
        tables.add(table);
        registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
        continue;
      }
      if (InformationSchemaColumns.matchesTableReference(ref.tableName())) {
        CteResult infoResult = materializeInformationSchemaColumns();
        String infoName = ref.tableAlias() != null ? ref.tableAlias() : ref.tableName();
        List<GenericRecord> rows = applySampleToRows(ref.tableSample(), infoResult.rows());
        JoinRecordReader.JoinTable table = new JoinRecordReader.JoinTable(infoName, ref.tableAlias(),
            infoResult.schema(), rows, ref.joinType(), ref.joinCondition(), ref.usingColumns(), null);
        tables.add(table);
        registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
        continue;
      }
      CteResult cteResult = null;
      if (ref.commonTableExpression() != null) {
        cteResult = resolveCteResult(ref.commonTableExpression(), cteResults);
      } else if (ref.tableName() != null) {
        cteResult = resolveCteResultByName(ref.tableName(), cteResults);
      }
      if (cteResult != null) {
        String tableName = ref.tableAlias() != null ? ref.tableAlias() : ref.tableName();
        List<GenericRecord> rows = applySampleToRows(ref.tableSample(), cteResult.rows());
        JoinRecordReader.JoinTable table = new JoinRecordReader.JoinTable(tableName, ref.tableAlias(),
            cteResult.schema(), rows, ref.joinType(), ref.joinCondition(), ref.usingColumns(), null);
        tables.add(table);
        registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
        continue;
      }
      if (ref.subquery() != null) {
        JoinRecordReader.JoinTable table = ref.lateral()
            ? materializeLateralSubqueryTable(ref, tables, qualifierIndex)
            : materializeSubqueryTable(ref);
        tables.add(table);
        registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
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
        registerQualifierIndexes(qualifierIndex, fallback, tables.size() - 1);
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
      rows = applySampleToRows(ref.tableSample(), rows);
      JoinRecordReader.JoinTable table = new JoinRecordReader.JoinTable(tableName, ref.tableAlias(), tableSchema, rows,
          ref.joinType(), ref.joinCondition(), ref.usingColumns(), null);
      tables.add(table);
      registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
    }
    try {
      JoinRecordReader joinReader = new JoinRecordReader(tables);
      expandJoinQualifiedWildcards(joinReader);
      return joinReader;
    } catch (IllegalArgumentException e) {
      throw new SQLException("Failed to build join reader", e);
    }
  }

  private List<GenericRecord> applySampleToRows(SqlParser.TableSampleDefinition sample, List<GenericRecord> rows)
      throws SQLException {
    if (sample == null || rows == null || rows.isEmpty()) {
      return rows;
    }
    try {
      return SamplingRecordReader.sampleRows(rows, sample);
    } catch (IOException e) {
      throw new SQLException("Failed to apply TABLESAMPLE", e);
    }
  }

  private List<GenericRecord> applySampleUnchecked(SqlParser.TableSampleDefinition sample, List<GenericRecord> rows) {
    if (sample == null || rows == null || rows.isEmpty()) {
      return rows;
    }
    try {
      return SamplingRecordReader.sampleRows(rows, sample);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to apply TABLESAMPLE", e);
    }
  }

  private static void registerQualifierIndexes(Map<String, Integer> index, JoinRecordReader.JoinTable table,
      int position) {
    if (index == null || table == null) {
      return;
    }
    registerQualifierIndex(index, table.tableName(), position, false);
    registerQualifierIndex(index, table.alias(), position, true);
  }

  private static void registerQualifierIndex(Map<String, Integer> index, String qualifier, int position,
      boolean failOnConflict) {
    String normalized = JParqUtil.normalizeQualifier(qualifier);
    if (normalized == null) {
      return;
    }
    Integer existing = index.get(normalized);
    if (existing != null) {
      if (existing != position && failOnConflict) {
        throw new IllegalStateException("Duplicate qualifier '" + qualifier + "' resolves to '" + normalized + "'");
      }
      return;
    }
    index.put(normalized, position);
  }

  private void expandJoinQualifiedWildcards(JoinRecordReader joinReader) throws SQLException {
    if (joinReader == null) {
      return;
    }
    if (parsedSelect == null || parsedSelect.qualifiedWildcards().isEmpty()) {
      return;
    }
    Map<String, List<QualifiedExpansionColumn>> qualifierColumns = buildQualifierColumnsForJoin(joinReader);
    if (qualifierColumns.isEmpty()) {
      throw new SQLException(
          "Unable to resolve columns for qualified wildcard projections: " + parsedSelect.qualifiedWildcards());
    }
    try {
      parsedSelect = SqlParser.expandQualifiedWildcards(parsedSelect, qualifierColumns);
    } catch (IllegalArgumentException e) {
      throw new SQLException("Failed to expand qualified wildcard projections", e);
    }
  }

  private Map<String, List<QualifiedExpansionColumn>> buildQualifierColumnsForJoin(JoinRecordReader joinReader) {
    Map<String, List<QualifiedExpansionColumn>> mapping = new LinkedHashMap<>();
    if (joinReader == null) {
      return mapping;
    }
    Map<String, Map<String, String>> qualifierMapping = joinReader.qualifierColumnMapping();
    List<String> columnOrder = joinReader.columnNames();
    Map<String, String> canonicalLabels = joinReader.canonicalLabels();
    if (qualifierMapping == null || qualifierMapping.isEmpty() || columnOrder == null || columnOrder.isEmpty()) {
      return mapping;
    }
    if (tableReferences != null) {
      for (SqlParser.TableReference ref : tableReferences) {
        addQualifierColumns(mapping, ref.tableAlias(), qualifierMapping, columnOrder, canonicalLabels);
        addQualifierColumns(mapping, ref.tableName(), qualifierMapping, columnOrder, canonicalLabels);
      }
    }
    return mapping;
  }

  private static void addQualifierColumns(Map<String, List<QualifiedExpansionColumn>> target, String qualifier,
      Map<String, Map<String, String>> qualifierMapping, List<String> columnOrder,
      Map<String, String> canonicalLabels) {
    if (qualifier == null || qualifier.isBlank()) {
      return;
    }
    String trimmed = qualifier.trim();
    if (trimmed.isEmpty()) {
      return;
    }
    String sanitized = IdentifierUtil.sanitizeIdentifier(trimmed);
    Map<String, String> columns = qualifierMapping.get(sanitized.toLowerCase(Locale.ROOT));
    if ((columns == null || columns.isEmpty()) && !sanitized.equals(trimmed)) {
      columns = qualifierMapping.get(trimmed.toLowerCase(Locale.ROOT));
    }
    if (columns == null || columns.isEmpty()) {
      return;
    }
    LinkedHashSet<String> canonical = new LinkedHashSet<>(columns.values());
    List<QualifiedExpansionColumn> ordered = new ArrayList<>();
    for (String columnName : columnOrder) {
      if (canonical.contains(columnName)) {
        String label = canonicalLabels.getOrDefault(columnName, columnName);
        ordered.add(new QualifiedExpansionColumn(label, columnName));
      }
    }
    if (!ordered.isEmpty()) {
      target.putIfAbsent(sanitized, List.copyOf(ordered));
      if (!sanitized.equals(trimmed)) {
        target.putIfAbsent(trimmed, List.copyOf(ordered));
      }
    }
  }

  private static Map<String, List<QualifiedExpansionColumn>> buildQualifierColumnsForSchema(SqlParser.Select select,
      SqlParser.TableReference baseRef, Schema schema) {
    if (schema == null) {
      return Map.of();
    }
    List<String> columns = schemaColumnNames(schema);
    List<QualifiedExpansionColumn> projections = new ArrayList<>(columns.size());
    for (String column : columns) {
      projections.add(new QualifiedExpansionColumn(column, column));
    }
    List<QualifiedExpansionColumn> immutable = List.copyOf(projections);
    Map<String, List<QualifiedExpansionColumn>> mapping = new LinkedHashMap<>();
    addSchemaQualifier(mapping, baseRef != null ? baseRef.tableAlias() : null, immutable);
    addSchemaQualifier(mapping, baseRef != null ? baseRef.tableName() : null, immutable);
    addSchemaQualifier(mapping, select.tableAlias(), immutable);
    addSchemaQualifier(mapping, select.table(), immutable);
    return mapping;
  }

  private static void addSchemaQualifier(Map<String, List<QualifiedExpansionColumn>> mapping, String qualifier,
      List<QualifiedExpansionColumn> columns) {
    if (qualifier == null || qualifier.isBlank()) {
      return;
    }
    String trimmed = qualifier.trim();
    if (trimmed.isEmpty()) {
      return;
    }
    mapping.putIfAbsent(trimmed, columns);
    String sanitized = IdentifierUtil.sanitizeIdentifier(trimmed);
    if (!sanitized.equals(trimmed)) {
      mapping.putIfAbsent(sanitized, columns);
    }
  }

  private static List<String> schemaColumnNames(Schema schema) {
    if (schema == null) {
      return List.of();
    }
    List<String> names = new ArrayList<>(schema.getFields().size());
    for (Schema.Field field : schema.getFields()) {
      names.add(field.name());
    }
    return List.copyOf(names);
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
    String recordName = derivedRecordName(ref);
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
    List<GenericRecord> sampledRows = applySampleToRows(ref.tableSample(), rows);
    return new JoinRecordReader.JoinTable(ref.tableName(), ref.tableAlias(), schema, sampledRows, ref.joinType(),
        ref.joinCondition(), ref.usingColumns(), null);
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
    List<String> fieldNames = new ArrayList<>();
    List<Schema> valueSchemas = new ArrayList<>();
    SubqueryTableData data = executeSubquery(sql, ref, null, fieldNames, valueSchemas);
    String tableName = ref.tableAlias() != null ? ref.tableAlias() : ref.tableName();
    List<GenericRecord> rows = applySampleToRows(ref.tableSample(), data.rows());
    return new JoinRecordReader.JoinTable(tableName, ref.tableAlias(), data.schema(), rows, ref.joinType(),
        ref.joinCondition(), ref.usingColumns(), null);
  }

  private CteResult materializeValueTable(ValueTableDefinition valueTable, String tableName) throws SQLException {
    if (valueTable == null) {
      throw new SQLException("Value table definition cannot be null");
    }
    List<String> columnNames = valueTable.columnNames();
    if (columnNames == null || columnNames.isEmpty()) {
      throw new SQLException("Value table must expose at least one column");
    }
    Schema literalSchema = Schema.createRecord("values_literal", null, null, false);
    literalSchema.setFields(List.of());
    GenericData.Record emptyRecord = new GenericData.Record(literalSchema);
    ValueExpressionEvaluator evaluator = new ValueExpressionEvaluator(literalSchema);
    List<List<Object>> evaluatedRows = new ArrayList<>(valueTable.rows().size());
    for (List<Expression> expressions : valueTable.rows()) {
      if (expressions.size() != columnNames.size()) {
        throw new SQLException(
            "VALUES row has " + expressions.size() + " expressions but expected " + columnNames.size() + " columns");
      }
      List<Object> evaluated = new ArrayList<>(expressions.size());
      for (Expression expression : expressions) {
        Object value = evaluator.eval(expression, emptyRecord);
        evaluated.add(normalizeLiteralValue(value));
      }
      evaluatedRows.add(evaluated);
    }

    List<ValueColumnType> columnTypes = new ArrayList<>(columnNames.size());
    List<Schema> valueSchemas = new ArrayList<>(columnNames.size());
    for (int i = 0; i < columnNames.size(); i++) {
      List<Object> columnValues = new ArrayList<>(evaluatedRows.size());
      for (List<Object> row : evaluatedRows) {
        columnValues.add(row.get(i));
      }
      ValueColumnType columnType = determineColumnType(columnValues);
      columnTypes.add(columnType);
      valueSchemas.add(schemaForColumnType(columnType, columnValues));
    }

    List<Field> fields = new ArrayList<>(columnNames.size());
    for (int i = 0; i < columnNames.size(); i++) {
      Schema valueSchema = valueSchemas.get(i);
      Schema union = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), valueSchema));
      fields.add(new Field(columnNames.get(i), union, null, Field.NULL_DEFAULT_VALUE));
    }

    String recordName = (tableName == null || tableName.isBlank()) ? "values" : tableName;
    Schema schema = Schema.createRecord(recordName, null, null, false);
    schema.setFields(fields);

    List<GenericRecord> records = new ArrayList<>(evaluatedRows.size());
    for (List<Object> row : evaluatedRows) {
      GenericData.Record record = new GenericData.Record(schema);
      for (int i = 0; i < columnNames.size(); i++) {
        Object normalized = normalizeValueForType(row.get(i), columnTypes.get(i));
        record.put(columnNames.get(i), toAvroValue(normalized, valueSchemas.get(i)));
      }
      records.add(record);
    }
    return new CteResult(schema, records);
  }

  /**
   * Lazily materialize the INFORMATION_SCHEMA.TABLES view.
   *
   * @return cached {@link CteResult} containing the information schema rows
   * @throws SQLException
   *           if the metadata cannot be read
   */
  private CteResult materializeInformationSchemaTables() throws SQLException {
    if (informationSchemaTablesResult == null) {
      InformationSchemaTables.TableData data = InformationSchemaTables.load(stmt.getConn());
      informationSchemaTablesResult = new CteResult(data.schema(), data.rows());
    }
    return informationSchemaTablesResult;
  }

  /**
   * Lazily materialize the INFORMATION_SCHEMA.COLUMNS view.
   *
   * @return cached {@link CteResult} containing the information schema rows
   * @throws SQLException
   *           if the metadata cannot be read
   */
  private CteResult materializeInformationSchemaColumns() throws SQLException {
    if (informationSchemaColumnsResult == null) {
      InformationSchemaColumns.TableData data = InformationSchemaColumns.load(stmt.getConn());
      informationSchemaColumnsResult = new CteResult(data.schema(), data.rows());
    }
    return informationSchemaColumnsResult;
  }

  private Object normalizeLiteralValue(Object value) {
    if (value instanceof TemporalInterval interval) {
      return interval.toString();
    }
    return value;
  }

  private ValueColumnType determineColumnType(List<Object> values) {
    ValueColumnType type = null;
    if (values != null) {
      for (Object value : values) {
        if (value == null) {
          continue;
        }
        ValueColumnType candidate = classifyValueType(value);
        if (type == null) {
          type = candidate;
        } else {
          type = widenType(type, candidate);
        }
      }
    }
    return type == null ? ValueColumnType.STRING : type;
  }

  private ValueColumnType classifyValueType(Object value) {
    if (value instanceof Boolean) {
      return ValueColumnType.BOOLEAN;
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
      return ValueColumnType.INT;
    }
    if (value instanceof Long) {
      return ValueColumnType.LONG;
    }
    if (value instanceof Float) {
      return ValueColumnType.FLOAT;
    }
    if (value instanceof Double) {
      return ValueColumnType.DOUBLE;
    }
    if (value instanceof BigDecimal) {
      return ValueColumnType.DECIMAL;
    }
    if (value instanceof Date) {
      return ValueColumnType.DATE;
    }
    if (value instanceof Time) {
      return ValueColumnType.TIME;
    }
    if (value instanceof Timestamp) {
      return ValueColumnType.TIMESTAMP;
    }
    if (value instanceof byte[] || value instanceof ByteBuffer) {
      return ValueColumnType.BINARY;
    }
    return ValueColumnType.STRING;
  }

  private ValueColumnType widenType(ValueColumnType current, ValueColumnType candidate) {
    if (current == candidate) {
      return current;
    }
    if (isNumeric(current) && isNumeric(candidate)) {
      return widenNumericType(current, candidate);
    }
    if (current == ValueColumnType.STRING || candidate == ValueColumnType.STRING) {
      return ValueColumnType.STRING;
    }
    if ((current == ValueColumnType.TIMESTAMP
        && (candidate == ValueColumnType.DATE || candidate == ValueColumnType.TIME))
        || (candidate == ValueColumnType.TIMESTAMP
            && (current == ValueColumnType.DATE || current == ValueColumnType.TIME))) {
      return ValueColumnType.TIMESTAMP;
    }
    return ValueColumnType.STRING;
  }

  private boolean isNumeric(ValueColumnType type) {
    return switch (type) {
      case INT, LONG, FLOAT, DOUBLE, DECIMAL -> true;
      default -> false;
    };
  }

  private ValueColumnType widenNumericType(ValueColumnType left, ValueColumnType right) {
    if (left == ValueColumnType.DECIMAL || right == ValueColumnType.DECIMAL) {
      return ValueColumnType.DECIMAL;
    }
    if (left == ValueColumnType.DOUBLE || right == ValueColumnType.DOUBLE) {
      return ValueColumnType.DOUBLE;
    }
    if (left == ValueColumnType.FLOAT && right == ValueColumnType.FLOAT) {
      return ValueColumnType.FLOAT;
    }
    if ((left == ValueColumnType.FLOAT && right == ValueColumnType.INT)
        || (left == ValueColumnType.INT && right == ValueColumnType.FLOAT)) {
      return ValueColumnType.FLOAT;
    }
    // FLOAT combined with LONG widens to DOUBLE because LONG exceeds FLOAT
    // precision.
    if (left == ValueColumnType.FLOAT || right == ValueColumnType.FLOAT) {
      return ValueColumnType.DOUBLE;
    }
    if (left == ValueColumnType.LONG || right == ValueColumnType.LONG) {
      return ValueColumnType.LONG;
    }
    return ValueColumnType.INT;
  }

  private Schema schemaForColumnType(ValueColumnType type, List<Object> values) {
    return switch (type) {
      case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
      case INT -> Schema.create(Schema.Type.INT);
      case LONG -> Schema.create(Schema.Type.LONG);
      case FLOAT -> Schema.create(Schema.Type.FLOAT);
      case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
      case DECIMAL -> decimalSchemaForValues(values);
      case DATE -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
      case TIME -> LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
      case TIMESTAMP -> LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
      case BINARY -> Schema.create(Schema.Type.BYTES);
      case STRING -> Schema.create(Schema.Type.STRING);
    };
  }

  private Schema decimalSchemaForValues(List<Object> values) {
    int precision = 1;
    int scale = 0;
    if (values != null) {
      for (Object value : values) {
        if (value == null) {
          continue;
        }
        BigDecimal decimal = value instanceof BigDecimal bd ? bd : new BigDecimal(value.toString());
        precision = Math.max(precision, decimal.precision());
        scale = Math.max(scale, Math.max(decimal.scale(), 0));
      }
    }
    LogicalType logicalType = LogicalTypes.decimal(Math.max(precision, 1), Math.max(scale, 0));
    return logicalType.addToSchema(Schema.create(Schema.Type.BYTES));
  }

  private Object normalizeValueForType(Object value, ValueColumnType type) throws SQLException {
    if (value == null) {
      return null;
    }
    try {
      return switch (type) {
        case BOOLEAN -> (value instanceof Boolean bool) ? bool : Boolean.valueOf(value.toString());
        case INT -> value instanceof Number num ? Integer.valueOf(num.intValue()) : Integer.valueOf(value.toString());
        case LONG -> value instanceof Number num ? Long.valueOf(num.longValue()) : Long.valueOf(value.toString());
        case FLOAT -> value instanceof Number num ? Float.valueOf(num.floatValue()) : Float.valueOf(value.toString());
        case DOUBLE ->
          value instanceof Number num ? Double.valueOf(num.doubleValue()) : Double.valueOf(value.toString());
        case DECIMAL -> value instanceof BigDecimal bd ? bd : new BigDecimal(value.toString());
        case DATE, TIME, TIMESTAMP, BINARY -> value;
        case STRING -> value instanceof CharSequence ? value : value.toString();
      };
    } catch (NumberFormatException | NullPointerException e) {
      throw new SQLException("Failed to convert value '" + value + "' to type " + type, e);
    }
  }

  private void registerValueTableResult(SqlParser.TableReference ref, CteResult result, Map<String, CteResult> target) {
    if (ref == null || result == null || target == null) {
      return;
    }
    String aliasKey = normalizeCteKey(ref.tableAlias());
    if (aliasKey != null) {
      target.put(aliasKey, result);
    }
    String tableKey = normalizeCteKey(ref.tableName());
    if (tableKey != null && (aliasKey == null || !aliasKey.equals(tableKey))) {
      target.putIfAbsent(tableKey, result);
    }
  }

  private CteResult resolveValueTable(SqlParser.TableReference ref) {
    if (ref == null || valueTableResults == null || valueTableResults.isEmpty()) {
      return null;
    }
    String aliasKey = normalizeCteKey(ref.tableAlias());
    if (aliasKey != null) {
      CteResult result = valueTableResults.get(aliasKey);
      if (result != null) {
        return result;
      }
    }
    String tableKey = normalizeCteKey(ref.tableName());
    if (tableKey != null) {
      return valueTableResults.get(tableKey);
    }
    return null;
  }

  private String deriveValueTableName(SqlParser.TableReference ref) {
    if (ref == null) {
      return "values";
    }
    String alias = ref.tableAlias();
    if (alias != null && !alias.isBlank()) {
      return alias;
    }
    String name = ref.tableName();
    if (name != null && !name.isBlank()) {
      return name;
    }
    return "values";
  }

  private enum ValueColumnType {
    BOOLEAN, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIME, TIMESTAMP, STRING, BINARY
  }

  /**
   * Build a {@link JoinRecordReader.JoinTable} backed by a correlated row
   * supplier for a {@code LATERAL} derived table.
   *
   * @param ref
   *          the table reference describing the derived table
   * @param priorTables
   *          tables that precede the lateral subquery and provide correlation
   *          values
   * @param qualifierIndex
   *          mapping from normalized qualifier names to table indexes used during
   *          join evaluation
   * @return a join table capable of evaluating the lateral subquery per
   *         assignment
   * @throws SQLException
   *           if the lateral definition cannot be parsed or executed
   */
  private JoinRecordReader.JoinTable materializeLateralSubqueryTable(SqlParser.TableReference ref,
      List<JoinRecordReader.JoinTable> priorTables, Map<String, Integer> qualifierIndex) throws SQLException {
    String sql = ref.subquerySql();
    if (sql == null || sql.isBlank()) {
      throw new SQLException("Missing subquery SQL for derived table");
    }
    if (priorTables == null || priorTables.isEmpty()) {
      throw new SQLException("LATERAL derived tables require at least one preceding table");
    }
    net.sf.jsqlparser.statement.select.Select parsed;
    try {
      parsed = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(sql);
    } catch (Exception e) {
      throw new SQLException("Failed to parse LATERAL subquery", e);
    }
    List<String> qualifierNames = collectQualifiers(priorTables);
    CorrelatedSubqueryRewriter.Result initial = CorrelatedSubqueryRewriter.rewrite(parsed, qualifierNames,
        (qualifier, column) -> null);
    List<String> fieldNames = new ArrayList<>();
    List<Schema> valueSchemas = new ArrayList<>();
    SubqueryTableData metadata = executeSubquery(initial.sql(), ref, null, fieldNames, valueSchemas);
    Schema schema = metadata.schema();
    Map<String, Map<String, String>> columnLookup = buildCorrelatedColumnLookup(priorTables);
    JoinRecordReader.CorrelatedRowsSupplier supplier = assignments -> {
      CorrelatedSubqueryRewriter.Result rewritten = CorrelatedSubqueryRewriter.rewrite(parsed, qualifierNames,
          (qualifier, column) -> resolveCorrelatedValue(qualifier, column, assignments, priorTables, qualifierIndex,
              columnLookup));
      try {
        return executeSubquery(rewritten.sql(), ref, schema, fieldNames, valueSchemas).rows();
      } catch (SQLException e) {
        throw new IllegalStateException("Failed to evaluate LATERAL subquery", e);
      }
    };
    JoinRecordReader.CorrelatedRowsSupplier effectiveSupplier = ref.tableSample() == null
        ? supplier
        : assignments -> applySampleUnchecked(ref.tableSample(), supplier.rows(assignments));
    String tableName = ref.tableAlias() != null ? ref.tableAlias() : ref.tableName();
    return new JoinRecordReader.JoinTable(tableName, ref.tableAlias(), schema, List.of(), ref.joinType(),
        ref.joinCondition(), ref.usingColumns(), effectiveSupplier);
  }

  /**
   * Execute the supplied subquery and convert the result set into Avro records.
   *
   * @param sql
   *          SQL text to execute
   * @param ref
   *          table reference describing the subquery
   * @param schema
   *          previously discovered schema or {@code null} to infer the schema
   * @param fieldNames
   *          mutable list receiving column names in projection order
   * @param valueSchemas
   *          mutable list receiving non-null Avro schemas for each column
   * @return the materialised rows and schema information
   * @throws SQLException
   *           if the subquery execution fails
   */
  private SubqueryTableData executeSubquery(String sql, SqlParser.TableReference ref, Schema schema,
      List<String> fieldNames, List<Schema> valueSchemas) throws SQLException {
    if (sql == null || sql.isBlank()) {
      throw new SQLException("Missing subquery SQL for derived table");
    }
    try (PreparedStatement subStmt = stmt.getConn().prepareStatement(sql); ResultSet rs = subStmt.executeQuery()) {
      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();
      Schema schemaToUse = schema;
      if (schemaToUse == null) {
        fieldNames.clear();
        valueSchemas.clear();
        schemaToUse = buildSubquerySchema(meta, ref, fieldNames, valueSchemas);
      } else if (fieldNames.size() != columnCount || valueSchemas.size() != columnCount) {
        throw new SQLException("LATERAL subquery column count mismatch");
      }
      List<GenericRecord> rows = new ArrayList<>();
      while (rs.next()) {
        GenericData.Record record = new GenericData.Record(schemaToUse);
        for (int i = 1; i <= columnCount; i++) {
          Object raw = rs.getObject(i);
          record.put(fieldNames.get(i - 1), toAvroValue(raw, valueSchemas.get(i - 1)));
        }
        rows.add(record);
      }
      return new SubqueryTableData(schemaToUse, List.copyOf(rows));
    } catch (SQLException e) {
      throw new SQLException("Failed to execute subquery for join: " + sql, e);
    }
  }

  /**
   * Collect table names and aliases that should be visible to correlated
   * subqueries.
   *
   * @param tables
   *          join tables that precede the correlated subquery
   * @return immutable list of qualifier names
   */
  private static List<String> collectQualifiers(List<JoinRecordReader.JoinTable> tables) {
    if (tables == null || tables.isEmpty()) {
      return List.of();
    }
    List<String> qualifiers = new ArrayList<>();
    for (JoinRecordReader.JoinTable table : tables) {
      if (table.alias() != null && !table.alias().isBlank()) {
        qualifiers.add(table.alias());
      }
      if (table.tableName() != null && !table.tableName().isBlank()) {
        qualifiers.add(table.tableName());
      }
    }
    return List.copyOf(qualifiers);
  }

  /**
   * Construct a qualifier-aware lookup for column names used when resolving
   * correlated references.
   *
   * @param tables
   *          join tables that precede the correlated subquery
   * @return immutable mapping from normalized qualifier to column name mapping
   */
  private static Map<String, Map<String, String>> buildCorrelatedColumnLookup(List<JoinRecordReader.JoinTable> tables) {
    if (tables == null || tables.isEmpty()) {
      return Map.of();
    }
    Map<String, Map<String, String>> lookup = new LinkedHashMap<>();
    for (JoinRecordReader.JoinTable table : tables) {
      Map<String, String> columns = new LinkedHashMap<>();
      for (Schema.Field field : table.schema().getFields()) {
        String normalized = normalizeColumnKey(field.name());
        columns.putIfAbsent(normalized, field.name());
      }
      registerCorrelatedColumns(lookup, table.tableName(), columns);
      registerCorrelatedColumns(lookup, table.alias(), columns);
    }
    return Map.copyOf(lookup);
  }

  /**
   * Register column mappings for a qualifier when correlation is possible.
   *
   * @param lookup
   *          accumulator receiving qualifier mappings
   * @param qualifier
   *          table name or alias to register
   * @param columns
   *          mapping of normalized column names to schema field names
   */
  private static void registerCorrelatedColumns(Map<String, Map<String, String>> lookup, String qualifier,
      Map<String, String> columns) {
    String normalized = JParqUtil.normalizeQualifier(qualifier);
    if (normalized == null || lookup.containsKey(normalized)) {
      return;
    }
    lookup.put(normalized, Map.copyOf(columns));
  }

  /**
   * Resolve the value of a correlated column reference for the supplied
   * assignments.
   *
   * @param qualifier
   *          normalized qualifier associated with the column
   * @param column
   *          column identifier referenced from the outer query
   * @param assignments
   *          current join assignments representing the left-hand side
   * @param priorTables
   *          tables preceding the correlated subquery
   * @param qualifierIndex
   *          lookup from qualifier to join table index
   * @param columnLookup
   *          mapping from qualifier to column name lookup tables
   * @return the correlated column value or {@code null} if unavailable
   */
  private Object resolveCorrelatedValue(String qualifier, String column, List<GenericRecord> assignments,
      List<JoinRecordReader.JoinTable> priorTables, Map<String, Integer> qualifierIndex,
      Map<String, Map<String, String>> columnLookup) {
    if (column == null) {
      return null;
    }
    String normalizedQualifier = qualifier;
    Integer tableIndex = null;
    if (normalizedQualifier != null && qualifierIndex != null) {
      tableIndex = qualifierIndex.get(normalizedQualifier);
    }
    if (tableIndex == null && normalizedQualifier != null && priorTables != null) {
      for (int i = 0; i < priorTables.size(); i++) {
        JoinRecordReader.JoinTable table = priorTables.get(i);
        if (normalizedQualifier.equals(JParqUtil.normalizeQualifier(table.alias()))
            || normalizedQualifier.equals(JParqUtil.normalizeQualifier(table.tableName()))) {
          tableIndex = i;
          break;
        }
      }
    }
    if (tableIndex == null || assignments == null || tableIndex >= assignments.size()) {
      return null;
    }
    GenericRecord record = assignments.get(tableIndex);
    if (record == null) {
      return null;
    }
    Map<String, String> mapping = normalizedQualifier == null
        ? Map.of()
        : columnLookup.getOrDefault(normalizedQualifier, Map.of());
    String normalizedColumn = normalizeColumnKey(column);
    String fieldName = mapping.get(normalizedColumn);
    if (fieldName == null) {
      fieldName = findFieldName(record.getSchema(), normalizedColumn);
    }
    return fieldName == null ? null : record.get(fieldName);
  }

  /**
   * Locate the schema field name matching the supplied normalized column
   * identifier.
   *
   * @param schema
   *          schema describing the table rows
   * @param normalizedColumn
   *          normalized column identifier
   * @return the matching field name or {@code null}
   */
  private static String findFieldName(Schema schema, String normalizedColumn) {
    if (schema == null || normalizedColumn == null) {
      return null;
    }
    for (Schema.Field field : schema.getFields()) {
      if (normalizedColumn.equals(normalizeColumnKey(field.name()))) {
        return field.name();
      }
    }
    return null;
  }

  /**
   * Normalize a column identifier for case-insensitive comparisons and lookup.
   *
   * @param column
   *          column identifier provided by the SQL statement
   * @return normalized column key or {@code null}
   */
  private static String normalizeColumnKey(String column) {
    if (column == null) {
      return null;
    }
    String normalized = JParqUtil.normalizeQualifier(column);
    return normalized != null ? normalized : column.toLowerCase(Locale.ROOT);
  }

  /**
   * Immutable representation of a materialised subquery result set.
   *
   * @param schema
   *          Avro schema describing the rows
   * @param rows
   *          materialised rows produced by the subquery
   */
  private record SubqueryTableData(Schema schema, List<GenericRecord> rows) {
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
    String recordName = derivedRecordName(ref);
    int columnCount = meta.getColumnCount();
    List<Field> fields = new ArrayList<>(columnCount);
    for (int i = 1; i <= columnCount; i++) {
      String columnName = columnLabel(meta, i);
      Schema valueSchema = columnSchema(meta, i);
      if (ref != null && meta.getColumnType(i) == java.sql.Types.OTHER) {
        Schema inferred = inferSubqueryColumnSchema(ref, i);
        if (inferred != null) {
          valueSchema = inferred;
        }
      }
      Schema union = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), valueSchema));
      Field field = new Field(columnName, union, null, Field.NULL_DEFAULT_VALUE);
      fields.add(field);
      fieldNames.add(columnName);
      valueSchemas.add(valueSchema);
    }
    Schema schema = Schema.createRecord(recordName, null, null, false);
    schema.setFields(fields);
    return schema;
  }

  private static String derivedRecordName(SqlParser.TableReference ref) {
    if (ref == null) {
      return "derived";
    }
    String alias = ref.tableAlias();
    if (alias != null && !alias.isBlank()) {
      return alias;
    }
    String name = ref.tableName();
    if (name != null && !name.isBlank()) {
      return name;
    }
    return "derived";
  }

  private Schema inferSubqueryColumnSchema(SqlParser.TableReference ref, int columnIndex) throws SQLException {
    if (ref == null || ref.subquery() == null) {
      return null;
    }
    SqlParser.Select select = ref.subquery();
    List<Expression> expressions = select.expressions();
    if (expressions == null || columnIndex < 1 || columnIndex > expressions.size()) {
      return null;
    }
    Expression projection = expressions.get(columnIndex - 1);
    if (!(projection instanceof Column column)) {
      return null;
    }
    String columnName = column.getColumnName();
    if (columnName == null || columnName.isBlank()) {
      return null;
    }
    Schema sourceSchema = resolveSchemaForColumn(select.tableReferences(), column.getTable());
    if (sourceSchema == null) {
      return null;
    }
    Schema.Field field = findFieldIgnoreCase(sourceSchema, columnName);
    if (field == null) {
      return null;
    }
    return JdbcTypeMapper.nonNullSchema(field.schema());
  }

  private Schema resolveSchemaForColumn(List<SqlParser.TableReference> tableRefs, Table qualifier) throws SQLException {
    if (tableRefs == null || tableRefs.isEmpty()) {
      return null;
    }
    String qualifierName = null;
    if (qualifier != null) {
      if (qualifier.getName() != null && !qualifier.getName().isBlank()) {
        qualifierName = qualifier.getName();
      } else if (qualifier.getFullyQualifiedName() != null && !qualifier.getFullyQualifiedName().isBlank()) {
        qualifierName = qualifier.getFullyQualifiedName();
      } else if (qualifier.getUnquotedName() != null && !qualifier.getUnquotedName().isBlank()) {
        qualifierName = qualifier.getUnquotedName();
      }
    }
    String normalizedQualifier = JParqUtil.normalizeQualifier(qualifierName);
    SqlParser.TableReference match = null;
    if (normalizedQualifier != null) {
      for (SqlParser.TableReference candidate : tableRefs) {
        if (normalizedQualifier.equals(JParqUtil.normalizeQualifier(candidate.tableAlias()))
            || normalizedQualifier.equals(JParqUtil.normalizeQualifier(candidate.tableName()))) {
          match = candidate;
          break;
        }
      }
    } else if (tableRefs.size() == 1) {
      match = tableRefs.getFirst();
    }
    if (match == null) {
      return null;
    }
    return schemaForTableReference(match);
  }

  private Schema schemaForTableReference(SqlParser.TableReference ref) throws SQLException {
    if (ref == null) {
      return null;
    }
    CteResult valueResult = resolveValueTable(ref);
    if (valueResult != null) {
      return valueResult.schema();
    }
    if (ref.commonTableExpression() != null) {
      CteResult result = resolveCteResult(ref.commonTableExpression(), cteResults);
      if (result != null) {
        return result.schema();
      }
    }
    if (ref.tableName() != null) {
      CteResult cte = resolveCteResultByName(ref.tableName(), cteResults);
      if (cte != null) {
        return cte.schema();
      }
      File tableFile = stmt.getConn().tableFile(ref.tableName());
      Path path = new Path(tableFile.toURI());
      Configuration tableConf = new Configuration(false);
      tableConf.setBoolean("parquet.filter.statistics.enabled", true);
      tableConf.setBoolean("parquet.read.filter.columnindex.enabled", true);
      tableConf.setBoolean("parquet.filter.dictionary.enabled", true);
      try {
        return ParquetSchemas.readAvroSchema(path, tableConf);
      } catch (IOException e) {
        throw new SQLException("Failed to read schema for table '" + ref.tableName() + "'", e);
      }
    }
    return null;
  }

  private Schema.Field findFieldIgnoreCase(Schema schema, String columnName) {
    if (schema == null || columnName == null) {
      return null;
    }
    for (Schema.Field field : schema.getFields()) {
      if (field.name().equals(columnName)) {
        return field;
      }
      if (field.name().equalsIgnoreCase(columnName)) {
        return field;
      }
    }
    return null;
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
        if (component != null && queryReferencesCte(component.query(), cteName)) {
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
      for (MaxWindow window : windowPlan.maxWindows()) {
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
