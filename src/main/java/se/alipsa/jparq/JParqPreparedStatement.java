package se.alipsa.jparq;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.BatchUpdateException;
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
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
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
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.AvroProjections;
import se.alipsa.jparq.engine.ColumnMappingUtil;
import se.alipsa.jparq.engine.ColumnsUsed;
import se.alipsa.jparq.engine.CorrelatedBindings;
import se.alipsa.jparq.engine.CorrelationMappings;
import se.alipsa.jparq.engine.DecimalCasts;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.HadoopConfigurations;
import se.alipsa.jparq.engine.Identifier;
import se.alipsa.jparq.engine.InMemoryRecordReader;
import se.alipsa.jparq.engine.JoinPredicatePlanner;
import se.alipsa.jparq.engine.JoinRecordReader;
import se.alipsa.jparq.engine.MaterializedNames;
import se.alipsa.jparq.engine.NumericExpressionTypes;
import se.alipsa.jparq.engine.OrderingUtil;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.ParquetTable;
import se.alipsa.jparq.engine.ProjectionFields;
import se.alipsa.jparq.engine.QueryCancellation;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.RelationalKeys;
import se.alipsa.jparq.engine.SamplingRecordReader;
import se.alipsa.jparq.engine.SqlComparisons;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SqlParser.QualifiedExpansionColumn;
import se.alipsa.jparq.engine.SqlParser.ValueTableDefinition;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.UnnestTableBuilder;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.engine.function.AggregateFunctions;
import se.alipsa.jparq.engine.function.SystemFunctions;
import se.alipsa.jparq.engine.window.WindowFunctions;
import se.alipsa.jparq.engine.window.WindowPlan;
import se.alipsa.jparq.engine.window.WindowState;
import se.alipsa.jparq.helper.JParqUtil;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.LiteralConverter;
import se.alipsa.jparq.helper.TemporalInterval;
import se.alipsa.jparq.meta.InformationSchemaColumns;
import se.alipsa.jparq.meta.InformationSchemaTables;

/**
 * An implementation of the java.sql.PreparedStatement interface.
 *
 * <h2>Prepared Statement Behavior</h2>
 *
 * <p>
 * This implementation uses a two-phase approach for parameterized queries:
 * </p>
 * <ul>
 * <li><b>Preparation phase</b>: Validates SQL syntax by parsing the query with
 * placeholders. This catches syntax errors and invalid table references early,
 * providing fail-fast behavior.</li>
 * <li><b>Execution phase</b>: Re-plans the query with bound parameter values to
 * enable optimizations like predicate pushdown to Parquet filters. This ensures
 * that WHERE clause conditions with parameters can be pushed down to the
 * storage layer for efficient filtering.</li>
 * </ul>
 *
 * <p>
 * This design trades some preparation-time overhead for correctness and
 * performance:
 * </p>
 * <ul>
 * <li><b>Early error detection</b>: Syntax and schema errors are caught at
 * {@code prepareStatement()} time rather than deferred to
 * {@code executeQuery()}.</li>
 * <li><b>Execution-time planning</b>: Query optimization with actual parameter
 * values enables Parquet filter pushdown, which can dramatically reduce I/O for
 * large datasets.</li>
 * <li><b>Parameter safety</b>: Parameters are bound through safe literal
 * rendering (escaping quotes, hex-encoding binary data) to prevent SQL
 * injection.</li>
 * </ul>
 *
 * <p>
 * Named parameters using the {@code :name} syntax are rewritten to JDBC
 * positional markers and can be bound via the dedicated setter overloads (for
 * example {@link #setString(String, String)}).
 * </p>
 *
 * @see #executeQuery()
 * @see #bindParameters()
 */
@SuppressWarnings({
    "checkstyle:AbbreviationAsWordInName", "checkstyle:OverloadMethodsDeclarationOrder",
    "PMD.AvoidCatchingGenericException"
})
public class JParqPreparedStatement implements PreparedStatement {
  private static final Logger LOG = LoggerFactory.getLogger(JParqPreparedStatement.class);
  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ISO_LOCAL_TIME;
  private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
  private final JParqStatement stmt;
  private final String originalSql;

  // --- Query Plan Fields (calculated in constructor) ---
  private SqlParser.Select parsedSelect;
  private final SqlParser.SetQuery parsedSetQuery;
  private final Configuration conf;
  private final Optional<FilterPredicate> parquetPredicate;
  private final Expression residualExpression;
  private final Schema fileAvroSchema;
  private final PushdownInfo pushdownInfo;
  private final Map<Integer, FilterPredicate> joinPredicates;
  private final String tableSchema;
  private final String tableCatalog;
  private final JParqConnection.TableLocation baseTableLocation;
  // Validation belongs to this prepared plan, never to a connection-wide cache.
  private final Map<JParqConnection.TableLocation, ParquetTable> validatedTables = new LinkedHashMap<>();
  private final boolean joinQuery;
  private final List<SqlParser.TableReference> tableReferences;
  private final boolean setOperationQuery;
  private final Map<Identifier, CteResult> cteResults;
  private final Map<Identifier, CteResult> valueTableResults;
  private final CteResult baseCteResult;
  private final Map<Identifier, CteResult> inheritedCteContext;
  private final int parameterCount;
  private final Map<String, List<Integer>> namedParameterIndexes;
  private final boolean hasParameters;
  private CteResult informationSchemaTablesResult;
  private CteResult informationSchemaColumnsResult;
  private final Map<Integer, Object> parameterValues = new LinkedHashMap<>();
  private final List<Map<Integer, Object>> batchedParameters = new ArrayList<>();
  private final boolean ownsStatement;
  private JParqPreparedStatement boundStatement;
  private boolean boundStatementStale;
  private boolean closed = false;
  private boolean nested;
  private QueryCancellation inheritedCancellation;
  private final SubqueryExecutor.ExecutionPlans reusablePlan;
  private SubqueryExecutor.ExecutionPlans executionPlans;

  JParqPreparedStatement(JParqStatement stmt, String sql) throws SQLException {
    this(stmt, sql, Map.of(), false);
  }

  JParqPreparedStatement(JParqStatement stmt, String sql, Map<Identifier, CteResult> inheritedCtes)
      throws SQLException {
    this(stmt, sql, inheritedCtes, false);
  }

  JParqPreparedStatement(JParqStatement stmt, String sql, boolean ownsStatement) throws SQLException {
    this(stmt, sql, Map.of(), ownsStatement);
  }

  JParqPreparedStatement(JParqStatement stmt, String sql, Map<Identifier, CteResult> inheritedCtes,
      boolean ownsStatement) throws SQLException {
    this(stmt, sql, inheritedCtes, ownsStatement, null);
  }

  /**
   * Instantiates a parsed nested shape with fresh row-dependent materializations.
   *
   * @param stmt
   *          owning statement
   * @param sql
   *          stable SQL for this shape
   * @param inheritedCtes
   *          current inherited CTE values
   * @param ownsStatement
   *          whether closing this wrapper closes the statement
   * @param reusablePlan
   *          execution-local parsed and validated shape, or null
   * @throws SQLException
   *           if planning or materialization fails
   */
  private JParqPreparedStatement(JParqStatement stmt, String sql, Map<Identifier, CteResult> inheritedCtes,
      boolean ownsStatement, SubqueryExecutor.ExecutionPlans reusablePlan) throws SQLException {
    this.stmt = stmt;
    this.reusablePlan = reusablePlan;
    this.executionPlans = reusablePlan;
    this.ownsStatement = ownsStatement;
    ParameterParseResult parseResult = parseParameters(sql);
    this.originalSql = parseResult.sql();
    this.parameterCount = parseResult.parameterCount();
    this.namedParameterIndexes = parseResult.namedParameterIndexes();
    this.hasParameters = parameterCount > 0;
    this.inheritedCteContext = inheritedCtes == null ? Map.of() : Map.copyOf(inheritedCtes);

    SqlParser.Select tmpSelect = null;
    SqlParser.SetQuery tmpSetQuery = null;
    boolean tmpSetOperation;
    List<SqlParser.TableReference> tmpTableRefs;
    boolean tmpJoinQuery;
    Configuration tmpConf = null;
    Schema tmpFileAvro = null;
    Optional<FilterPredicate> tmpPredicate = Optional.empty();
    Expression tmpResidual = null;
    JParqConnection.TableLocation tmpBaseLocation = null;
    String tmpSchemaName = null;
    CteResult tmpBaseCteResult = null;
    Map<Identifier, CteResult> tmpCteResults;
    Map<Identifier, CteResult> tmpValueTables = new LinkedHashMap<>();

    // --- QUERY PLANNING PHASE (Expensive CPU Work) ---
    try {
      if (hasParameters) {
        // For parameterized queries, perform lightweight syntax validation at
        // preparation time
        // to catch errors early (fail-fast), but defer full query planning (including
        // schema
        // resolution and optimization) to execution time when actual parameter values
        // are known.
        // This enables predicate pushdown with bound values while still providing early
        // error detection.
        SqlParser.parseQuery(originalSql); // Validates syntax and basic structure
        tmpSetOperation = false;
        tmpTableRefs = List.of();
        tmpJoinQuery = false;
        tmpCteResults = this.inheritedCteContext;
      } else {
        SqlParser.Query query = reusablePlan == null
            ? SqlParser.parseQuery(originalSql)
            : reusablePlan.query(originalSql);

        Map<Identifier, CteResult> mutableCtes = new LinkedHashMap<>();
        if (inheritedCtes != null && !inheritedCtes.isEmpty()) {
          for (Map.Entry<Identifier, CteResult> entry : inheritedCtes.entrySet()) {
            Identifier key = entry.getKey();
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
          SqlParser.TableReference baseRef = tmpTableRefs.isEmpty() ? null : tmpTableRefs.getFirst();
          boolean baseIsDerivedTable = baseRef != null && baseRef.isDerivedTable();
          tmpJoinQuery = tmpSelect.hasMultipleTables();

          boolean baseIsCte = baseRef != null && !baseIsDerivedTable && (baseRef.commonTableExpression() != null
              || resolveCteResultByName(baseRef.tableName(), tmpCteResults) != null);
          boolean baseIsInformationSchemaTables = baseRef != null
              && InformationSchemaTables.matchesTableReference(baseRef.tableName());
          boolean baseIsInformationSchemaColumns = baseRef != null
              && InformationSchemaColumns.matchesTableReference(baseRef.tableName());

          AggregateFunctions.AggregatePlan aggregatePlan = null;

          if (tmpJoinQuery) {
            tmpConf = HadoopConfigurations.create();
            configureParquetFiltering(tmpConf);
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
            resolved = applyColumnAliases(resolved, baseRef);
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
          } else if (baseIsDerivedTable) {
            List<String> fieldNames = new ArrayList<>();
            List<Schema> valueSchemas = new ArrayList<>();
            SubqueryTableData data = executeSubquery(baseRef.subquerySql(), baseRef, null, fieldNames, valueSchemas,
                tmpCteResults);
            tmpBaseCteResult = new CteResult(data.schema(), data.rows());
            tmpFileAvro = data.schema();
            if (!tmpSelect.qualifiedWildcards().isEmpty()) {
              tmpSelect = SqlParser.expandQualifiedWildcards(tmpSelect,
                  buildQualifierColumnsForSchema(tmpSelect, baseRef, tmpFileAvro));
            }
            aggregatePlan = AggregateFunctions.plan(tmpSelect);
            tmpResidual = tmpSelect.where();
            tmpPredicate = Optional.empty();
          } else if (baseRef != null && baseRef.unnest() != null) {
            CteResult resolved = materializeUnnestTable(baseRef);
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
            rejectTableColumnAliases(baseRef);
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
            tmpSchemaName = formatSchemaForMetadata("information_schema");
            aggregatePlan = AggregateFunctions.plan(tmpSelect);
            tmpResidual = tmpSelect.where();
            tmpPredicate = Optional.empty();
          } else {
            rejectTableColumnAliases(baseRef);
            tmpConf = HadoopConfigurations.create();
            configureParquetFiltering(tmpConf);

            String targetTable = baseRef != null ? baseRef.tableName() : tmpSelect.table();
            String targetSchema = baseRef != null ? baseRef.schemaName() : null;
            tmpBaseLocation = stmt.getConn().resolveTable(targetSchema, targetTable);
            tmpSchemaName = formatSchemaForMetadata(tmpBaseLocation.schemaName());
            Schema avro = validatedTable(tmpBaseLocation, tmpConf).schema();

            tmpFileAvro = avro;

            if (!tmpSelect.qualifiedWildcards().isEmpty()) {
              Map<String, List<QualifiedExpansionColumn>> qualifierColumns = buildQualifierColumnsForSchema(tmpSelect,
                  baseRef, tmpFileAvro);
              tmpSelect = SqlParser.expandQualifiedWildcards(tmpSelect, qualifierColumns);
            }

            aggregatePlan = AggregateFunctions.plan(tmpSelect);
            configureProjectionPushdown(tmpSelect, tmpFileAvro, aggregatePlan, tmpConf, tmpCteResults);

            if (stmt.getConn().isPredicatePushdown() && (baseRef == null || baseRef.tableSample() == null)
                && tmpSelect.where() != null) {
              var split = ParquetFilterBuilder.split(tmpFileAvro, tmpSelect.where());
              tmpPredicate = split.predicate();
              tmpResidual = split.residual();
            } else {
              tmpPredicate = Optional.empty();
              tmpResidual = tmpSelect.where();
            }

          }
          if (tmpBaseCteResult != null) {
            validateSelectColumns(tmpSelect, tmpFileAvro, aggregatePlan, tmpCteResults, tmpConf);
          }
        }
      }
    } catch (Exception e) {
      throw SqlExceptions.wrap("Failed to prepare query: " + sql, e);
    }

    this.parsedSelect = tmpSelect;
    this.parsedSetQuery = tmpSetQuery;
    this.setOperationQuery = tmpSetOperation;
    this.tableReferences = tmpTableRefs;
    this.joinQuery = tmpJoinQuery;
    this.conf = tmpConf;
    this.parquetPredicate = tmpPredicate;
    this.residualExpression = tmpResidual;
    this.fileAvroSchema = tmpFileAvro;
    this.cteResults = tmpCteResults;
    try {
      this.joinPredicates = tmpJoinQuery ? planJoinPredicates() : Map.of();
    } catch (RuntimeException exception) {
      throw SqlExceptions.wrap("Failed to prepare query: " + sql, exception);
    }
    PushdownInfo plannedPushdown = determinePushdownInfo(hasParameters, tmpSetOperation, tmpJoinQuery, tmpBaseCteResult,
        tmpConf, tmpSelect, tmpPredicate, tmpResidual);
    this.pushdownInfo = stmt.getConn().isProjectionPushdown()
        ? plannedPushdown
        : new PushdownInfo(plannedPushdown.parquetPredicateAttached(), plannedPushdown.residualFilterPresent(),
            plannedPushdown.statisticsFilteringEnabled(), plannedPushdown.columnIndexFilteringEnabled(),
            plannedPushdown.dictionaryFilteringEnabled(), plannedPushdown.bloomFilteringEnabled(),
            plannedPushdown.message() + " Projection pushdown disabled by connection property.",
            plannedPushdown.parquetPredicateSummary(), plannedPushdown.residualExpressionSummary());
    this.tableSchema = tmpSchemaName;
    this.tableCatalog = stmt.getConn().getCatalog();
    this.baseTableLocation = tmpBaseLocation;
    this.valueTableResults = tmpValueTables;
    this.baseCteResult = tmpBaseCteResult;
    logPushdownInfo(this.pushdownInfo);
  }

  /**
   * Executes the query using the pre-calculated plan. This method only focuses on
   * I/O (building and reading the ParquetReader).
   */
  @SuppressWarnings("PMD.CloseResource")
  @Override
  public JParqResultSet executeQuery() throws SQLException {
    try (JParqStatement.ExecutionScope execution = stmt.beginExecution(inheritedCancellation)) {
      var cancellation = execution.cancellation();
      cancellation.checkCanceled();
      try {
        return executeQuery(cancellation);
      } catch (SQLException | RuntimeException exception) {
        cancellation.checkCanceled();
        if (SqlExceptions.findSqlStateFailure(exception) != null) {
          throw SqlExceptions.wrap(exception.getMessage(), exception);
        }
        throw exception;
      }
    }
  }

  /**
   * Executes under the cancellation state shared by all nested query work.
   *
   * @param cancellation
   *          execution cancellation state
   * @return completed or streaming result
   * @throws SQLException
   *           if execution fails or is canceled
   */
  // The context handle must outlive this call when ownership passes to the
  // result.
  @SuppressWarnings("PMD.UseTryWithResources")
  private JParqResultSet executeQuery(QueryCancellation cancellation) throws SQLException {
    if (hasParameters) {
      return executeWithBoundParameters();
    }
    if (reusablePlan == null) {
      executionPlans = new SubqueryExecutor.ExecutionPlans();
    }
    Connection conn = getConnection();
    JParqDatabaseMetaData metaData = (JParqDatabaseMetaData) conn.getMetaData();
    var context = SystemFunctions.openContext(conn.getCatalog(), metaData.getUserName());
    boolean transferred = false;
    try {
      if (setOperationQuery) {
        JParqResultSet rs = executeSetOperation();
        publishResult(rs, cancellation, context);
        transferred = true;
        return rs;
      }
      RecordReader reader;
      String resultTableName;
      String resultSchema = tableSchema;
      Schema resultMetadata;
      SqlParser.TableReference baseRef = (tableReferences == null || tableReferences.isEmpty())
          ? null
          : tableReferences.getFirst();
      try {
        if (joinQuery) {
          reader = buildJoinReader(cancellation);
          resultTableName = buildJoinTableName();
          resultSchema = null;
        } else if (baseCteResult != null) {
          resultTableName = parsedSelect.tableAlias() != null ? parsedSelect.tableAlias() : parsedSelect.table();
          if ((resultTableName == null || resultTableName.isBlank()) && baseRef != null && baseRef.tableName() != null
              && !baseRef.tableName().isBlank()) {
            resultTableName = baseRef.tableName();
          }
          reader = new InMemoryRecordReader(baseCteResult.rows(), baseCteResult.schema(), resultTableName);
        } else {
          FilterPredicate predicate = parquetPredicate.orElse(null);
          if (reusablePlan != null && stmt.getConn().isPredicatePushdown()
              && (baseRef == null || baseRef.tableSample() == null)) {
            predicate = ParquetFilterBuilder.withCorrelatedBindings(
                () -> ParquetFilterBuilder.split(fileAvroSchema, parsedSelect.where()).predicate().orElse(null));
          }
          reader = validatedTable(baseTableLocation, conf).open(conf, predicate);
          resultTableName = baseTableLocation.tableName();
        }
        resultMetadata = reader instanceof JoinRecordReader joinReader ? joinReader.schema() : fileAvroSchema;
        reader = cancellation.track(reader);
        if (!joinQuery) {
          RecordReader sampled = applyTableSample(baseRef == null ? null : baseRef.tableSample(), reader);
          if (sampled != reader) {
            reader = cancellation.track(sampled);
          }
        }
      } catch (SQLException e) {
        throw e;
      } catch (Exception e) {
        throw new SQLException("Failed to open query data source", e);
      }

      // Derive label/physical lists from the parsed SELECT
      // Convention from SqlParser: labels() empty => SELECT *
      final List<String> labels = parsedSelect.labels().isEmpty() ? null : parsedSelect.labels();
      final List<String> physical = parsedSelect.labels().isEmpty() ? null : parsedSelect.columnNames();
      // Create the result set, passing reader, select plan, residual filter,
      // plus projection labels and physical names (for metadata & value lookups)
      SubqueryExecutor subqueryExecutor = createSubqueryExecutor(cancellation);
      JParqResultSet rs;
      try {
        rs = new JParqResultSet(reader, parsedSelect, resultTableName, resultSchema, tableCatalog, residualExpression,
            labels, physical, subqueryExecutor);
        rs.setMetadataSchema(resultMetadata);
      } catch (SQLException | RuntimeException exception) {
        try {
          subqueryExecutor.close();
        } catch (SQLException closeException) {
          exception.addSuppressed(closeException);
        }
        try {
          reader.close();
        } catch (IOException closeException) {
          exception.addSuppressed(closeException);
        }
        throw exception;
      }

      publishResult(rs, cancellation, context);
      transferred = true;
      return rs;
    } finally {
      if (!transferred) {
        context.close();
      }
    }
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "A PreparedStatement cannot execute a new query string. Use the no-arg executeQuery().");
  }

  /**
   * Attaches execution ownership before exposing a result to the statement.
   *
   * @param result
   *          completed result
   * @param cancellation
   *          execution cancellation state
   * @param context
   *          this execution's function context
   * @throws SQLException
   *           if cancellation raced result construction
   */
  private void publishResult(JParqResultSet result, QueryCancellation cancellation,
      SystemFunctions.ContextScope context) throws SQLException {
    result.setExecutionContext(cancellation, context);
    try {
      cancellation.checkCanceled();
      if (!nested) {
        stmt.setCurrentRs(result);
      }
    } catch (SQLException exception) {
      result.close();
      throw exception;
    }
  }

  /**
   * Identifies nested physical-table plans without binding-dependent input data.
   * Derived inputs, joins, CTEs, and set operations must rebuild their current
   * materializations while reusing their parsed and validated shapes.
   *
   * @return whether this statement can execute again with new outer bindings
   */
  public boolean isReusableSubqueryShape() {
    // joinPredicates captures current bindings; join statements must never be
    // cached.
    // See planJoinPredicates, which runs during construction for each outer row.
    return nested && !hasParameters && !setOperationQuery && !joinQuery && baseCteResult == null && cteResults.isEmpty()
        && inheritedCteContext.isEmpty();
  }

  @Override
  public void close() {
    // Close the bound statement if it exists
    if (boundStatement != null) {
      boundStatement.close();
      boundStatement = null;
    }
    if (ownsStatement) {
      stmt.close();
    }
    executionPlans = null;
    closed = true;
  }

  /**
   * Creates an executor that keeps its plan cache after a temporary statement
   * wrapper is closed by Statement.executeQuery.
   *
   * @param cancellation
   *          owning execution's cancellation state
   * @return executor owned by the result or join materialization
   */
  private SubqueryExecutor createSubqueryExecutor(QueryCancellation cancellation) {
    var plans = executionPlans;
    return new SubqueryExecutor(stmt.getConn(), sql -> prepareSubqueryStatement(sql, cteResults, cancellation, plans),
        this::subqueryTableColumns, plans);
  }

  private JParqPreparedStatement prepareSubqueryStatement(String sql, Map<Identifier, CteResult> cteContext)
      throws SQLException {
    return prepareSubqueryStatement(sql, cteContext, inheritedCancellation);
  }

  /**
   * Prepares nested work without publishing its private result on the outer
   * statement.
   *
   * @param sql
   *          nested SQL
   * @param cteContext
   *          inherited CTE values
   * @param cancellation
   *          outer execution state, including during result iteration
   * @return nested prepared statement
   * @throws SQLException
   *           if preparation fails or execution is canceled
   */
  private JParqPreparedStatement prepareSubqueryStatement(String sql, Map<Identifier, CteResult> cteContext,
      QueryCancellation cancellation) throws SQLException {
    return prepareSubqueryStatement(sql, cteContext, cancellation, executionPlans);
  }

  /**
   * Prepares nested work using an explicitly retained execution-local cache.
   *
   * @param sql
   *          nested SQL
   * @param cteContext
   *          current inherited CTE values
   * @param cancellation
   *          owning cancellation state
   * @param plans
   *          retained parent shapes, or null during ordinary preparation
   * @return nested statement
   * @throws SQLException
   *           if preparation fails or execution is canceled
   */
  private JParqPreparedStatement prepareSubqueryStatement(String sql, Map<Identifier, CteResult> cteContext,
      QueryCancellation cancellation, SubqueryExecutor.ExecutionPlans plans) throws SQLException {
    Map<Identifier, CteResult> inherited = cteContext == null ? Map.of() : cteContext;
    try (var execution = stmt.beginExecution(cancellation)) {
      execution.cancellation().checkCanceled();
      var shape = plans == null ? null : plans.child(sql);
      var prepared = new JParqPreparedStatement(stmt, sql, inherited, false, shape);
      prepared.nested = true;
      prepared.inheritedCancellation = execution.cancellation();
      return prepared;
    }
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

  private String formatSchemaForMetadata(String schemaName) {
    if (schemaName == null) {
      return null;
    }
    return stmt.getConn().isCaseSensitive() ? schemaName : schemaName.toUpperCase(Locale.ROOT);
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
    List<List<NumericType>> componentTypes = new ArrayList<>(components.size());
    List<String> labels = null;
    List<Integer> sqlTypes = null;
    List<Schema> sourceSchemas = new ArrayList<>();
    int columnCount = -1;
    for (SqlParser.SetComponent component : components) {
      List<List<Object>> componentData = new ArrayList<>();
      List<NumericType> normalizedTypes = new ArrayList<>();
      try (JParqPreparedStatement prepared = prepareSubqueryStatement(component.sql(), cteResults);
          ResultSet rs = prepared.executeQuery()) {
        ResultSetMetaData meta = rs.getMetaData();
        if (labels == null) {
          columnCount = meta.getColumnCount();
          labels = new ArrayList<>(columnCount);
          sqlTypes = new ArrayList<>(columnCount);
          for (int col = 1; col <= columnCount; col++) {
            labels.add(columnLabel(meta, col));
            sqlTypes.add(Types.NULL);
            sourceSchemas.add(null);
          }
        } else if (meta.getColumnCount() != columnCount) {
          throw new SQLException("Set operation components must project the same number of columns");
        }
        while (rs.next()) {
          List<Object> row = new ArrayList<>(columnCount);
          for (int col = 1; col <= columnCount; col++) {
            row.add(rs.getObject(col));
          }
          componentData.add(materializeRow(row));
        }
        for (int col = 1; col <= columnCount; col++) {
          int index = col - 1;
          Supplier<List<Object>> values = new Supplier<>() {
            private List<Object> cached;

            /** {@inheritDoc} */
            @Override
            public List<Object> get() {
              if (cached == null) {
                cached = componentData.stream().map(row -> row.get(index)).toList();
              }
              return cached;
            }
          };
          int type = meta.getColumnType(col);
          Schema schema = sourceColumnSchema(meta, col);
          Expression expression = prepared.parsedSelect != null
              && prepared.parsedSelect.expressions().size() == columnCount
                  ? prepared.parsedSelect.expressions().get(index)
                  : null;
          Expression projection = ExpressionEvaluator.unwrapParenthesis(expression);
          boolean simpleDeclaration = projection instanceof CastExpression || projection instanceof SignedExpression
              || LiteralConverter.isLiteral(projection);
          Schema projected = type == Types.OTHER ? setExpressionSchema(projection, values) : null;
          int projectedType = projected == null ? Types.NULL : JdbcTypeMapper.mapSchemaToJdbcType(projected);
          if (type == Types.OTHER || projection instanceof NullValue) {
            boolean unknown = type == Types.OTHER;
            schema = projected;
            type = projectedType;
            // Preserve the existing contract for unknown nonnumeric projections.
            // Only numeric values need new coercion and metadata here.
            if (unknown && type != Types.NULL && NumericType.of(type, 0, 0) == null) {
              type = Types.OTHER;
              schema = sourceColumnSchema(meta, col);
            }
          }
          boolean trustedBounds = projection instanceof Column
              || (projection instanceof CastExpression cast && DecimalCasts.declaredType(cast) != null);
          NumericType numeric = NumericType.of(type, meta.getPrecision(col), meta.getScale(col));
          if (numeric != null) {
            if ((meta.getPrecision(col) == 0 || simpleDeclaration) && schema != null
                && schema.getLogicalType() instanceof LogicalTypes.Decimal decimal) {
              numeric = NumericType.of(type, decimal.getPrecision(), decimal.getScale());
            }
            if ((type == Types.DECIMAL || type == Types.NUMERIC) && (!trustedBounds || meta.getPrecision(col) == 0)) {
              List<Object> returned = values.get();
              if (returned.stream().anyMatch(Objects::nonNull)) {
                LogicalTypes.Decimal bounds = DecimalTypeInference.infer(returned);
                numeric = numeric.common(NumericType.of(type, bounds.getPrecision(), bounds.getScale()));
              }
            }
            schema = numeric.schema();
          }
          NumericType declared = NumericType.of(meta.getColumnType(col), meta.getPrecision(col), meta.getScale(col));
          // Unknown aggregate values may still disagree with metadata (#289).
          // Only direct columns can bypass value normalization.
          normalizedTypes.add(projection instanceof Column && numeric != null
              && numeric.jdbcType() == meta.getColumnType(col) && numeric.equals(declared) ? numeric : null);
          mergeSetColumn(sqlTypes, sourceSchemas, index, type, schema, numeric);
        }
      }
      componentRows.add(componentData);
      componentTypes.add(normalizedTypes);
    }
    sqlTypes.replaceAll(type -> type == Types.NULL ? Types.OTHER : type);
    coerceSetRows(componentRows, componentTypes, sqlTypes, sourceSchemas);
    List<List<Object>> combined = evaluateSetOperation(components, componentRows, setKeyFunction(sqlTypes));
    List<List<Object>> ordered = applySetOrdering(combined, labels, sqlTypes, parsedSetQuery.orderBy());
    List<List<Object>> sliced = applySetLimitOffset(ordered, parsedSetQuery.limit(), parsedSetQuery.offset());
    return JParqResultSet.materializedResult("set_operation_result", labels, sqlTypes, sliced, sourceSchemas);
  }

  /**
   * Resolves untyped set projections from simple declarations or returned values.
   * Declared numeric expressions use the shared expression type resolver.
   *
   * @param expression
   *          projected expression, when available
   * @param values
   *          lazily supplied returned column values
   * @return inferred schema, or null for an untyped NULL
   */
  private Schema setExpressionSchema(Expression expression, Supplier<List<Object>> values) {
    Expression projection = ExpressionEvaluator.unwrapParenthesis(expression);
    if (projection instanceof NullValue) {
      return null;
    }
    NumericType resolved = NumericExpressionTypes.resolve(projection, column -> null);
    if (resolved != null) {
      return resolved.schema();
    }
    if (LiteralConverter.isLiteral(projection)) {
      Object literal = LiteralConverter.toLiteral(projection);
      return schemaForColumnType(classifyValueType(literal), List.of(literal));
    }
    List<Object> returned = values.get();
    // Keep nonnumeric CASTs distinct from untyped NULL; their declared metadata
    // remains outside numeric promotion and is tracked in #288.
    if (!(projection instanceof CastExpression) && returned.stream().allMatch(Objects::isNull)) {
      return null;
    }
    return schemaForColumnType(determineColumnType(returned), returned);
  }

  /**
   * Merges a branch column into the common set-operation declaration.
   *
   * @param types
   *          common JDBC types
   * @param schemas
   *          common source schemas
   * @param index
   *          output position
   * @param type
   *          branch JDBC type
   * @param schema
   *          branch schema, or null
   * @param numeric
   *          branch numeric descriptor, or null
   * @throws SQLException
   *           if branch types are incompatible
   */
  private void mergeSetColumn(List<Integer> types, List<Schema> schemas, int index, int type, Schema schema,
      NumericType numeric) throws SQLException {
    int previous = types.get(index);
    if (type == Types.NULL) {
      return;
    }
    if (previous == Types.NULL) {
      types.set(index, numeric == null ? type : numeric.jdbcType());
      schemas.set(index, schema);
      return;
    }
    Schema prior = schemas.get(index);
    NumericType previousNumeric = NumericType.of(previous, prior);
    if (previousNumeric != null && numeric != null) {
      NumericType common = previousNumeric.common(numeric);
      types.set(index, common.jdbcType());
      schemas.set(index, common.schema());
    } else if (previous != type) {
      throw new SQLException("Set operation components must use compatible column types", "42000");
    } else if (!Objects.equals(prior, schema)) {
      schemas.set(index, null);
    }
  }

  /**
   * Normalizes changed numeric branches before duplicate counting or ordering.
   * Already normalized branches and unchanged rows retain their snapshots.
   *
   * @param components
   *          rows of each branch
   * @param componentTypes
   *          normalized branch types, with null for unknown representations
   * @param types
   *          resolved output types
   * @param schemas
   *          resolved source schemas
   * @throws SQLException
   *           if a numeric conversion loses data
   */
  private void coerceSetRows(List<List<List<Object>>> components, List<List<NumericType>> componentTypes,
      List<Integer> types, List<Schema> schemas) throws SQLException {
    List<NumericType> numericTypes = new ArrayList<>(types.size());
    for (int col = 0; col < types.size(); col++) {
      numericTypes.add(NumericType.of(types.get(col), schemas.get(col)));
    }
    for (int component = 0; component < components.size(); component++) {
      List<Integer> conversions = new ArrayList<>();
      for (int col = 0; col < types.size(); col++) {
        if (numericTypes.get(col) != null && !numericTypes.get(col).equals(componentTypes.get(component).get(col))) {
          conversions.add(col);
        }
      }
      if (conversions.isEmpty()) {
        continue;
      }
      List<List<Object>> rows = components.get(component);
      for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
        List<Object> original = rows.get(rowIndex);
        List<Object> converted = null;
        for (int col : conversions) {
          Object value = numericTypes.get(col).convert(original.get(col));
          if (value != original.get(col)) {
            if (converted == null) {
              converted = new ArrayList<>(original);
            }
            converted.set(col, value);
          }
        }
        if (converted != null) {
          rows.set(rowIndex, materializeRow(converted));
        }
      }
    }
  }

  /**
   * Resolves key normalization once for the common set-operation output types.
   * Known numeric columns have already been coerced; OTHER columns may still
   * contain mixed Integer and Long values that must compare as one number.
   *
   * @param sqlTypes
   *          final output JDBC types
   * @return a key function retaining compatible scalar rows without copying
   */
  private UnaryOperator<List<Object>> setKeyFunction(List<Integer> sqlTypes) {
    if (!sqlTypes.contains(Types.OTHER)) {
      return row -> RelationalKeys.row(row, false);
    }
    boolean[] normalizeColumns = new boolean[sqlTypes.size()];
    for (int column = 0; column < sqlTypes.size(); column++) {
      normalizeColumns[column] = sqlTypes.get(column) == Types.OTHER;
    }
    return row -> RelationalKeys.row(row, normalizeColumns);
  }

  /**
   * Evaluate the list of set operation components honoring SQL operator
   * precedence (INTERSECT before UNION/UNION ALL/EXCEPT/EXCEPT ALL).
   *
   * @param components
   *          parsed set operation components in encounter order
   * @param rowsPerComponent
   *          materialized rows corresponding to each component
   * @param keyFunction
   *          key normalization policy for the final output column types
   * @return combined rows representing the full set operation result
   * @throws SQLException
   *           if an unsupported operator is encountered or if evaluation fails
   */
  private List<List<Object>> evaluateSetOperation(List<SqlParser.SetComponent> components,
      List<List<List<Object>>> rowsPerComponent, UnaryOperator<List<Object>> keyFunction) throws SQLException {
    if (components.isEmpty()) {
      return List.of();
    }
    List<List<List<Object>>> valueStack = new ArrayList<>();
    List<SqlParser.SetOperator> operatorStack = new ArrayList<>();
    valueStack.add(new ArrayList<>(rowsPerComponent.getFirst()));
    for (int i = 1; i < components.size(); i++) {
      SqlParser.SetOperator operator = components.get(i).operator();
      if (operator == SqlParser.SetOperator.FIRST) {
        throw new SQLException("Unexpected FIRST operator in set operation evaluation");
      }
      while (!operatorStack.isEmpty() && precedence(operatorStack.getLast()) >= precedence(operator)) {
        SqlParser.SetOperator top = operatorStack.removeLast();
        List<List<Object>> right = valueStack.removeLast();
        List<List<Object>> left = valueStack.removeLast();
        valueStack.add(applySetOperator(left, right, top, keyFunction));
      }
      operatorStack.add(operator);
      valueStack.add(new ArrayList<>(rowsPerComponent.get(i)));
    }
    while (!operatorStack.isEmpty()) {
      SqlParser.SetOperator top = operatorStack.removeLast();
      List<List<Object>> right = valueStack.removeLast();
      List<List<Object>> left = valueStack.removeLast();
      valueStack.add(applySetOperator(left, right, top, keyFunction));
    }
    return valueStack.isEmpty() ? List.<List<Object>>of() : valueStack.getFirst();
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
   * @param keyFunction
   *          key normalization policy for the final output column types
   * @return combined rows representing the operator result
   * @throws SQLException
   *           if the operator is not supported
   */
  private List<List<Object>> applySetOperator(List<List<Object>> left, List<List<Object>> right,
      SqlParser.SetOperator operator, UnaryOperator<List<Object>> keyFunction) throws SQLException {
    return switch (operator) {
      case UNION -> mergeDistinct(left, right, keyFunction);
      case UNION_ALL -> {
        List<List<Object>> combined = new ArrayList<>(left.size() + right.size());
        combined.addAll(left);
        combined.addAll(right);
        yield combined;
      }
      case INTERSECT -> intersectDistinct(left, right, keyFunction);
      case INTERSECT_ALL -> intersectAll(left, right, keyFunction);
      case EXCEPT -> exceptDistinct(left, right, keyFunction);
      case EXCEPT_ALL -> exceptAll(left, right, keyFunction);
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
   * @param sqlTypes
   *          common output JDBC types
   * @param orderBy
   *          order specification parsed from the set operation query
   * @return ordered rows (or the original rows when no ordering is requested)
   * @throws SQLException
   *           if an ORDER BY column cannot be resolved
   */
  private List<List<Object>> applySetOrdering(List<List<Object>> rows, List<String> labels, List<Integer> sqlTypes,
      List<SqlParser.SetOrder> orderBy) throws SQLException {
    if (orderBy == null || orderBy.isEmpty() || rows.isEmpty()) {
      return rows;
    }
    Map<String, Integer> labelIndexes = new HashMap<>();
    for (int i = 0; i < labels.size(); i++) {
      String key = Identifier.lookupKey(labels.get(i));
      if (key != null) {
        labelIndexes.put(key, i);
      }
    }
    List<ResolvedSetOrder> resolved = new ArrayList<>(orderBy.size());
    boolean hasExpressions = false;
    for (SqlParser.SetOrder order : orderBy) {
      int index;
      if (order.columnIndex() != null) {
        index = order.columnIndex() - 1;
      } else if (order.expression() == null) {
        if (order.columnLabel() == null) {
          throw new SQLException("Set operation ORDER BY requires column index, label, or expression");
        }
        String lookupKey = Identifier.lookupKey(order.columnLabel());
        Integer mapped = lookupKey == null ? null : labelIndexes.get(lookupKey);
        if (mapped == null) {
          throw new SQLException("Unknown set operation ORDER BY column: " + order.columnLabel());
        }
        index = mapped;
      } else {
        Set<String> expressionColumns = ColumnsUsed.inWhere(order.expression());
        for (String column : expressionColumns) {
          if (column == null) {
            continue;
          }
          String lookupKey = Identifier.lookupKey(column);
          if (lookupKey == null || !labelIndexes.containsKey(lookupKey)) {
            throw new SQLException("Unknown set operation ORDER BY expression column: " + column);
          }
        }
        index = -1;
        hasExpressions = true;
      }
      if ((index < 0 && order.expression() == null) || index >= labels.size()) {
        throw new SQLException("Set operation ORDER BY column index out of range: " + (index + 1));
      }
      resolved.add(new ResolvedSetOrder(index, order.asc(), order.expression(), order.nullOrdering()));
    }
    ValueExpressionEvaluator evaluator = null;
    Schema setOrderSchema = null;
    if (hasExpressions) {
      setOrderSchema = buildSetOrderSchema(labels, sqlTypes);
      CorrelationMappings mappings = new CorrelationMappings(List.of(), Map.of(), Map.of(), Map.of());
      evaluator = new ValueExpressionEvaluator(setOrderSchema, null, mappings, WindowState.empty());
    }

    List<SortableSetRow> sortableRows = new ArrayList<>(rows.size());
    for (List<Object> row : rows) {
      List<Object> keys = new ArrayList<>(resolved.size());
      for (ResolvedSetOrder order : resolved) {
        Object keyVal;
        if (order.expression() != null) {
          keyVal = evaluateSetOrderExpression(order.expression(), row, labels, setOrderSchema, evaluator);
        } else {
          keyVal = row.get(order.index());
        }
        keys.add(keyVal);
      }
      sortableRows.add(new SortableSetRow(row, keys));
    }

    sortableRows.sort((left, right) -> compareSetRows(left.keys(), right.keys(), resolved));
    List<List<Object>> sorted = new ArrayList<>(sortableRows.size());
    for (SortableSetRow sortableRow : sortableRows) {
      sorted.add(sortableRow.row());
    }
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
   * Merge set operation components using DISTINCT semantics. Known scalar numeric
   * columns share the common set type; OTHER columns normalize mixed whole-number
   * representations as well as nested content. All duplicates present in the
   * accumulated rows or the incoming rows are removed while preserving the
   * original encounter order.
   *
   * @param accumulated
   *          rows previously materialized
   * @param incoming
   *          rows produced by the current component
   * @param keyFunction
   *          key normalization policy for the final output column types
   * @return a combined list containing only distinct rows
   */
  private List<List<Object>> mergeDistinct(List<List<Object>> accumulated, List<List<Object>> incoming,
      UnaryOperator<List<Object>> keyFunction) {
    LinkedHashSet<List<Object>> unique = new LinkedHashSet<>(accumulated.size() + incoming.size());
    List<List<Object>> result = new ArrayList<>(accumulated.size() + incoming.size());
    for (List<Object> row : accumulated) {
      List<Object> key = keyFunction.apply(row);
      if (unique.add(key)) {
        result.add(row);
      }
    }
    for (List<Object> row : incoming) {
      List<Object> key = keyFunction.apply(row);
      if (unique.add(key)) {
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
   * @param keyFunction
   *          key normalization policy for the final output column types
   * @return a list containing rows that appear in both inputs without duplicates
   */
  private List<List<Object>> intersectDistinct(List<List<Object>> accumulated, List<List<Object>> incoming,
      UnaryOperator<List<Object>> keyFunction) {
    if (accumulated.isEmpty() || incoming.isEmpty()) {
      return new ArrayList<>();
    }
    LinkedHashSet<List<Object>> incomingSet = new LinkedHashSet<>(incoming.stream().map(keyFunction).toList());
    LinkedHashSet<List<Object>> result = new LinkedHashSet<>();
    List<List<Object>> ordered = new ArrayList<>();
    for (List<Object> row : accumulated) {
      List<Object> key = keyFunction.apply(row);
      if (incomingSet.contains(key) && result.add(key)) {
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
   * @param keyFunction
   *          key normalization policy for the final output column types
   * @return a list containing rows that appear in both inputs according to
   *         INTERSECT ALL semantics
   */
  private List<List<Object>> intersectAll(List<List<Object>> accumulated, List<List<Object>> incoming,
      UnaryOperator<List<Object>> keyFunction) {
    if (accumulated.isEmpty() || incoming.isEmpty()) {
      return new ArrayList<>();
    }
    Map<List<Object>, Integer> remaining = new HashMap<>();
    for (List<Object> row : incoming) {
      List<Object> key = keyFunction.apply(row);
      remaining.merge(key, 1, Integer::sum);
    }
    List<List<Object>> result = new ArrayList<>();
    for (List<Object> row : accumulated) {
      List<Object> key = keyFunction.apply(row);
      Integer count = remaining.get(key);
      if (count != null && count > 0) {
        result.add(row);
        if (count == 1) {
          remaining.remove(key);
        } else {
          remaining.put(key, count - 1);
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
   * @param keyFunction
   *          key normalization policy for the final output column types
   * @return a list containing rows that appear only in the left operand
   */
  private List<List<Object>> exceptDistinct(List<List<Object>> accumulated, List<List<Object>> incoming,
      UnaryOperator<List<Object>> keyFunction) {
    if (accumulated.isEmpty()) {
      return new ArrayList<>();
    }
    if (incoming.isEmpty()) {
      return mergeDistinct(accumulated, List.of(), keyFunction);
    }
    LinkedHashSet<List<Object>> incomingSet = new LinkedHashSet<>(incoming.stream().map(keyFunction).toList());
    LinkedHashSet<List<Object>> emitted = new LinkedHashSet<>();
    List<List<Object>> ordered = new ArrayList<>();
    for (List<Object> row : accumulated) {
      List<Object> key = keyFunction.apply(row);
      if (!incomingSet.contains(key) && emitted.add(key)) {
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
   * @param keyFunction
   *          key normalization policy for the final output column types
   * @return a list containing rows that remain after applying EXCEPT ALL
   *         semantics
   */
  private List<List<Object>> exceptAll(List<List<Object>> accumulated, List<List<Object>> incoming,
      UnaryOperator<List<Object>> keyFunction) {
    if (accumulated.isEmpty()) {
      return new ArrayList<>();
    }
    if (incoming.isEmpty()) {
      return new ArrayList<>(accumulated);
    }
    Map<List<Object>, Integer> remaining = new HashMap<>();
    for (List<Object> row : incoming) {
      List<Object> key = keyFunction.apply(row);
      remaining.merge(key, 1, Integer::sum);
    }
    List<List<Object>> result = new ArrayList<>();
    for (List<Object> row : accumulated) {
      List<Object> key = keyFunction.apply(row);
      Integer count = remaining.get(key);
      if (count == null) {
        result.add(row);
      } else if (count == 1) {
        remaining.remove(key);
      } else {
        remaining.put(key, count - 1);
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
    for (int i = 0; i < orders.size(); i++) {
      ResolvedSetOrder order = orders.get(i);
      Object lv = left.get(i);
      Object rv = right.get(i);
      int cmp;
      if (lv == null || rv == null) {
        cmp = OrderingUtil.compareNulls(lv, rv, order.asc(), order.nullOrdering());
      } else {
        cmp = compareSetValues(lv, rv);
        if (!order.asc()) {
          cmp = -cmp;
        }
      }
      if (cmp != 0) {
        return cmp;
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
      return SqlComparisons.compareNumbers(lNum, rNum);
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

  private Schema buildSetOrderSchema(List<String> labels, List<Integer> sqlTypes) {
    if (labels == null || labels.isEmpty()) {
      return SchemaBuilder.record("set_order").fields().endRecord();
    }
    Schema schema = Schema.createRecord("set_order", null, null, false);
    List<Field> fields = new ArrayList<>(labels.size());
    Set<String> used = new LinkedHashSet<>();
    for (int i = 0; i < labels.size(); i++) {
      String label = labels.get(i);
      Schema baseType = mapSqlTypeToAvroType(sqlTypes, i);
      Schema union = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), baseType));
      Field field = new Field(MaterializedNames.allocate(label, used), union, null, Field.NULL_DEFAULT_VALUE);
      field.addProp(MaterializedNames.LOOKUP_KEY, Identifier.lookupKey(label));
      fields.add(field);
    }
    schema.setFields(fields);
    return schema;
  }

  private Schema mapSqlTypeToAvroType(List<Integer> sqlTypes, int index) {
    Schema.Type type = Schema.Type.STRING;
    if (sqlTypes != null && index >= 0 && index < sqlTypes.size()) {
      Integer sqlType = sqlTypes.get(index);
      if (sqlType != null) {
        type = switch (sqlType) {
          case Types.BOOLEAN, Types.BIT -> Schema.Type.BOOLEAN;
          case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> Schema.Type.INT;
          case Types.BIGINT -> Schema.Type.LONG;
          case Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL -> Schema.Type.DOUBLE;
          case Types.DATE, Types.TIME, Types.TIMESTAMP -> Schema.Type.LONG;
          case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR ->
            Schema.Type.STRING;
          default -> Schema.Type.STRING;
        };
      }
    }
    return Schema.create(type);
  }

  private Object evaluateSetOrderExpression(Expression expression, List<Object> row, List<String> labels, Schema schema,
      ValueExpressionEvaluator evaluator) throws SQLException {
    if (expression == null || evaluator == null || schema == null) {
      return null;
    }
    GenericData.Record record = new GenericData.Record(schema);
    for (int i = 0; i < labels.size() && i < row.size(); i++) {
      Schema.Field field = schema.getFields().get(i);
      if (field == null) {
        continue;
      }
      record.put(field.name(), adaptValueForSchema(field.schema(), row.get(i)));
    }
    return evaluator.eval(expression, record);
  }

  private Object adaptValueForSchema(Schema schema, Object value) throws SQLException {
    if (value == null || schema == null) {
      return null;
    }
    Schema base = schema.getType() == Schema.Type.UNION
        ? schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst()
            .orElse(Schema.create(Schema.Type.STRING))
        : schema;
    return switch (base.getType()) {
      case BOOLEAN -> (value instanceof Boolean) ? value : Boolean.parseBoolean(value.toString());
      case INT -> {
        if (value instanceof Number) {
          yield ((Number) value).intValue();
        }
        try {
          yield Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
          throw new SQLException("Cannot convert value to INT: " + value, e);
        }
      }
      case LONG -> {
        switch (value) {
          case Date date -> {
            yield date.getTime();
          }
          case Time time -> {
            yield time.getTime();
          }
          case Timestamp ts -> {
            yield ts.getTime();
          }
          case Number number -> {
            yield number.longValue();
          }
          default -> {
          }
        }
        try {
          yield Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
          throw new SQLException("Cannot convert value to LONG: " + value, e);
        }
      }
      case DOUBLE -> {
        if (value instanceof BigDecimal bigDecimal) {
          yield bigDecimal.doubleValue();
        }
        if (value instanceof Number num) {
          yield num.doubleValue();
        }
        try {
          yield Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
          throw new SQLException("Cannot convert value to DOUBLE: " + value, e);
        }
      }
      case STRING -> value.toString();
      default -> value;
    };
  }

  /**
   * Internal representation of a set operation ORDER BY directive once column
   * positions have been resolved.
   *
   * @param index
   *          resolved output position
   * @param asc
   *          whether to sort ascending
   * @param expression
   *          computed ordering expression, or {@code null}
   * @param nullOrdering
   *          explicit NULL placement, or {@code null} for the default
   */
  private record ResolvedSetOrder(int index, boolean asc, Expression expression,
      OrderByElement.NullOrdering nullOrdering) {
  }

  private record SortableSetRow(List<Object> row, List<Object> keys) {
  }

  /**
   * Materialised result of a common table expression.
   *
   */
  private static final class CteResult {
    private final Schema schema;
    private final List<GenericRecord> rows;
    // Owned by this immutable materialization; fresh correlated/recursive results
    // receive a new cache, while inherited CTE references reuse renamed records.
    private final Map<List<String>, CteResult> aliasedResults = new LinkedHashMap<>();

    /**
     * Creates one materialized CTE with its own alias cache.
     *
     * @param schema
     *          Avro schema describing the rows
     * @param rows
     *          rows produced by this materialization
     */
    private CteResult(Schema schema, List<GenericRecord> rows) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
    }

    /**
     * Returns the materialized schema.
     *
     * @return the materialized Avro schema
     */
    private Schema schema() {
      return schema;
    }

    /**
     * Returns the rows owned by this materialization.
     *
     * @return the immutable materialized row list
     */
    private List<GenericRecord> rows() {
      return rows;
    }
  }

  /**
   * Reuses validated physical schemas within this prepared plan.
   *
   * @param location
   *          logical input table
   * @param configuration
   *          footer reader configuration
   * @return the plan's validated table
   * @throws SQLException
   *           if a shard cannot be read or has an incompatible schema
   */
  private ParquetTable validatedTable(JParqConnection.TableLocation location, Configuration configuration)
      throws SQLException {
    if (reusablePlan != null) {
      return reusablePlan.table(location, configuration);
    }
    ParquetTable table = validatedTables.get(location);
    if (table == null) {
      table = ParquetTable.validate(location, configuration);
      validatedTables.put(location, table);
    }
    return table;
  }

  /** Plans single-table WHERE conjuncts before opening join inputs. */
  private Map<Integer, FilterPredicate> planJoinPredicates() throws SQLException {
    if (!stmt.getConn().isPredicatePushdown() || parsedSelect.where() == null) {
      return Map.of();
    }
    List<Schema> schemas = new ArrayList<>();
    for (var reference : tableReferences) {
      Schema schema = null;
      if (!reference.isDerivedTable() && reference.subquery() == null && reference.commonTableExpression() == null
          && reference.valueTable() == null && reference.unnest() == null && reference.joinGroup().isEmpty()
          && !InformationSchemaTables.matchesTableReference(reference.tableName())
          && !InformationSchemaColumns.matchesTableReference(reference.tableName())
          && resolveCteResultByName(reference.tableName(), cteResults) == null) {
        var location = stmt.getConn().resolveTable(reference.schemaName(), reference.tableName());
        schema = validatedTable(location, HadoopConfigurations.create()).schema();
      }
      schemas.add(schema);
    }
    Map<Integer, FilterPredicate> predicates = new LinkedHashMap<>();
    for (int index = 0; index < tableReferences.size(); index++) {
      int participant = index;
      var predicate = reusablePlan == null
          ? JoinPredicatePlanner.plan(parsedSelect.where(), tableReferences, schemas, participant)
          : ParquetFilterBuilder.withCorrelatedBindings(
              () -> JoinPredicatePlanner.plan(parsedSelect.where(), tableReferences, schemas, participant));
      if (predicate.isPresent()) {
        predicates.put(index, predicate.get());
      }
    }
    return Map.copyOf(predicates);
  }

  /**
   * Loads join inputs under the current execution's cancellation state.
   *
   * @param cancellation
   *          shared execution state
   * @return materialized join reader
   * @throws SQLException
   *           if loading fails or execution is canceled
   */
  private JoinRecordReader buildJoinReader(QueryCancellation cancellation) throws SQLException {
    JoinRecordReader joinReader = buildJoinReader(tableReferences, joinPredicates, cancellation);
    validateJoinOutputColumns(joinReader);
    expandJoinQualifiedWildcards(joinReader);
    return joinReader;
  }

  /**
   * Materializes one join level while retaining nested grouping boundaries.
   *
   * @param references
   *          ordered inputs at this join level
   * @param predicates
   *          physical filters safe to apply at this level
   * @param cancellation
   *          shared execution cancellation state
   * @return reader over this join level
   * @throws SQLException
   *           if an input or predicate cannot be evaluated
   */
  private JoinRecordReader buildJoinReader(List<SqlParser.TableReference> references,
      Map<Integer, FilterPredicate> predicates, QueryCancellation cancellation) throws SQLException {
    List<JoinRecordReader.JoinTable> tables = new ArrayList<>();
    Map<String, Integer> qualifierIndex = new LinkedHashMap<>();
    for (int referenceIndex = 0; referenceIndex < references.size(); referenceIndex++) {
      cancellation.checkCanceled();
      SqlParser.TableReference ref = references.get(referenceIndex);
      if (!ref.joinGroup().isEmpty()) {
        JoinRecordReader.JoinTable table = materializeJoinGroup(ref, cancellation);
        tables.add(table);
        registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
        continue;
      }
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
        // VALUES inputs can reuse relation names in different nested groups.
        // Materialize this reference rather than consulting the name-only cache.
        CteResult valueResult = materializeValueTable(ref.valueTable(), deriveValueTableName(ref));
        String tableName = deriveValueTableName(ref);
        List<GenericRecord> rows = applySampleToRows(ref.tableSample(), valueResult.rows());
        JoinRecordReader.JoinTable table = new JoinRecordReader.JoinTable(tableName, ref.tableAlias(),
            valueResult.schema(), rows, ref.joinType(), ref.joinCondition(), ref.usingColumns(), null);
        tables.add(table);
        registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
        continue;
      }
      if (InformationSchemaTables.matchesTableReference(ref.tableName())) {
        rejectTableColumnAliases(ref);
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
        rejectTableColumnAliases(ref);
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
      } else if (!ref.isDerivedTable() && ref.tableName() != null) {
        cteResult = resolveCteResultByName(ref.tableName(), cteResults);
      }
      if (cteResult != null) {
        cteResult = applyColumnAliases(cteResult, ref);
        validateJoinTableColumns(cteResult.schema(), ref);
        String tableName = ref.tableAlias() != null ? ref.tableAlias() : ref.tableName();
        List<GenericRecord> rows = applySampleToRows(ref.tableSample(), cteResult.rows());
        JoinRecordReader.JoinTable table = new JoinRecordReader.JoinTable(tableName, ref.tableAlias(),
            cteResult.schema(), rows, ref.joinType(), ref.joinCondition(), ref.usingColumns(), null);
        tables.add(table);
        registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
        continue;
      }
      if (ref.isDerivedTable()) {
        String tableName = ref.tableAlias() != null ? ref.tableAlias() : ref.tableName();
        JoinRecordReader.JoinTable table = ref.lateral()
            ? materializeLateralSubqueryTable(ref, tableName, tables, qualifierIndex)
            : materializeSubqueryTable(ref, tableName);
        validateJoinTableColumns(table.schema(), ref);
        tables.add(table);
        registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
        continue;
      }
      String tableName = ref.tableName();
      rejectTableColumnAliases(ref);
      if (tableName == null || tableName.isBlank()) {
        throw new SQLException("JOIN requires explicit table names");
      }
      JParqConnection.TableLocation tableLocation = stmt.getConn().resolveTable(ref.schemaName(), tableName);
      Configuration tableConf = HadoopConfigurations.copy(conf);
      ParquetTable input = validatedTable(tableLocation, tableConf);
      Schema tableSchema = input.schema();
      validateJoinTableColumns(tableSchema, ref);
      List<GenericRecord> rows = new ArrayList<>();
      FilterPredicate predicate = predicates.get(referenceIndex);
      try (RecordReader tableReader = cancellation.track(input.open(tableConf, predicate))) {
        GenericRecord record = tableReader.read();
        while (record != null) {
          rows.add(record);
          record = tableReader.read();
        }
      } catch (IOException e) {
        throw new SQLException("Failed to read data for table " + tableName, e);
      }
      rows = applySampleToRows(ref.tableSample(), rows);
      JoinRecordReader.JoinTable table = new JoinRecordReader.JoinTable(tableLocation.tableName(), ref.tableAlias(),
          tableSchema, rows, ref.joinType(), ref.joinCondition(), ref.usingColumns(), null);
      tables.add(table);
      registerQualifierIndexes(qualifierIndex, table, tables.size() - 1);
    }
    try (var executor = createSubqueryExecutor(cancellation)) {
      validateJoinConditions(tables);
      return new JoinRecordReader(tables, executor);
    } catch (IllegalArgumentException e) {
      throw new SQLException("Failed to build join reader", e);
    }
  }

  /**
   * Materializes a parenthesized join as one input while retaining its names.
   *
   * @param reference
   *          grouped input and its enclosing join condition
   * @param cancellation
   *          shared execution state
   * @return grouped rows, schema, and visible qualifier mappings
   * @throws SQLException
   *           if a nested join or alias list is invalid
   */
  private JoinRecordReader.JoinTable materializeJoinGroup(SqlParser.TableReference reference,
      QueryCancellation cancellation) throws SQLException {
    try (JoinRecordReader nested = buildJoinReader(reference.joinGroup(), Map.of(), cancellation)) {
      List<GenericRecord> rows = new ArrayList<>();
      GenericRecord row;
      while ((row = nested.read()) != null) {
        cancellation.checkCanceled();
        rows.add(row);
      }
      CteResult result = applyColumnAliases(new CteResult(nested.schema(), rows), reference);
      Map<String, Map<String, String>> qualifiers = new LinkedHashMap<>();
      Map<String, Map<String, String>> nestedMappings = nested.qualifierColumnMapping();
      if (reference.tableAlias() == null) {
        for (String qualifier : SqlParser.visibleQualifiers(reference.joinGroup())) {
          Map<String, String> columns = nestedMappings.get(JParqUtil.normalizeQualifier(qualifier));
          if (columns != null) {
            qualifiers.put(qualifier, columns);
          }
        }
      }
      return new JoinRecordReader.JoinTable(null, reference.tableAlias(), result.schema(),
          applySampleToRows(reference.tableSample(), result.rows()), reference.joinType(), reference.joinCondition(),
          reference.usingColumns(), null, qualifiers);
    } catch (IOException e) {
      throw new SQLException("Failed to read parenthesized join", e);
    }
  }

  /**
   * Validates ON references against only the inputs visible at that join step,
   * honoring the shared quoted lowercase name fallback.
   *
   * @param tables
   *          ordered join participants
   * @throws SQLException
   *           if an ON expression references a missing column
   */
  private void validateJoinConditions(List<JoinRecordReader.JoinTable> tables) throws SQLException {
    Set<String> available = new LinkedHashSet<>();
    Map<String, Map<String, String>> qualified = new LinkedHashMap<>();
    for (JoinRecordReader.JoinTable table : tables) {
      Map<String, String> columns = new LinkedHashMap<>();
      for (Field field : table.schema().getFields()) {
        columns.put(MaterializedNames.lookupKey(field), field.name());
      }
      available.addAll(columns.keySet());
      for (var nested : table.nestedQualifiers().entrySet()) {
        qualified.put(JParqUtil.normalizeQualifier(nested.getKey()), nested.getValue());
      }
      String qualifier = table.alias() == null ? table.tableName() : table.alias();
      if (qualifier != null) {
        qualified.put(JParqUtil.normalizeQualifier(qualifier), columns);
      }
      Set<String> missing = new LinkedHashSet<>();
      for (Column column : ColumnsUsed.referencesInScope(table.joinCondition())) {
        String referenceQualifier = column.getTable() == null ? null : column.getTable().getName();
        Set<String> visible = referenceQualifier == null || referenceQualifier.isBlank()
            ? available
            : ColumnMappingUtil.qualifierColumns(qualified, referenceQualifier).keySet();
        if (!containsColumn(visible, normalizeColumnKey(column.getColumnName()))) {
          missing.add(column.toString());
        }
      }
      if (!missing.isEmpty()) {
        throw SqlExceptions.syntaxFailure("Missing columns in JOIN condition: " + String.join(", ", missing));
      }
    }
  }

  /**
   * Rejects alias lists on table inputs without positional alias binding.
   *
   * @param reference
   *          resolved physical or information_schema reference, or null
   * @throws SQLException
   *           if an unsupported table input declares column aliases
   */
  private static void rejectTableColumnAliases(SqlParser.TableReference reference) throws SQLException {
    if (reference != null && !reference.columnAliases().isEmpty()) {
      if (InformationSchemaTables.matchesTableReference(reference.tableName())
          || InformationSchemaColumns.matchesTableReference(reference.tableName())) {
        throw SqlExceptions.syntaxFailure("Column alias lists on information_schema inputs are not supported; "
            + "use a derived table or CTE instead");
      }
      throw SqlExceptions.syntaxFailure(
          "Column alias lists on physical tables are not supported (#263); " + "use a derived table or CTE instead");
    }
  }

  /**
   * Chooses the visible relation name for column diagnostics.
   *
   * @param reference
   *          input reference, or null
   * @return alias or table name, without exposing a derived input's physical
   *         table
   */
  private static String referenceName(SqlParser.TableReference reference) {
    if (reference == null) {
      return null;
    }
    return reference.tableAlias() != null || reference.isDerivedTable()
        ? reference.tableAlias()
        : reference.tableName();
  }

  /**
   * Validates unqualified references against the combined visible join schema.
   * Qualified references are also checked against their individual inputs.
   *
   * @param reader
   *          combined join schema and qualifier mappings
   * @throws SQLException
   *           if a column is missing
   */
  private void validateJoinOutputColumns(JoinRecordReader reader) throws SQLException {
    final Schema schema = reader.schema();
    final Set<String> required = new LinkedHashSet<>();
    List<Expression> expressions = new ArrayList<>(parsedSelect.expressions());
    expressions.add(parsedSelect.where());
    expressions.addAll(parsedSelect.groupByExpressions());
    for (SqlParser.OrderKey key : parsedSelect.orderBy()) {
      expressions.add(key.expression());
    }
    for (Expression expression : expressions) {
      addColumns(required, ColumnsUsed.inScope(expression));
    }
    if (tableReferences.stream().anyMatch(ref -> !ref.joinGroup().isEmpty())) {
      Map<String, Map<String, String>> qualifierMapping = reader.qualifierColumnMapping();
      List<Expression> qualifiedExpressions = new ArrayList<>(expressions);
      qualifiedExpressions.add(parsedSelect.having());
      for (Expression expression : qualifiedExpressions) {
        for (Column column : ColumnsUsed.referencesInScope(expression)) {
          String qualifier = column.getTable() == null ? null : column.getTable().getName();
          validateGroupedQualifier(qualifierMapping, qualifier, column.getColumnName());
        }
      }
      for (SqlParser.OrderKey key : parsedSelect.orderBy()) {
        validateGroupedQualifier(qualifierMapping, key.qualifier(), key.column());
      }
    }
    Set<String> referenced = new LinkedHashSet<>(required);
    for (SqlParser.OrderKey key : parsedSelect.orderBy()) {
      if (key.expression() == null && !isOrdinalReference(key.column()) && !isGroupingFunctionReference(key.column())) {
        addColumn(required, key.column());
      }
    }
    addColumns(required, ColumnsUsed.inScope(parsedSelect.having()));
    validateRequiredColumns(schema, required, null, parsedSelect.labels(), parsedSelect.columnNames(), referenced);
  }

  /**
   * Validates a qualified reference against the names exported by join groups.
   *
   * @param qualifierMapping
   *          visible qualifier mappings for the join output
   * @param qualifier
   *          optional SQL relation qualifier
   * @param column
   *          SQL column identifier
   * @throws SQLException
   *           if the qualifier does not export the column or its label is
   *           ambiguous
   */
  private static void validateGroupedQualifier(Map<String, Map<String, String>> qualifierMapping, String qualifier,
      String column) throws SQLException {
    if (qualifier == null || qualifier.isBlank()) {
      return;
    }
    Map<String, String> columns = ColumnMappingUtil.qualifierColumns(qualifierMapping, qualifier);
    String columnKey = Identifier.lookupKey(column);
    String alternative = ColumnMappingUtil.alternativeColumnKey(columnKey);
    if (!columns.containsKey(columnKey) && (alternative == null || !columns.containsKey(alternative))) {
      throw SqlExceptions.syntaxFailure("Missing columns in parenthesized JOIN: " + qualifier + "." + column);
    }
    String canonical = columns.containsKey(columnKey) ? columns.get(columnKey) : columns.get(alternative);
    if ("".equals(canonical)) {
      throw SqlExceptions.syntaxFailure("Ambiguous column reference: " + qualifier + "." + column);
    }
  }

  /**
   * Ensure that a join participant exposes all columns referenced in the query
   * and join predicates.
   *
   * @param schema
   *          schema describing the join table
   * @param ref
   *          table reference used in the join
   * @throws SQLException
   *           if a referenced column is missing
   */
  private void validateJoinTableColumns(Schema schema, SqlParser.TableReference ref) throws SQLException {
    if (schema == null || ref == null) {
      return;
    }
    Set<String> required = collectColumnsForTable(ref);
    if (ref.usingColumns() != null) {
      required.addAll(ref.usingColumns());
    }
    List<String> labels = parsedSelect == null ? List.of() : parsedSelect.labels();
    List<String> columnNames = parsedSelect == null ? List.of() : parsedSelect.columnNames();
    validateRequiredColumns(schema, required, referenceName(ref), labels, columnNames, required);
  }

  /**
   * Collect all column names referenced for a specific table in the parsed SELECT
   * statement.
   *
   * @param ref
   *          table reference being validated
   * @return mutable set of referenced column names (empty if none)
   */
  private Set<String> collectColumnsForTable(SqlParser.TableReference ref) {
    if (parsedSelect == null || ref == null) {
      return new LinkedHashSet<>();
    }
    List<String> qualifiers = qualifierList(ref, false);
    if (qualifiers.isEmpty()) {
      return new LinkedHashSet<>();
    }
    boolean singleTable = !parsedSelect.hasMultipleTables();
    List<String> primaryQualifier = singleTable ? qualifierList(ref, true) : List.of();
    Set<String> required = new LinkedHashSet<>();
    collectColumnReferencesFromList(parsedSelect.expressions(), qualifiers, primaryQualifier, required);
    collectColumnReferencesFromExpression(parsedSelect.where(), qualifiers, primaryQualifier, required);
    for (Expression group : parsedSelect.groupByExpressions()) {
      collectColumnReferencesFromExpression(group, qualifiers, primaryQualifier, required);
    }
    for (SqlParser.OrderKey key : parsedSelect.orderBy()) {
      if (shouldIncludeOrderKey(key, qualifiers, singleTable)) {
        addColumn(required, key.column());
      }
    }
    for (SqlParser.OrderKey key : parsedSelect.preOrderBy()) {
      if (shouldIncludeOrderKey(key, qualifiers, singleTable)) {
        addColumn(required, key.column());
      }
    }
    collectColumnReferencesFromExpression(ref.joinCondition(), qualifiers, primaryQualifier, required);
    return required;
  }

  /**
   * Collect column references from a list of expressions using the provided
   * qualifiers.
   *
   * @param expressions
   *          expressions to inspect
   * @param qualifiers
   *          qualifiers that identify the target table
   * @param primaryQualifier
   *          single qualifier to use for unqualified columns (empty when
   *          ambiguous)
   * @param target
   *          set receiving the column names
   */
  private void collectColumnReferencesFromList(List<Expression> expressions, List<String> qualifiers,
      List<String> primaryQualifier, Set<String> target) {
    collectColumnReferencesFromList(expressions, qualifiers, primaryQualifier, target, null);
  }

  private void collectColumnReferencesFromList(List<Expression> expressions, List<String> qualifiers,
      List<String> primaryQualifier, Set<String> target, Set<String> referenced) {
    if (expressions == null || expressions.isEmpty()) {
      return;
    }
    for (Expression expression : expressions) {
      collectColumnReferencesFromExpression(expression, qualifiers, primaryQualifier, target, referenced);
    }
  }

  /**
   * Collect column references from a single expression.
   *
   * @param expression
   *          expression to examine
   * @param qualifiers
   *          qualifiers that identify the target table
   * @param primaryQualifier
   *          single qualifier used when unqualified columns should be included
   * @param target
   *          set receiving the column names
   */
  private void collectColumnReferencesFromExpression(Expression expression, List<String> qualifiers,
      List<String> primaryQualifier, Set<String> target) {
    collectColumnReferencesFromExpression(expression, qualifiers, primaryQualifier, target, null);
  }

  private void collectColumnReferencesFromExpression(Expression expression, List<String> qualifiers,
      List<String> primaryQualifier, Set<String> target, Set<String> referenced) {
    if (expression == null || target == null) {
      return;
    }
    Set<String> collected = SqlParser.collectQualifiedColumns(expression, qualifiers, false);
    addColumns(target, collected);
    addColumns(referenced, collected);
    if (primaryQualifier != null && !primaryQualifier.isEmpty()) {
      Set<String> primaryCollected = SqlParser.collectQualifiedColumns(expression, primaryQualifier);
      addColumns(target, primaryCollected);
      addColumns(referenced, primaryCollected);
    }
  }

  /**
   * Decide if an ORDER BY key should be associated with the current table.
   *
   * @param key
   *          order key from the parsed statement
   * @param qualifiers
   *          qualifiers that map to the current table
   * @param singleTable
   *          flag indicating whether the query only references one table
   * @return true if the column belongs to the table being validated
   */
  private boolean shouldIncludeOrderKey(SqlParser.OrderKey key, List<String> qualifiers, boolean singleTable) {
    if (key == null || key.column() == null) {
      return false;
    }
    if (isOrdinalReference(key.column())) {
      return false;
    }
    if (singleTable) {
      return true;
    }
    return matchesQualifier(key.qualifier(), qualifiers);
  }

  /**
   * Check whether a qualifier matches any of the supplied candidates.
   *
   * @param qualifier
   *          qualifier to compare
   * @param candidates
   *          potential matches
   * @return true if the qualifier matches a candidate
   */
  private boolean matchesQualifier(String qualifier, List<String> candidates) {
    if (qualifier == null || candidates == null || candidates.isEmpty()) {
      return false;
    }
    String normalized = JParqUtil.normalizeQualifier(qualifier);
    for (String candidate : candidates) {
      if (normalized != null && normalized.equals(JParqUtil.normalizeQualifier(candidate))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Build a list of qualifiers for a table reference.
   *
   * @param ref
   *          table reference
   * @param primaryOnly
   *          whether to return a single qualifier for unqualified lookups
   * @return ordered list of qualifiers (alias before table name)
   */
  private List<String> qualifierList(SqlParser.TableReference ref, boolean primaryOnly) {
    if (ref == null) {
      return List.of();
    }
    List<String> qualifiers = new ArrayList<>(2);
    if (ref.tableAlias() != null && !ref.tableAlias().isBlank()) {
      qualifiers.add(ref.tableAlias());
      if (primaryOnly) {
        return List.copyOf(qualifiers);
      }
    }
    if (ref.tableName() != null && !ref.tableName().isBlank()) {
      qualifiers.add(ref.tableName());
    }
    return qualifiers.isEmpty() ? List.of() : List.copyOf(qualifiers);
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
    for (String qualifier : table.nestedQualifiers().keySet()) {
      registerQualifierIndex(index, qualifier, position, true);
    }
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
      throw SqlExceptions.syntaxFailure(
          "Unable to resolve columns for qualified wildcard projections: " + parsedSelect.qualifiedWildcards());
    }
    try {
      parsedSelect = SqlParser.expandQualifiedWildcards(parsedSelect, qualifierColumns);
    } catch (IllegalArgumentException e) {
      throw new SQLSyntaxErrorException("Failed to expand qualified wildcard projections: " + e.getMessage(), "42000",
          e);
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
    for (SqlParser.TableReference reference : tableReferences) {
      addQualifierColumns(mapping, reference.tableAlias(), qualifierMapping, columnOrder, canonicalLabels);
      addQualifierColumns(mapping, reference.tableName(), qualifierMapping, columnOrder, canonicalLabels);
      if (reference.tableAlias() == null && !reference.joinGroup().isEmpty()) {
        for (String qualifier : SqlParser.visibleQualifiers(reference.joinGroup())) {
          addQualifierColumns(mapping, qualifier, qualifierMapping, columnOrder, canonicalLabels);
        }
      }
    }
    return mapping;
  }

  private void addQualifierColumns(Map<String, List<QualifiedExpansionColumn>> target, String qualifier,
      Map<String, Map<String, String>> qualifierMapping, List<String> columnOrder,
      Map<String, String> canonicalLabels) {
    if (qualifier == null || qualifier.isBlank()) {
      return;
    }
    Identifier qualifierId = Identifier.of(qualifier);
    if (qualifierId == null) {
      return;
    }
    Map<String, String> columns = qualifierMapping.getOrDefault(qualifierId.lookupKey(), Map.of());
    if (columns == null || columns.isEmpty()) {
      return;
    }
    if (columns.containsValue("") && parsedSelect.qualifiedWildcards().stream().anyMatch(
        wildcard -> Identifier.lookupKey(wildcard.qualifier()).equals(Identifier.lookupKey(qualifierId.text())))) {
      throw SqlExceptions.syntaxFailure("Ambiguous column in join alias: " + qualifier
          + "; assign unique column aliases before using a qualified wildcard");
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
      target.putIfAbsent(qualifierId.text(), List.copyOf(ordered));
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
      projections.add(new QualifiedExpansionColumn(MaterializedNames.sqlLabel(schema.getField(column)), column));
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
    Identifier identifier = Identifier.of(qualifier);
    if (identifier == null) {
      return;
    }
    String trimmed = qualifier.trim();
    String text = identifier.text();
    mapping.putIfAbsent(text, columns);
    if (!trimmed.equals(text)) {
      mapping.putIfAbsent(trimmed, columns);
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
   * Materialize a derived table used in a JOIN by executing the associated
   * subquery and adapting the result into {@link GenericRecord} instances.
   *
   * @param ref
   *          table reference describing the derived table
   * @param tableName
   *          SQL-visible name, or null for an anonymous join input
   * @return a {@link JoinRecordReader.JoinTable} backed by the subquery result
   * @throws SQLException
   *           if the subquery fails to execute or cannot be converted into an
   *           Avro representation
   */
  private JoinRecordReader.JoinTable materializeSubqueryTable(SqlParser.TableReference ref, String tableName)
      throws SQLException {
    String sql = ref.subquerySql();
    if (sql == null || sql.isBlank()) {
      throw new SQLException("Missing subquery SQL for derived table");
    }
    List<String> fieldNames = new ArrayList<>();
    List<Schema> valueSchemas = new ArrayList<>();
    SubqueryTableData data = executeSubquery(sql, ref, null, fieldNames, valueSchemas);
    List<GenericRecord> rows = applySampleToRows(ref.tableSample(), data.rows());
    return new JoinRecordReader.JoinTable(tableName, ref.tableAlias(), data.schema(), rows, ref.joinType(),
        ref.joinCondition(), ref.usingColumns(), null);
  }

  /**
   * Materialize a standalone {@code UNNEST} table reference into an in-memory
   * representation. Direct correlated column bindings retain declared element
   * schemas even for null or empty arrays; null arrays produce no rows.
   *
   * @param baseRef
   *          the table reference describing the {@code UNNEST} invocation
   * @return {@link CteResult} containing the unnested rows and schema
   * @throws SQLException
   *           if the {@code UNNEST} expression cannot be evaluated
   */
  private CteResult materializeUnnestTable(SqlParser.TableReference baseRef) throws SQLException {
    if (baseRef == null || baseRef.unnest() == null) {
      throw new SQLException("UNNEST table reference cannot be null");
    }
    Schema literalSchema = Schema.createRecord("unnest_literal", null, null, false);
    literalSchema.setFields(List.of());
    GenericData.Record emptyRecord = new GenericData.Record(literalSchema);
    ValueExpressionEvaluator evaluator = new ValueExpressionEvaluator(literalSchema);
    Object evaluated = evaluator.eval(baseRef.unnest().expression(), emptyRecord);
    Schema arraySchema = baseRef.unnest().expression() instanceof net.sf.jsqlparser.expression.Function function
        && CorrelatedBindings.matches(function)
            ? JdbcTypeMapper.nonNullSchema(CorrelatedBindings.schema(function))
            : null;
    Iterable<?> iterable = JParqUtil.toIterable(evaluated);
    if (iterable == null && evaluated != null) {
      throw new SQLException("UNNEST expression must evaluate to an array or iterable value");
    }
    List<Object> elements = new ArrayList<>();
    if (iterable != null) {
      for (Object element : iterable) {
        elements.add(element);
      }
    }
    Schema elementSchema = arraySchema != null && arraySchema.getType() == Schema.Type.ARRAY
        ? JdbcTypeMapper.nonNullSchema(arraySchema.getElementType())
        : resolveUnnestElementSchema(elements);
    String fallbackAlias = baseRef.tableAlias() != null && !baseRef.tableAlias().isBlank()
        ? baseRef.tableAlias()
        : "unnest";
    UnnestTableBuilder.AliasPlan aliasPlan = UnnestTableBuilder.AliasPlan.build(baseRef.unnest(), elementSchema,
        fallbackAlias);
    Schema unnestSchema = UnnestTableBuilder.buildSchema(baseRef, elementSchema, aliasPlan);
    List<GenericRecord> rows = UnnestTableBuilder.materializeRows(elements, elementSchema, aliasPlan, unnestSchema);
    return new CteResult(unnestSchema, rows);
  }

  /**
   * Resolve the Avro schema describing the elements produced by an {@code UNNEST}
   * expression.
   *
   * @param elements
   *          evaluated elements from the {@code UNNEST} input expression
   * @return schema describing a single element
   * @throws SQLException
   *           if the element type cannot be resolved
   */
  private Schema resolveUnnestElementSchema(List<Object> elements) throws SQLException {
    if (elements != null) {
      for (Object element : elements) {
        if (element instanceof GenericRecord record) {
          Schema schema = JdbcTypeMapper.nonNullSchema(record.getSchema());
          if (schema != null) {
            return schema;
          }
        }
      }
    }
    ValueColumnType elementType = determineColumnType(elements);
    return schemaForColumnType(elementType, elements);
  }

  private CteResult materializeValueTable(ValueTableDefinition valueTable, String tableName) throws SQLException {
    if (valueTable == null) {
      throw new SQLException("Value table definition cannot be null");
    }
    List<String> columnNames = valueTable.columnNames();
    // Zero columns and one empty row supply the implicit input for SELECT
    // without FROM; explicit VALUES constructors are validated by the parser.
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
      if (columnValues.stream().allMatch(Objects::isNull) && isBooleanValueColumn(valueTable, i)) {
        columnType = ValueColumnType.BOOLEAN;
      }
      columnTypes.add(columnType);
      valueSchemas.add(schemaForColumnType(columnType, columnValues));
    }

    List<Field> fields = new ArrayList<>(columnNames.size());
    Set<String> usedNames = new LinkedHashSet<>();
    for (int i = 0; i < columnNames.size(); i++) {
      Schema valueSchema = valueSchemas.get(i);
      Schema union = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), valueSchema));
      String label = columnNames.get(i);
      Field field = new Field(MaterializedNames.allocate(label, usedNames), union, null, Field.NULL_DEFAULT_VALUE);
      field.addProp(MaterializedNames.SQL_LABEL, label);
      field.addProp(MaterializedNames.LOOKUP_KEY, Identifier.lookupKey(label));
      fields.add(field);
    }

    String recordName = (tableName == null || tableName.isBlank()) ? "values" : tableName;
    Schema schema = Schema.createRecord(MaterializedNames.recordName(recordName), null, null, false);
    schema.setFields(fields);

    List<GenericRecord> records = new ArrayList<>(evaluatedRows.size());
    for (List<Object> row : evaluatedRows) {
      GenericData.Record record = new GenericData.Record(schema);
      for (int i = 0; i < columnNames.size(); i++) {
        Object normalized = normalizeValueForType(row.get(i), columnTypes.get(i));
        record.put(i, toAvroValue(normalized, valueSchemas.get(i)));
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

  /**
   * Recognizes explicitly Boolean VALUES columns even when every value is NULL.
   *
   * @param table
   *          the VALUES definition
   * @param column
   *          the zero-based column index
   * @return whether every typed expression is a Boolean cast and at least one
   *         such cast is present
   */
  private boolean isBooleanValueColumn(ValueTableDefinition table, int column) {
    boolean booleanCast = false;
    for (List<Expression> row : table.rows()) {
      Expression expression = ExpressionEvaluator.unwrapParenthesis(row.get(column));
      if (expression instanceof NullValue) {
        continue;
      }
      if (!(expression instanceof CastExpression cast) || cast.getColDataType() == null
          || !("BOOLEAN".equalsIgnoreCase(cast.getColDataType().getDataType())
              || "BOOL".equalsIgnoreCase(cast.getColDataType().getDataType()))) {
        return false;
      }
      booleanCast = true;
    }
    return booleanCast;
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
    if (value instanceof Object[] || value instanceof Collection<?>) {
      return ValueColumnType.ARRAY;
    }
    if (value instanceof GenericRecord) {
      return ValueColumnType.STRUCT;
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

  /**
   * Promotes materialized numeric values using the common SQL numeric families.
   *
   * @param left
   *          first numeric family
   * @param right
   *          second numeric family
   * @return the common value family
   */
  private ValueColumnType widenNumericType(ValueColumnType left, ValueColumnType right) {
    NumericType common = numericValueType(left).common(numericValueType(right));
    return switch (common.jdbcType()) {
      case Types.INTEGER -> ValueColumnType.INT;
      case Types.BIGINT -> ValueColumnType.LONG;
      case Types.REAL -> ValueColumnType.FLOAT;
      case Types.DOUBLE -> ValueColumnType.DOUBLE;
      case Types.DECIMAL -> ValueColumnType.DECIMAL;
      default -> throw new IllegalStateException("Unexpected numeric type: " + common);
    };
  }

  /**
   * Maps a materialized numeric family to the shared numeric descriptor.
   *
   * @param type
   *          materialized numeric family
   * @return its numeric descriptor
   */
  private NumericType numericValueType(ValueColumnType type) {
    return NumericType.of(switch (type) {
      case INT -> Types.INTEGER;
      case LONG -> Types.BIGINT;
      case FLOAT -> Types.REAL;
      case DOUBLE -> Types.DOUBLE;
      case DECIMAL -> Types.DECIMAL;
      default -> throw new IllegalArgumentException("Not a numeric family: " + type);
    }, 0, 0);
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
      case ARRAY -> inferredArraySchema(values);
      case STRUCT -> values.stream().filter(GenericRecord.class::isInstance).map(GenericRecord.class::cast).findFirst()
          .map(GenericRecord::getSchema).orElse(Schema.create(Schema.Type.STRING));
    };
  }

  /**
   * Infers one element schema across all materialized arrays, recursively.
   *
   * @param values
   *          arrays whose non-null elements determine the element type
   * @return array schema with nullable elements; unknown elements use STRING
   */
  private Schema inferredArraySchema(List<Object> values) {
    List<Object> elements = new ArrayList<>();
    for (Object value : values) {
      if (value instanceof Object[] array) {
        elements.addAll(Arrays.asList(array));
      } else if (value instanceof Collection<?> collection) {
        elements.addAll(collection);
      }
    }
    Schema element = schemaForColumnType(determineColumnType(elements), elements);
    return Schema.createArray(Schema.createUnion(Schema.create(Schema.Type.NULL), element));
  }

  /**
   * Builds a decimal schema covering all integer and fractional digits.
   *
   * @param values
   *          materialized numeric values, allowing nulls
   * @return the inferred decimal bytes schema
   * @throws NumberFormatException
   *           if a nonnull value is not numeric
   */
  private Schema decimalSchemaForValues(List<Object> values) {
    return DecimalTypeInference.infer(values).addToSchema(Schema.create(Schema.Type.BYTES));
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
        case DATE, TIME, TIMESTAMP, BINARY, ARRAY, STRUCT -> value;
        case STRING -> value instanceof CharSequence ? value : value.toString();
      };
    } catch (NumberFormatException | NullPointerException e) {
      throw new SQLException("Failed to convert value '" + value + "' to type " + type, e);
    }
  }

  private void registerValueTableResult(SqlParser.TableReference ref, CteResult result,
      Map<Identifier, CteResult> target) {
    if (ref == null || result == null || target == null) {
      return;
    }
    Identifier aliasKey = Identifier.of(ref.tableAlias());
    if (aliasKey != null) {
      target.put(aliasKey, result);
    }
    Identifier tableKey = Identifier.of(ref.tableName());
    if (tableKey != null && (aliasKey == null || !aliasKey.matches(tableKey))) {
      target.putIfAbsent(tableKey, result);
    }
  }

  private CteResult resolveValueTable(SqlParser.TableReference ref) {
    if (ref == null || valueTableResults == null || valueTableResults.isEmpty()) {
      return null;
    }
    Identifier aliasKey = Identifier.of(ref.tableAlias());
    if (aliasKey != null) {
      CteResult result = valueTableResults.get(aliasKey);
      if (result != null) {
        return result;
      }
    }
    Identifier tableKey = Identifier.of(ref.tableName());
    if (tableKey != null) {
      return valueTableResults.get(tableKey);
    }
    return null;
  }

  private String deriveValueTableName(SqlParser.TableReference ref) {
    if (ref == null) {
      return "values";
    }
    Identifier alias = Identifier.of(ref.tableAlias());
    if (alias != null) {
      return alias.text();
    }
    Identifier name = Identifier.of(ref.tableName());
    if (name != null) {
      return name.text();
    }
    return "values";
  }

  private enum ValueColumnType {
    ARRAY, STRUCT, BOOLEAN, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIME, TIMESTAMP, STRING, BINARY
  }

  /**
   * Build a {@link JoinRecordReader.JoinTable} backed by a correlated row
   * supplier for a {@code LATERAL} derived table.
   *
   * @param ref
   *          the table reference describing the derived table
   * @param tableName
   *          SQL-visible name, or null for an anonymous join input
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
  private JoinRecordReader.JoinTable materializeLateralSubqueryTable(SqlParser.TableReference ref, String tableName,
      List<JoinRecordReader.JoinTable> priorTables, Map<String, Integer> qualifierIndex) throws SQLException {
    String sql = ref.subquerySql();
    if (sql == null || sql.isBlank()) {
      throw new SQLException("Missing subquery SQL for derived table");
    }
    if (priorTables == null || priorTables.isEmpty()) {
      throw new SQLException("LATERAL derived tables require at least one preceding table");
    }
    var query = executionPlans.lateral(ref, collectQualifiers(priorTables));
    Map<String, Map<String, String>> columnLookup = buildCorrelatedColumnLookup(priorTables);
    List<Schema> bindingSchemas = new ArrayList<>();
    for (var reference : query.references()) {
      var resolved = resolveCorrelatedField(reference.qualifier(), reference.column(), priorTables, qualifierIndex,
          columnLookup);
      bindingSchemas.add(resolved == null ? null : resolved.field().schema());
    }
    List<String> fieldNames = new ArrayList<>();
    List<Schema> valueSchemas = new ArrayList<>();
    SubqueryTableData metadata;
    var initialBindings = new CorrelatedBindings(query.key(), Collections.nCopies(query.references().size(), null),
        bindingSchemas);
    try (initialBindings) {
      metadata = executeSubquery(query.sql(), ref, null, fieldNames, valueSchemas);
    }
    Schema schema = metadata.schema();
    JoinRecordReader.CorrelatedRowsSupplier supplier = assignments -> {
      List<Object> values = new ArrayList<>();
      for (var reference : query.references()) {
        values.add(resolveCorrelatedValue(reference.qualifier(), reference.column(), assignments, priorTables,
            qualifierIndex, columnLookup));
      }
      var bindings = new CorrelatedBindings(query.key(), values, bindingSchemas);
      try (bindings) {
        return executeSubquery(query.sql(), ref, schema, fieldNames, valueSchemas).rows();
      } catch (SQLException e) {
        throw new IllegalStateException("Failed to evaluate LATERAL subquery", e);
      }
    };
    JoinRecordReader.CorrelatedRowsSupplier effectiveSupplier = ref.tableSample() == null
        ? supplier
        : assignments -> applySampleUnchecked(ref.tableSample(), supplier.rows(assignments));
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
    return executeSubquery(sql, ref, schema, fieldNames, valueSchemas, this.cteResults);
  }

  private SubqueryTableData executeSubquery(String sql, SqlParser.TableReference ref, Schema schema,
      List<String> fieldNames, List<Schema> valueSchemas, Map<Identifier, CteResult> cteContext) throws SQLException {
    if (sql == null || sql.isBlank()) {
      throw new SQLException("Missing subquery SQL for derived table");
    }
    try (PreparedStatement subStmt = prepareSubqueryStatement(sql, cteContext); ResultSet rs = subStmt.executeQuery()) {
      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();
      List<String> columnAliases = ref == null ? List.of() : ref.columnAliases();
      if (!columnAliases.isEmpty() && columnAliases.size() != columnCount) {
        throw new SQLException("Derived table alias declares " + columnAliases.size()
            + " columns but subquery produces " + columnCount + " columns", "42000");
      }
      Schema schemaToUse = schema;
      if (schemaToUse == null) {
        fieldNames.clear();
        valueSchemas.clear();
        for (int i = 1; i <= columnCount; i++) {
          // Infer computed values before converting them to Avro; a string
          // fallback would lose their numeric type and aggregate promotion.
          if ((ref == null || !ref.lateral()) && requiresValueInference(meta, i)) {
            CteResult data = consumeResultSet(rs, derivedRecordName(ref), columnAliases);
            for (Field field : data.schema().getFields()) {
              fieldNames.add(field.name());
              valueSchemas.add(JdbcTypeMapper.nonNullSchema(field.schema()));
            }
            return new SubqueryTableData(data.schema(), List.copyOf(data.rows()));
          }
        }
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
      String context = "Failed to execute derived table '";
      if (e.getMessage() != null && e.getMessage().contains(context)) {
        throw e;
      }
      throw new SQLException(context + derivedRecordName(ref) + "': " + e.getMessage(), e.getSQLState(), e);
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
      qualifiers.addAll(table.nestedQualifiers().keySet());
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
        String normalized = normalizeColumnKey(MaterializedNames.sqlLabel(field));
        columns.putIfAbsent(normalized, field.name());
      }
      registerCorrelatedColumns(lookup, table.tableName(), columns);
      registerCorrelatedColumns(lookup, table.alias(), columns);
      for (var nested : table.nestedQualifiers().entrySet()) {
        registerCorrelatedColumns(lookup, nested.getKey(), nested.getValue());
      }
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
    var resolved = resolveCorrelatedField(qualifier, column, priorTables, qualifierIndex, columnLookup);
    if (resolved == null || assignments == null || resolved.tableIndex() >= assignments.size()) {
      return null;
    }
    GenericRecord record = assignments.get(resolved.tableIndex());
    return record == null ? null : AvroCoercions.unwrap(record.get(resolved.field().name()), resolved.field().schema());
  }

  /**
   * A source field and the preceding join input that supplies it.
   *
   * @param tableIndex
   *          position in the join assignment
   * @param field
   *          declared source field
   */
  private record CorrelatedField(int tableIndex, Field field) {
  }

  /**
   * Resolves the same source field for correlation values and their schemas.
   *
   * @param qualifier
   *          normalized outer qualifier
   * @param column
   *          outer column identifier
   * @param priorTables
   *          preceding join inputs
   * @param qualifierIndex
   *          qualifier-to-input lookup
   * @param columnLookup
   *          qualifier-to-column mappings
   * @return the resolved field and input position, or null when unavailable
   */
  private static CorrelatedField resolveCorrelatedField(String qualifier, String column,
      List<JoinRecordReader.JoinTable> priorTables, Map<String, Integer> qualifierIndex,
      Map<String, Map<String, String>> columnLookup) {
    if (column == null || qualifier == null || priorTables == null) {
      return null;
    }
    Integer tableIndex = qualifierIndex == null ? null : qualifierIndex.get(qualifier);
    if (tableIndex == null) {
      for (int i = 0; i < priorTables.size(); i++) {
        JoinRecordReader.JoinTable table = priorTables.get(i);
        if (qualifier.equals(JParqUtil.normalizeQualifier(table.alias()))
            || qualifier.equals(JParqUtil.normalizeQualifier(table.tableName()))) {
          tableIndex = i;
          break;
        }
      }
    }
    if (tableIndex == null) {
      return null;
    }
    Schema source = priorTables.get(tableIndex).schema();
    String normalizedColumn = normalizeColumnKey(column);
    String fieldName = columnLookup.getOrDefault(qualifier, Map.of()).get(normalizedColumn);
    if (fieldName == null) {
      fieldName = findFieldName(source, normalizedColumn);
    }
    Field field = fieldName == null ? null : source.getField(fieldName);
    return field == null ? null : new CorrelatedField(tableIndex, field);
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
    return Identifier.lookupKey(column);
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
    Set<String> usedNames = new LinkedHashSet<>();
    for (int i = 1; i <= columnCount; i++) {
      String alias = ref == null || ref.columnAliases().isEmpty() ? null : ref.columnAliases().get(i - 1);
      String sqlLabel = alias == null ? columnLabel(meta, i) : alias;
      String columnName = MaterializedNames.allocate(sqlLabel, usedNames);
      // Correlated runs must not inherit decimal bounds from the NULL probe.
      Schema valueSchema = ref != null && ref.lateral() && requiresValueInference(meta, i)
          ? Schema.create(Schema.Type.STRING)
          : columnSchema(meta, i);
      // Non-LATERAL unknown columns are inferred by consumeResultSet instead.
      if (ref != null && ref.lateral() && meta.getColumnType(i) == Types.OTHER) {
        Schema inferred = inferSubqueryColumnSchema(ref, i);
        if (inferred != null) {
          valueSchema = inferred;
        }
      }
      Schema union = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), valueSchema));
      Field field = new Field(columnName, union, null, Field.NULL_DEFAULT_VALUE);
      field.addProp(MaterializedNames.LOOKUP_KEY, Identifier.lookupKey(sqlLabel));
      field.addProp(MaterializedNames.SQL_LABEL, sqlLabel);
      fields.add(field);
      fieldNames.add(columnName);
      valueSchemas.add(valueSchema);
    }
    Schema schema = Schema.createRecord(MaterializedNames.recordName(recordName), null, null, false);
    schema.setFields(fields);
    return schema;
  }

  /**
   * Resolves the record name used for derived-table schemas and diagnostics.
   *
   * @param ref
   *          derived table reference, or null for the default name
   * @return alias, source table name, or {@code derived} for anonymous inputs
   */
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

  /**
   * Resolves a subquery input using retained physical or inherited CTE schemas.
   *
   * @param table
   *          local table reference
   * @return its unqualified source columns, or null when unknown
   */
  private Set<String> subqueryTableColumns(Table table) {
    return subqueryTableColumns(table, cteResults, conf);
  }

  /**
   * Resolves input columns while the owning prepared plan is being constructed.
   *
   * @param table
   *          local source
   * @param ctes
   *          CTEs available during planning
   * @param configuration
   *          physical input configuration
   * @return source column names, or null when unknown
   */
  private Set<String> subqueryTableColumns(Table table, Map<Identifier, CteResult> ctes, Configuration configuration) {
    Set<String> virtual = SubqueryExecutor.virtualColumns(table);
    if (virtual != null) {
      return virtual;
    }
    try {
      CteResult cte = resolveCteResultByName(table.getName(), ctes);
      Schema schema;
      if (cte != null) {
        schema = cte.schema();
      } else {
        var location = stmt.getConn().resolveTable(table.getSchemaName(), table.getName());
        schema = validatedTable(location, configuration == null ? HadoopConfigurations.create() : configuration)
            .schema();
      }
      return schema.getFields().stream().map(Schema.Field::name).collect(java.util.stream.Collectors.toSet());
    } catch (SQLException exception) {
      if (QueryCancellation.findCancellation(exception).isPresent()) {
        throw new IllegalStateException("Canceled while resolving subquery columns", exception);
      }
      return null;
    }
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
      JParqConnection.TableLocation location = stmt.getConn().resolveTable(ref.schemaName(), ref.tableName());
      Configuration tableConf = HadoopConfigurations.create();
      configureParquetFiltering(tableConf);
      return validatedTable(location, tableConf).schema();
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
      List<Schema> valueSchemas, List<String> columnAliases, List<List<Object>> rawRows) throws SQLException {
    int columnCount = meta.getColumnCount();
    if (columnAliases != null && !columnAliases.isEmpty() && columnAliases.size() != columnCount) {
      List<String> metaLabels = new ArrayList<>(columnCount);
      for (int i = 1; i <= columnCount; i++) {
        metaLabels.add(columnLabel(meta, i));
      }
      throw new SQLException("Number of column aliases (" + columnAliases.size()
          + ") does not match projected column count " + columnCount + " (metadata labels: " + metaLabels + ")");
    }
    List<Field> fields = new ArrayList<>(columnCount);
    Set<String> usedNames = new LinkedHashSet<>();
    for (int i = 1; i <= columnCount; i++) {
      String columnName;
      String lookupKey = null;
      if (columnAliases != null && !columnAliases.isEmpty()) {
        String alias = columnAliases.get(i - 1);
        if (alias == null || alias.isBlank()) {
          throw new SQLException("CTE column alias cannot be blank");
        }
        Identifier aliasId = Identifier.of(alias);
        if (aliasId == null) {
          throw new SQLException("Invalid CTE column alias: '" + alias + "'");
        }
        columnName = alias;
        lookupKey = aliasId.lookupKey();
      } else {
        columnName = columnLabel(meta, i);
        lookupKey = Identifier.lookupKey(columnName);
      }
      Schema valueSchema;
      if (requiresValueInference(meta, i) && (!rawRows.isEmpty() || meta.getColumnType(i) == Types.ARRAY)) {
        List<Object> values = new ArrayList<>(rawRows.size());
        for (List<Object> row : rawRows) {
          values.add(row.get(i - 1));
        }
        ValueColumnType type = switch (meta.getColumnType(i)) {
          case Types.ARRAY -> ValueColumnType.ARRAY;
          case Types.STRUCT -> ValueColumnType.STRUCT;
          case Types.OTHER -> determineColumnType(values);
          default -> ValueColumnType.DECIMAL;
        };
        valueSchema = schemaForColumnType(type, values);
      } else {
        valueSchema = columnSchema(meta, i);
      }
      Schema union = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), valueSchema));
      String sqlLabel = columnName;
      columnName = MaterializedNames.allocate(sqlLabel, usedNames);
      Field field = new Field(columnName, union, null, Field.NULL_DEFAULT_VALUE);
      field.addProp(MaterializedNames.SQL_LABEL, sqlLabel);
      if (lookupKey != null) {
        field.addProp(MaterializedNames.LOOKUP_KEY, lookupKey);
      }
      fields.add(field);
      fieldNames.add(columnName);
      valueSchemas.add(valueSchema);
    }
    String schemaName = recordName == null || recordName.isBlank() ? "derived" : recordName;
    Schema schema = Schema.createRecord(MaterializedNames.recordName(schemaName), null, null, false);
    schema.setFields(fields);
    return schema;
  }

  /**
   * Determines whether materialized values are needed to infer a column schema.
   *
   * @param meta
   *          metadata describing the column
   * @param column
   *          one-based column index
   * @return whether the JDBC type, structured schema, or decimal precision is
   *         unknown
   * @throws SQLException
   *           if column metadata cannot be accessed
   */
  private boolean requiresValueInference(ResultSetMetaData meta, int column) throws SQLException {
    int type = meta.getColumnType(column);
    return type == Types.OTHER
        || ((type == Types.ARRAY || type == Types.STRUCT) && sourceColumnSchema(meta, column) == null)
        || ((type == Types.DECIMAL || type == Types.NUMERIC) && meta.getPrecision(column) == 0);
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
    Schema source = sourceColumnSchema(meta, column);
    if (source != null && (type == Types.ARRAY || type == Types.STRUCT)) {
      return source;
    }
    return switch (type) {
      case Types.BOOLEAN, Types.BIT -> Schema.create(Schema.Type.BOOLEAN);
      case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> Schema.create(Schema.Type.INT);
      case Types.BIGINT -> Schema.create(Schema.Type.LONG);
      case Types.FLOAT, Types.REAL -> Schema.create(Schema.Type.FLOAT);
      case Types.DOUBLE -> Schema.create(Schema.Type.DOUBLE);
      case Types.NUMERIC, Types.DECIMAL -> decimalSchema(meta, column);
      case Types.DATE -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
      case Types.TIME -> LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
      case Types.TIMESTAMP -> LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> Schema.create(Schema.Type.BYTES);
      case Types.ARRAY -> inferredArraySchema(List.of());
      default -> Schema.create(Schema.Type.STRING);
    };
  }

  /**
   * Reads source schema information retained by driver metadata.
   *
   * @param meta
   *          output metadata
   * @param column
   *          one-based output index
   * @return source schema, or null for computed columns
   */
  private Schema sourceColumnSchema(ResultSetMetaData meta, int column) {
    return meta instanceof AvroColumnMetadata avro ? avro.columnValueSchema(column) : null;
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
   *           if coercion fails or a numeric conversion would lose information
   */
  private Object toAvroValue(Object value, Schema schema) throws SQLException {
    if (value == null) {
      return null;
    }
    Schema effective = JdbcTypeMapper.nonNullSchema(schema);
    LogicalType logical = effective.getLogicalType();
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
    return switch (effective.getType()) {
      case BOOLEAN -> asBoolean(value);
      case INT, LONG -> toExactInteger(value, effective.getType());
      case FLOAT -> asNumber(value).floatValue();
      case DOUBLE -> asNumber(value).doubleValue();
      case BYTES -> toBinary(value);
      case STRING -> value.toString();
      case ARRAY -> {
        List<?> elements = value instanceof Object[] array ? Arrays.asList(array) : (List<?>) value;
        List<Object> converted = new ArrayList<>(elements.size());
        for (Object element : elements) {
          converted.add(toAvroValue(element, effective.getElementType()));
        }
        yield converted;
      }
      default -> value;
    };
  }

  private String columnLabel(ResultSetMetaData meta, int column) throws SQLException {
    String label = meta instanceof AvroColumnMetadata avro ? avro.columnSqlLabel(column) : meta.getColumnLabel(column);
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

  /**
   * Converts an integral value without truncating fractions or overflowing.
   *
   * @param value
   *          the numeric value to convert
   * @param type
   *          the target INT or LONG Avro type
   * @return the exact integer value
   * @throws SQLException
   *           if the conversion would lose information
   */
  private Number toExactInteger(Object value, Schema.Type type) throws SQLException {
    try {
      if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
        Number number = (Number) value;
        if (type == Schema.Type.INT) {
          return number.intValue();
        }
        return number.longValue();
      }
      if (value instanceof Long number) {
        if (type == Schema.Type.LONG) {
          return number;
        }
        return Math.toIntExact(number);
      }
      BigDecimal number = value instanceof BigDecimal decimal ? decimal : new BigDecimal(asNumber(value).toString());
      if (type == Schema.Type.INT) {
        return number.intValueExact();
      }
      return number.longValueExact();
    } catch (ArithmeticException | NumberFormatException e) {
      throw new SQLException("Cannot convert value '" + value + "' to " + type + " without loss", "22003", e);
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

  /**
   * Encodes a decimal without rounding or exceeding the target precision.
   *
   * @param value
   *          the JDBC numeric value
   * @param decimal
   *          the target logical decimal type
   * @return the encoded unscaled decimal bytes
   * @throws SQLException
   *           if the value is not numeric, requires rounding, or exceeds the
   *           target precision
   */
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
    try {
      number = number.setScale(decimal.getScale(), java.math.RoundingMode.UNNECESSARY);
    } catch (ArithmeticException e) {
      throw new SQLException(
          "Cannot convert value '" + value + "' to decimal scale " + decimal.getScale() + " without rounding", "22003",
          e);
    }
    if (number.precision() > decimal.getPrecision()) {
      throw new SQLException("Value '" + value + "' exceeds decimal precision " + decimal.getPrecision(), "22003");
    }
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

  private void evaluateCommonTableExpressions(List<SqlParser.CommonTableExpression> ctes,
      Map<Identifier, CteResult> target) throws SQLException {
    if (ctes == null || ctes.isEmpty()) {
      return;
    }
    Set<Identifier> declared = new LinkedHashSet<>();
    for (SqlParser.CommonTableExpression cte : ctes) {
      if (cte == null) {
        continue;
      }
      Identifier key = cte.name();
      if (key == null) {
        throw new SQLException("CTE definition is missing a name");
      }
      if (!declared.add(key)) {
        throw new SQLException("Duplicate CTE name detected: " + cte.name().text());
      }
      CteResult result = evaluateCte(cte, target);
      target.put(key, result);
    }
  }

  private CteResult evaluateCte(SqlParser.CommonTableExpression cte, Map<Identifier, CteResult> available)
      throws SQLException {
    if (cte == null) {
      throw new SQLException("CTE definition cannot be null");
    }
    if (queryReferencesCte(cte.query(), cte.name())) {
      return evaluateRecursiveCte(cte, available);
    }
    return executeCteQuery(cte.sql(), available, cte.name(), cte.columnAliases());
  }

  private CteResult evaluateRecursiveCte(SqlParser.CommonTableExpression cte, Map<Identifier, CteResult> available)
      throws SQLException {
    if (!(cte.query() instanceof SqlParser.SetQuery setQuery)) {
      throw new SQLException("Recursive CTE '" + cte.name().text() + "' must use a UNION or UNION ALL set query");
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
    Identifier key = cte.name();
    boolean changed;
    do {
      changed = false;
      if (working.isEmpty()) {
        break;
      }
      Map<Identifier, CteResult> iterationContext = new LinkedHashMap<>(available);
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
          throw new SQLException("Recursive CTE '" + cte.name().text() + "' produced mismatched schemas");
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

  private boolean queryReferencesCte(SqlParser.Query query, Identifier cteName) {
    if (query == null || cteName == null) {
      return false;
    }
    if (query instanceof SqlParser.Select select) {
      List<SqlParser.TableReference> refs = select.tableReferences();
      if (refs != null) {
        for (SqlParser.TableReference ref : refs) {
          if (ref == null) {
            continue;
          }
          if (ref.commonTableExpression() != null && cteName.matches(ref.commonTableExpression().name())) {
            return true;
          }
          Identifier refTable = Identifier.of(ref.tableName());
          if (refTable != null && cteName.matches(refTable)) {
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

  private CteResult executeCteQuery(String sql, Map<Identifier, CteResult> context, Identifier tableName,
      List<String> columnAliases) throws SQLException {
    try (JParqPreparedStatement prepared = prepareSubqueryStatement(sql, context);
        ResultSet rs = prepared.executeQuery()) {
      String recordName = tableName == null ? null : tableName.text();
      return consumeResultSet(rs, recordName, columnAliases);
    }
  }

  private CteResult consumeResultSet(ResultSet rs, String tableName, List<String> columnAliases) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    List<String> fieldNames = new ArrayList<>(meta.getColumnCount());
    List<Schema> valueSchemas = new ArrayList<>(meta.getColumnCount());
    boolean inferTypes = false;
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      inferTypes |= requiresValueInference(meta, i);
    }
    List<List<Object>> rawRows = new ArrayList<>();
    Schema schema = inferTypes
        ? null
        : buildResultSchema(meta, tableName, fieldNames, valueSchemas, columnAliases, List.of());
    List<GenericRecord> rows = new ArrayList<>();
    while (rs.next()) {
      List<Object> values = new ArrayList<>(meta.getColumnCount());
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        values.add(rs.getObject(i));
      }
      if (inferTypes) {
        rawRows.add(values);
      } else {
        rows.add(materializeResultRow(values, schema, fieldNames, valueSchemas));
      }
    }
    if (inferTypes) {
      // Unknown types or decimal bounds require buffering. Release each row as it
      // is converted so the two complete row collections are never retained.
      schema = buildResultSchema(meta, tableName, fieldNames, valueSchemas, columnAliases, rawRows);
      for (int i = 0; i < rawRows.size(); i++) {
        rows.add(materializeResultRow(rawRows.set(i, null), schema, fieldNames, valueSchemas));
      }
    }
    return new CteResult(schema, rows);
  }

  /**
   * Converts one JDBC row to its materialized Avro representation.
   *
   * @param values
   *          the JDBC column values
   * @param schema
   *          the result record schema
   * @param fieldNames
   *          the output field names in projection order
   * @param valueSchemas
   *          the non-null column schemas in projection order
   * @return the converted row
   * @throws SQLException
   *           if a value cannot be converted to its output type
   */
  private GenericRecord materializeResultRow(List<Object> values, Schema schema, List<String> fieldNames,
      List<Schema> valueSchemas) throws SQLException {
    GenericData.Record record = new GenericData.Record(schema);
    for (int i = 0; i < fieldNames.size(); i++) {
      record.put(fieldNames.get(i), toAvroValue(values.get(i), valueSchemas.get(i)));
    }
    return record;
  }

  private CteResult resolveCteResult(SqlParser.CommonTableExpression cte, Map<Identifier, CteResult> available) {
    if (cte == null || available == null || available.isEmpty()) {
      return null;
    }
    return available.get(cte.name());
  }

  private CteResult resolveCteResultByName(String name, Map<Identifier, CteResult> available) {
    if (name == null || available == null || available.isEmpty()) {
      return null;
    }
    Identifier key = Identifier.of(name);
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
      Object raw = AvroCoercions.unwrap(record.get(field.name()), record.getSchema().getField(field.name()).schema());
      converted.put(field.name(), toAvroValue(raw, nonNullSchema));
    }
    return converted;
  }

  /**
   * Applies a table-reference column alias list to a materialized CTE by
   * position.
   *
   * @param result
   *          original CTE data
   * @param reference
   *          consuming table reference
   * @return data exposing only the renamed SQL columns
   * @throws SQLException
   *           if the alias count differs from the output column count
   */
  private CteResult applyColumnAliases(CteResult result, SqlParser.TableReference reference) throws SQLException {
    List<String> aliases = reference.columnAliases();
    if (aliases.isEmpty()) {
      return result;
    }
    CteResult cached = result.aliasedResults.get(aliases);
    if (cached != null) {
      return cached;
    }
    List<Field> originalFields = result.schema().getFields();
    if (aliases.size() != originalFields.size()) {
      throw SqlExceptions.syntaxFailure("Table alias column count does not match CTE output column count");
    }
    Set<String> used = new LinkedHashSet<>();
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < aliases.size(); i++) {
      Field original = originalFields.get(i);
      String alias = aliases.get(i);
      Field field = new Field(MaterializedNames.allocate(alias, used), original.schema(), original.doc(),
          original.defaultVal());
      original.getObjectProps().forEach((key, value) -> {
        if (!MaterializedNames.SQL_LABEL.equals(key) && !MaterializedNames.LOOKUP_KEY.equals(key)) {
          field.addProp(key, value);
        }
      });
      field.addProp(MaterializedNames.SQL_LABEL, alias);
      field.addProp(MaterializedNames.LOOKUP_KEY, Identifier.lookupKey(alias));
      fields.add(field);
    }
    Schema schema = Schema.createRecord(result.schema().getName(), null, null, false);
    schema.setFields(fields);
    List<GenericRecord> rows = new ArrayList<>();
    for (GenericRecord original : result.rows()) {
      GenericRecord row = new GenericData.Record(schema);
      for (int i = 0; i < fields.size(); i++) {
        row.put(i, original.get(i));
      }
      rows.add(row);
    }
    CteResult renamed = new CteResult(schema, rows);
    result.aliasedResults.put(List.copyOf(aliases), renamed);
    return renamed;
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
      AggregateFunctions.AggregatePlan aggregatePlan, Configuration configuration, Map<Identifier, CteResult> ctes)
      throws SQLException {
    if (schema == null || configuration == null || select == null) {
      return;
    }

    final Set<String> requiredColumns = validateSelectColumns(select, schema, aggregatePlan, ctes, configuration);

    if (select.hasMultipleTables()) {
      return;
    }
    List<SqlParser.TableReference> refs = select.tableReferences();
    if (refs != null) {
      boolean hasDerivedTable = refs.stream().anyMatch(ref -> ref != null && ref.isDerivedTable());
      if (hasDerivedTable) {
        return;
      }
    }

    Set<String> selectColumns = ProjectionFields.fromSelect(select);
    boolean selectAll = selectColumns == null && aggregatePlan == null;
    if (selectAll) {
      return;
    }

    if (stmt.getConn().isProjectionPushdown() && !requiredColumns.isEmpty()) {
      Schema avroProjection = AvroProjections.project(schema, requiredColumns);
      AvroReadSupport.setRequestedProjection(configuration, avroProjection);
      AvroReadSupport.setAvroReadSchema(configuration, avroProjection);
    }
  }

  /**
   * Validates references against the visible input schema independently of
   * pushdown.
   *
   * @param select
   *          query being planned
   * @param schema
   *          input schema
   * @param aggregatePlan
   *          aggregate expressions, or null
   * @param ctes
   *          available common table expressions
   * @param configuration
   *          physical schema resolution configuration, or null
   * @return required input columns
   * @throws SQLException
   *           if a referenced column is missing
   */
  private Set<String> validateSelectColumns(SqlParser.Select select, Schema schema,
      AggregateFunctions.AggregatePlan aggregatePlan, Map<Identifier, CteResult> ctes, Configuration configuration)
      throws SQLException {
    ColumnRequirement requirement = requiredColumns(select, aggregatePlan);
    collectSubqueryDependencies(select, schema, requirement.required(),
        new SubqueryExecutor(stmt.getConn(), null, table -> subqueryTableColumns(table, ctes, configuration)));
    SqlParser.TableReference baseRef = select.tableReferences().isEmpty() ? null : select.tableReferences().getFirst();
    validateRequiredColumns(schema, requirement.required(), referenceName(baseRef), select.labels(),
        select.columnNames(), requirement.referenced());
    return requirement.required();
  }

  /**
   * Retains unqualified outer dependencies before projecting the physical input.
   *
   * @param select
   *          outer query plan
   * @param schema
   *          full outer schema
   * @param required
   *          required-column accumulator
   * @param resolver
   *          scope-aware subquery schema resolver
   */
  private void collectSubqueryDependencies(SqlParser.Select select, Schema schema, Set<String> required,
      SubqueryExecutor resolver) {
    Set<String> candidates = schema.getFields().stream().map(Schema.Field::name)
        .collect(java.util.stream.Collectors.toSet());
    var baseRef = select.tableReferences().isEmpty() ? null : select.tableReferences().getFirst();
    List<String> qualifiers = qualifierList(baseRef, false);
    final var visitor = new net.sf.jsqlparser.expression.ExpressionVisitorAdapter<Void>() {
      @Override
      public <S> Void visit(net.sf.jsqlparser.expression.AnyComparisonExpression any, S context) {
        return visit(any.getSelect(), context);
      }

      @Override
      public <S> Void visit(net.sf.jsqlparser.statement.select.Select nested, S context) {
        addColumns(required, resolver.rewrite(nested, qualifiers, candidates, Map.of(), (qualifier, column) -> null)
            .correlatedColumns());
        return null;
      }

      @Override
      public <S> Void visit(net.sf.jsqlparser.statement.select.ParenthesedSelect nested, S context) {
        return visit((net.sf.jsqlparser.statement.select.Select) nested, context);
      }
    };
    List<Expression> expressions = new ArrayList<>(select.expressions());
    expressions.add(select.where());
    expressions.add(select.having());
    select.orderBy().forEach(key -> expressions.add(key.expression()));
    for (Expression expression : expressions) {
      if (expression != null) {
        expression.accept(visitor, null);
      }
    }
  }

  private record ColumnRequirement(Set<String> required, Set<String> referenced) {
  }

  private ColumnRequirement requiredColumns(SqlParser.Select select, AggregateFunctions.AggregatePlan aggregatePlan) {
    if (select == null) {
      return new ColumnRequirement(Set.of(), Set.of());
    }
    Set<String> needed = new LinkedHashSet<>();
    Set<String> referenced = new LinkedHashSet<>();
    if (!select.innerDistinctColumns().isEmpty()) {
      for (String column : select.innerDistinctColumns()) {
        if (!isOrdinalReference(column)) {
          addColumn(needed, column);
          addColumn(referenced, column);
        }
      }
    }
    Set<String> whereColumns = ColumnsUsed.inWhere(select.where());
    addColumns(needed, whereColumns);
    addColumns(referenced, whereColumns);
    SqlParser.TableReference baseRef = select.tableReferences() == null || select.tableReferences().isEmpty()
        ? null
        : select.tableReferences().getFirst();
    List<String> qualifiers = qualifierList(baseRef, false);
    List<String> primaryQualifier = qualifierList(baseRef, true);
    collectColumnReferencesFromList(select.expressions(), qualifiers, primaryQualifier, needed, referenced);
    collectColumnReferencesFromExpression(select.where(), qualifiers, primaryQualifier, needed, referenced);
    collectColumnReferencesFromExpression(select.having(), qualifiers, primaryQualifier, needed);
    for (SqlParser.OrderKey key : select.orderBy()) {
      if (key.expression() != null) {
        addColumns(needed, ColumnsUsed.inWhere(key.expression()));
        collectColumnReferencesFromExpression(key.expression(), qualifiers, primaryQualifier, needed, referenced);
      } else if (!isOrdinalReference(key.column()) && !isGroupingFunctionReference(key.column())) {
        addColumn(needed, key.column());
      }
    }
    for (SqlParser.OrderKey key : select.preOrderBy()) {
      if (key.expression() != null) {
        addColumns(needed, ColumnsUsed.inWhere(key.expression()));
        collectColumnReferencesFromExpression(key.expression(), qualifiers, primaryQualifier, needed, referenced);
      } else if (!isOrdinalReference(key.column()) && !isGroupingFunctionReference(key.column())) {
        addColumn(needed, key.column());
      }
    }
    WindowPlan windowPlan = WindowFunctions.plan(select.expressions());
    if (windowPlan != null) {
      for (Expression input : windowPlan.inputExpressions()) {
        collectColumnReferencesFromExpression(input, qualifiers, primaryQualifier, needed, referenced);
        addColumns(needed, ColumnsUsed.inWhere(input));
      }
    }
    if (aggregatePlan != null) {
      for (AggregateFunctions.AggregateSpec spec : aggregatePlan.specs()) {
        if (!spec.countStar()) {
          for (Expression arg : spec.arguments()) {
            Set<String> argColumns = ColumnsUsed.inWhere(arg);
            addColumns(needed, argColumns);
            addColumns(referenced, argColumns);
          }
        }
      }
      for (AggregateFunctions.GroupExpression groupExpr : aggregatePlan.groupExpressions()) {
        Set<String> groupColumns = ColumnsUsed.inWhere(groupExpr.expression());
        addColumns(needed, groupColumns);
        addColumns(referenced, groupColumns);
      }
    }
    return new ColumnRequirement(needed, referenced);
  }

  /**
   * Validate that the referenced columns exist in the supplied schema.
   *
   * @param schema
   *          table schema to inspect
   * @param requiredColumns
   *          columns referenced by the query
   * @param tableName
   *          logical table name used for error context
   * @param labels
   *          select-list labels available to alias references
   * @param columnNames
   *          input names corresponding to select-list labels
   * @param referencedColumns
   *          columns requiring input binding rather than alias-only binding
   * @throws SQLException
   *           if any referenced column is missing from the schema
   */
  private void validateRequiredColumns(Schema schema, Set<String> requiredColumns, String tableName,
      List<String> labels, List<String> columnNames, Set<String> referencedColumns) throws SQLException {
    if (schema == null || requiredColumns == null || requiredColumns.isEmpty()) {
      return;
    }
    Set<String> available = new LinkedHashSet<>();
    for (Schema.Field field : schema.getFields()) {
      available.add(MaterializedNames.lookupKey(field));
    }
    Set<String> normalizedLabels = new LinkedHashSet<>();
    Set<String> mappedAliases = new LinkedHashSet<>();
    if (labels != null) {
      int index = 0;
      for (String label : labels) {
        String normalized = normalizeColumnKey(label);
        if (normalized != null) {
          normalizedLabels.add(normalized);
        }
        String columnName = columnNames == null || index >= columnNames.size() ? null : columnNames.get(index);
        if (normalized != null && columnName != null) {
          String normalizedColumn = normalizeColumnKey(columnName);
          if (normalizedColumn != null && !normalized.equals(normalizedColumn)) {
            mappedAliases.add(normalized);
          }
        }
        index++;
      }
    }
    Set<String> normalizedReferences = new LinkedHashSet<>();
    if (referencedColumns != null) {
      for (String column : referencedColumns) {
        String normalized = normalizeColumnKey(column);
        if (normalized != null) {
          normalizedReferences.add(normalized);
        }
      }
    }
    Set<String> missing = new LinkedHashSet<>();
    for (String column : requiredColumns) {
      String normalized = normalizeColumnKey(column);
      if (normalized == null) {
        continue;
      }
      boolean present = containsColumn(available, normalized);
      boolean labelMatch = normalizedLabels.contains(normalized);
      boolean missingReference = !normalizedReferences.contains(normalized);
      boolean aliasOnly = labelMatch && (missingReference || mappedAliases.contains(normalized));
      if (!present && !aliasOnly) {
        missing.add(column);
      }
    }
    if (!missing.isEmpty()) {
      String qualifier = tableName == null ? "" : " for table '" + tableName + "'";
      throw SqlExceptions.syntaxFailure("Missing columns" + qualifier + ": " + String.join(", ", missing));
    }
  }

  /**
   * Matches a normalized reference using the existing quoted lowercase fallback.
   *
   * @param available
   *          visible SQL lookup keys
   * @param normalized
   *          normalized reference key
   * @return whether the name exists in the visible input
   */
  private static boolean containsColumn(Set<String> available, String normalized) {
    if (normalized == null || available.contains(normalized)) {
      return true;
    }
    String alternative = ColumnMappingUtil.alternativeColumnKey(normalized);
    return alternative != null && available.contains(alternative);
  }

  private static void addColumns(Set<String> target, Set<String> source) {
    if (target == null || source == null || source.isEmpty()) {
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

  private boolean isOrdinalReference(String column) {
    if (column == null) {
      return false;
    }
    String trimmed = column.trim();
    if (trimmed.isEmpty()) {
      return false;
    }
    for (int i = 0; i < trimmed.length(); i++) {
      if (!Character.isDigit(trimmed.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private boolean isGroupingFunctionReference(String column) {
    if (column == null) {
      return false;
    }
    return column.trim().toUpperCase(Locale.ROOT).startsWith("GROUPING(");
  }

  @Override
  public Connection getConnection() throws SQLException {
    return stmt.getConnection();
  }

  private Map<Integer, Object> snapshotParameters() {
    return new LinkedHashMap<>(parameterValues);
  }

  private void validateParameterBindings(Map<Integer, Object> parameters) throws SQLException {
    if (!hasParameters) {
      return;
    }
    for (int i = 1; i <= parameterCount; i++) {
      if (!parameters.containsKey(i)) {
        throw new SQLException("Parameter " + i + " is not set");
      }
    }
    for (Integer index : parameters.keySet()) {
      if (index < 1 || index > parameterCount) {
        throw new SQLException("Parameter index " + index + " is out of range");
      }
    }
  }

  /**
   * Executes the query with bound parameters by re-planning with actual values.
   *
   * <p>
   * This method performs the second phase of the two-phase prepared statement
   * execution:
   *
   * </p>
   * <ol>
   * <li>Bind all parameters to their values, rendering them as safe SQL
   * literals</li>
   * <li>Create a new statement with the bound SQL for full query planning</li>
   * <li>Execute the re-planned query, enabling optimizations like predicate
   * pushdown</li>
   * </ol>
   *
   * <p>
   * Re-planning at execution time is necessary because Parquet filter predicates
   * require actual values (not placeholders) to construct efficient column-level
   * filters. This approach ensures that WHERE conditions with parameters like
   * {@code WHERE age > ?} can be pushed down to Parquet's storage layer,
   * dramatically reducing I/O for filtered queries.
   * </p>
   *
   * @return the result set from executing the query with bound parameters
   * @throws SQLException
   *           if parameter binding fails or query execution fails
   */
  private JParqResultSet executeWithBoundParameters() throws SQLException {
    String boundSql = bindParameters();
    JParqResultSet currentResultSet = stmt.getCurrentRs();
    if (currentResultSet != null) {
      currentResultSet.close();
      stmt.setCurrentRs(null);
    }
    if (boundStatement != null) {
      boundStatement.close();
      boundStatement = null;
    }
    boundStatement = new JParqPreparedStatement(stmt, boundSql, inheritedCteContext);
    boundStatement.nested = nested;
    boundStatement.inheritedCancellation = inheritedCancellation;
    JParqResultSet resultSet = boundStatement.executeQuery();
    boundStatementStale = false;
    return resultSet;
  }

  /**
   * Get the Parquet pushdown plan associated with this prepared statement.
   *
   * <p>
   * For parameterized statements, execution may create a bound child statement
   * with a more specific plan after parameters are rendered to SQL literals. In
   * that case the bound statement's pushdown info is returned.
   * </p>
   *
   * @return immutable pushdown information for this statement
   */
  public PushdownInfo getPushdownInfo() {
    return boundStatement == null || boundStatementStale ? pushdownInfo : boundStatement.pushdownInfo;
  }

  /**
   * Configure Parquet reader pruning features that apply when a filter predicate
   * is attached.
   *
   * @param configuration
   *          the Hadoop configuration to update
   */
  public static void configureParquetFiltering(Configuration configuration) {
    configuration.setBoolean(ParquetInputFormat.STATS_FILTERING_ENABLED, true);
    configuration.setBoolean(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, true);
    configuration.setBoolean(ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, true);
    configuration.setBoolean(ParquetInputFormat.BLOOM_FILTERING_ENABLED, true);
  }

  /**
   * Build pushdown information for the prepared statement.
   *
   * @param planningDeferred
   *          whether pushdown planning is deferred until execution
   * @param setOperation
   *          whether the statement is a set operation
   * @param join
   *          whether the statement is a join query
   * @param cteResult
   *          the base CTE result when the query is served from memory
   * @param configuration
   *          the Parquet reader configuration, if applicable
   * @param select
   *          the parsed select statement, if applicable
   * @param predicate
   *          the optional Parquet predicate
   * @param residual
   *          the residual expression evaluated in Java
   * @return immutable pushdown information
   */
  private PushdownInfo determinePushdownInfo(boolean planningDeferred, boolean setOperation, boolean join,
      CteResult cteResult, Configuration configuration, SqlParser.Select select, Optional<FilterPredicate> predicate,
      Expression residual) {
    if (!stmt.getConn().isPredicatePushdown()) {
      return new PushdownInfo(false, residual != null, false, false, false, false,
          "Predicate pushdown disabled by connection property.", null, residual == null ? null : residual.toString());
    }
    if (planningDeferred) {
      return new PushdownInfo(false, false, false, false, false, false,
          "Pushdown planning is deferred until executeQuery() binds parameter values.", null, null);
    }
    if (setOperation) {
      return new PushdownInfo(false, false, false, false, false, false,
          "Set operations are executed without Parquet predicate pushdown.", null, null);
    }
    boolean statsEnabled = isFilterEnabled(configuration, ParquetInputFormat.STATS_FILTERING_ENABLED);
    boolean columnIndexEnabled = isFilterEnabled(configuration, ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED);
    boolean dictionaryEnabled = isFilterEnabled(configuration, ParquetInputFormat.DICTIONARY_FILTERING_ENABLED);
    boolean bloomEnabled = isFilterEnabled(configuration, ParquetInputFormat.BLOOM_FILTERING_ENABLED);
    if (join) {
      String message = residual == null
          ? "Join queries have no residual WHERE clause."
          : "Join queries evaluate residual WHERE clauses after row assembly.";
      return new PushdownInfo(!joinPredicates.isEmpty(), residual != null, statsEnabled, columnIndexEnabled,
          dictionaryEnabled, bloomEnabled, message, joinPredicates.isEmpty() ? null : joinPredicates.toString(),
          residual == null ? null : residual.toString());
    }
    if (cteResult != null) {
      return new PushdownInfo(false, residual != null, false, false, false, false,
          residual == null
              ? "In-memory query results do not use Parquet predicate pushdown."
              : "In-memory query results apply residual filtering in Java.",
          null, residual == null ? null : residual.toString());
    }
    Expression where = select == null ? null : select.where();
    if (where == null) {
      return new PushdownInfo(false, false, statsEnabled, columnIndexEnabled, dictionaryEnabled, bloomEnabled,
          "No WHERE clause was provided for Parquet predicate pushdown.", null, null);
    }
    if (select != null && select.tableReferences().stream().anyMatch(reference -> reference.tableSample() != null)) {
      return new PushdownInfo(false, residual != null, statsEnabled, columnIndexEnabled, dictionaryEnabled,
          bloomEnabled, "Predicate pushdown disabled for TABLESAMPLE; sampling precedes WHERE.", null,
          residual == null ? null : residual.toString());
    }
    if (predicate.isPresent()) {
      return new PushdownInfo(true, residual != null, statsEnabled, columnIndexEnabled, dictionaryEnabled, bloomEnabled,
          residual == null
              ? "Parquet predicate attached; statistics, page indexes, dictionaries, and bloom filters can prune rows."
              : "Parquet predicate attached; residual filtering still runs in Java.",
          predicate.get().toString(), residual == null ? null : residual.toString());
    }
    return new PushdownInfo(false, residual != null, statsEnabled, columnIndexEnabled, dictionaryEnabled, bloomEnabled,
        "WHERE clause is not fully pushdownable; applying residual filtering in Java.", null,
        residual == null ? null : residual.toString());
  }

  /**
   * Check whether a Parquet filtering option is enabled.
   *
   * @param configuration
   *          the configuration to inspect
   * @param key
   *          the configuration key
   * @return {@code true} when the key is enabled
   */
  private static boolean isFilterEnabled(Configuration configuration, String key) {
    return configuration != null && configuration.getBoolean(key, false);
  }

  /**
   * Emit a debug summary of the prepared statement's pushdown plan.
   *
   * @param info
   *          the pushdown information to log
   */
  private void logPushdownInfo(PushdownInfo info) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Pushdown plan for SQL [{}]: {}", originalSql, info.summary());
    }
  }

  private void restoreParameters(Map<Integer, Object> snapshot) {
    parameterValues.clear();
    if (snapshot != null && !snapshot.isEmpty()) {
      parameterValues.putAll(snapshot);
    }
    boundStatementStale = true;
  }

  private void storeParameter(int parameterIndex, Object value) {
    if (parameterIndex < 1) {
      return;
    }
    parameterValues.put(parameterIndex, value);
    boundStatementStale = true;
  }

  /**
   * Stores a parameter value for every positional index associated with the
   * supplied named parameter.
   *
   * @param parameterName
   *          the name of the parameter to set
   * @param value
   *          the value to associate with the named parameter
   * @throws SQLException
   *           if the parameter name is not present in the prepared SQL
   */
  private void storeNamedParameter(String parameterName, Object value) throws SQLException {
    List<Integer> indexes = indexesForName(parameterName);
    for (Integer index : indexes) {
      storeParameter(index.intValue(), value);
    }
  }

  /**
   * Resolves positional indexes for a named parameter.
   *
   * @param parameterName
   *          the parameter name to resolve
   * @return the positional indexes that reference the parameter
   * @throws SQLException
   *           if the parameter name is null, blank, or not present in the SQL
   */
  private List<Integer> indexesForName(String parameterName) throws SQLException {
    if (parameterName == null || parameterName.isBlank()) {
      throw new SQLException("Parameter name must not be null or blank");
    }
    List<Integer> indexes = namedParameterIndexes.get(parameterName);
    if (indexes == null || indexes.isEmpty()) {
      throw new SQLException("Unknown parameter name: " + parameterName);
    }
    return indexes;
  }

  /**
   * Parses the SQL for positional and named parameters, rewriting any
   * {@code :name} placeholders to JDBC {@code ?} markers.
   *
   * @param sql
   *          the SQL string to inspect
   * @return a parse result containing the rewritten SQL, the discovered named
   *         parameter indexes, and the total number of parameters
   */
  private ParameterParseResult parseParameters(String sql) {
    if (sql == null || sql.isEmpty()) {
      return new ParameterParseResult("", Map.of(), 0);
    }
    Map<String, List<Integer>> nameToIndexes = new LinkedHashMap<>();
    StringBuilder rewritten = new StringBuilder(sql.length());
    int count = 0;
    ParseState state = ParseState.NORMAL;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      boolean consumed = false;
      switch (state) {
        case NORMAL -> {
          if (c == '\'') {
            state = ParseState.SINGLE_QUOTE;
          } else if (c == '"') {
            state = ParseState.DOUBLE_QUOTE;
          } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
            state = ParseState.LINE_COMMENT;
          } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
            state = ParseState.BLOCK_COMMENT;
          } else if (c == '?') {
            count++;
          } else if (c == ':' && (i == 0 || sql.charAt(i - 1) != ':') && i + 1 < sql.length()
              && isParameterStart(sql.charAt(i + 1))) {
            int start = i + 1;
            int end = start;
            while (end < sql.length() && isParameterPart(sql.charAt(end))) {
              end++;
            }
            String name = sql.substring(start, end);
            count++;
            nameToIndexes.computeIfAbsent(name, key -> new ArrayList<>()).add(count);
            rewritten.append('?');
            i = end - 1;
            consumed = true;
          }
        }
        case SINGLE_QUOTE -> {
          if (c == '\'') {
            if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
              rewritten.append(c);
              rewritten.append(sql.charAt(i + 1));
              i++; // Skip escaped quote
              consumed = true;
            } else {
              state = ParseState.NORMAL;
            }
          }
        }
        case DOUBLE_QUOTE -> {
          if (c == '"') {
            state = ParseState.NORMAL;
          }
        }
        case LINE_COMMENT -> {
          if (c == '\n' || c == '\r') {
            state = ParseState.NORMAL;
          }
        }
        case BLOCK_COMMENT -> {
          if (c == '*' && i + 1 < sql.length() && sql.charAt(i + 1) == '/') {
            state = ParseState.NORMAL;
            rewritten.append(c);
            rewritten.append(sql.charAt(i + 1));
            i++;
            consumed = true;
          }
        }
        default -> {
        }
      }
      if (!consumed) {
        rewritten.append(c);
      }
    }
    Map<String, List<Integer>> immutableNameIndexes = new LinkedHashMap<>();
    for (Map.Entry<String, List<Integer>> entry : nameToIndexes.entrySet()) {
      immutableNameIndexes.put(entry.getKey(), List.copyOf(entry.getValue()));
    }
    return new ParameterParseResult(rewritten.toString(), Map.copyOf(immutableNameIndexes), count);
  }

  private boolean isParameterStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private boolean isParameterPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  /**
   * Captures the result of parsing a SQL string for parameters.
   *
   * @param sql
   *          the rewritten SQL with JDBC positional markers
   * @param namedParameterIndexes
   *          map of parameter names to the positional indexes they occupy
   * @param parameterCount
   *          the total number of parameters discovered in the SQL
   */
  private record ParameterParseResult(String sql, Map<String, List<Integer>> namedParameterIndexes,
      int parameterCount) {
  }

  /**
   * Immutable summary of how a prepared statement will use Parquet filtering.
   *
   * @param parquetPredicateAttached
   *          whether a Parquet filter predicate was attached to the reader
   * @param residualFilterPresent
   *          whether residual filtering still runs in Java
   * @param statisticsFilteringEnabled
   *          whether row-group statistics pruning is enabled
   * @param columnIndexFilteringEnabled
   *          whether column-index/page pruning is enabled
   * @param dictionaryFilteringEnabled
   *          whether dictionary pruning is enabled
   * @param bloomFilteringEnabled
   *          whether bloom-filter pruning is enabled
   * @param message
   *          human-readable summary of the pushdown plan
   * @param parquetPredicateSummary
   *          string representation of the Parquet predicate, if any
   * @param residualExpressionSummary
   *          string representation of the residual expression, if any
   */
  public record PushdownInfo(boolean parquetPredicateAttached, boolean residualFilterPresent,
      boolean statisticsFilteringEnabled, boolean columnIndexFilteringEnabled, boolean dictionaryFilteringEnabled,
      boolean bloomFilteringEnabled, String message, String parquetPredicateSummary, String residualExpressionSummary) {

    /**
     * Build a one-line debug summary of the pushdown plan.
     *
     * @return summary text for logging
     */
    public String summary() {
      return "predicateAttached=" + parquetPredicateAttached + ", residual=" + residualFilterPresent + ", stats="
          + statisticsFilteringEnabled + ", columnIndex=" + columnIndexFilteringEnabled + ", dictionary="
          + dictionaryFilteringEnabled + ", bloom=" + bloomFilteringEnabled + ", message=" + message
          + (parquetPredicateSummary == null ? "" : ", predicate=" + parquetPredicateSummary)
          + (residualExpressionSummary == null ? "" : ", residualExpression=" + residualExpressionSummary);
    }
  }

  private String bindParameters() throws SQLException {
    validateParameterBindings(parameterValues);
    if (parameterCount == 0) {
      return originalSql;
    }
    StringBuilder rendered = new StringBuilder(originalSql.length() + 32);
    ParseState state = ParseState.NORMAL;
    int foundCount = 0;
    for (int i = 0; i < originalSql.length(); i++) {
      char c = originalSql.charAt(i);
      boolean consumed = false;
      switch (state) {
        case NORMAL -> {
          if (c == '\'') {
            state = ParseState.SINGLE_QUOTE;
          } else if (c == '"') {
            state = ParseState.DOUBLE_QUOTE;
          } else if (c == '-' && i + 1 < originalSql.length() && originalSql.charAt(i + 1) == '-') {
            state = ParseState.LINE_COMMENT;
          } else if (c == '/' && i + 1 < originalSql.length() && originalSql.charAt(i + 1) == '*') {
            state = ParseState.BLOCK_COMMENT;
          } else if (c == '?') {
            foundCount++;
            Object value = parameterValues.get(foundCount);
            rendered.append(renderLiteral(value));
            consumed = true;
          }
        }
        case SINGLE_QUOTE -> {
          if (c == '\'') {
            // Check for escaped quote ('')
            if (i + 1 < originalSql.length() && originalSql.charAt(i + 1) == '\'') {
              // Append both quotes (escape sequence)
              rendered.append(c);
              rendered.append(originalSql.charAt(i + 1));
              i++;
              consumed = true;
            } else {
              // End of string literal
              state = ParseState.NORMAL;
            }
          }
        }
        case DOUBLE_QUOTE -> {
          if (c == '"') {
            state = ParseState.NORMAL;
          }
        }
        case LINE_COMMENT -> {
          if (c == '\n' || c == '\r') {
            state = ParseState.NORMAL;
          }
        }
        case BLOCK_COMMENT -> {
          if (c == '*' && i + 1 < originalSql.length() && originalSql.charAt(i + 1) == '/') {
            state = ParseState.NORMAL;
            rendered.append(c);
            rendered.append(originalSql.charAt(i + 1));
            i++;
            consumed = true;
          }
        }
        default -> {
        }
      }
      if (!consumed) {
        rendered.append(c);
      }
    }
    if (foundCount != parameterCount) {
      throw new SQLException("Expected " + parameterCount + " parameters but found " + foundCount);
    }
    return rendered.toString();
  }

  private String renderLiteral(Object value) throws SQLException {
    switch (value) {
      case null -> {
        return "NULL";
      }
      case String str -> {
        return "'" + escapeSingleQuotes(str) + "'";
      }
      case Character ch -> {
        return "'" + escapeSingleQuotes(String.valueOf(ch)) + "'";
      }
      case Boolean bool -> {
        return bool ? "TRUE" : "FALSE";
      }
      default -> {
      }
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      return value.toString();
    }
    switch (value) {
      case BigDecimal decimal -> {
        return decimal.toPlainString();
      }
      case Float f -> {
        if (Float.isNaN(f) || Float.isInfinite(f)) {
          throw new SQLException("Floating point parameter cannot be NaN or infinite");
        }
        return f.toString();
      }
      case Double d -> {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
          throw new SQLException("Floating point parameter cannot be NaN or infinite");
        }
        return d.toString();
      }
      case Timestamp ts -> {
        LocalDateTime ldt = ts.toLocalDateTime();
        return "'" + TIMESTAMP_FORMAT.format(ldt) + "'";
      }
      case Date date -> {
        LocalDate localDate = date.toLocalDate();
        return "DATE '" + DATE_FORMAT.format(localDate) + "'";
      }
      case Time time -> {
        LocalTime lt = time.toLocalTime();
        return "'" + TIME_FORMAT.format(lt) + "'";
      }
      case LocalDate ld -> {
        return "DATE '" + DATE_FORMAT.format(ld) + "'";
      }
      case LocalTime lt -> {
        return "'" + TIME_FORMAT.format(lt) + "'";
      }
      case LocalDateTime ldt -> {
        return "'" + TIMESTAMP_FORMAT.format(ldt) + "'";
      }
      case ByteBuffer buffer -> {
        ByteBuffer dup = buffer.duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return toHexLiteral(bytes);
      }
      case byte[] bytes -> {
        return toHexLiteral(bytes);
      }
      default -> {
      }
    }
    if (value instanceof InputStream || value instanceof Reader) {
      throw new SQLException("Stream parameters are not supported");
    }
    throw new SQLException("Unsupported parameter type: " + value.getClass().getName());
  }

  private String toHexLiteral(byte[] bytes) {
    if (bytes == null) {
      return "NULL";
    }
    StringBuilder sb = new StringBuilder(2 + bytes.length * 2);
    sb.append("X'");
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    sb.append('\'');
    return sb.toString();
  }

  /**
   * Escapes single quotes in a string for SQL literals.
   *
   * <p>
   * Precondition: {@code value} must not be {@code null}. This is guaranteed by
   * the caller (renderLiteral). If {@code value} is {@code null}, this method
   * throws {@link IllegalArgumentException}.
   *
   * @param value
   *          the string to escape (must not be null)
   * @return the escaped string
   * @throws IllegalArgumentException
   *           if value is null
   */
  private String escapeSingleQuotes(String value) {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null (guaranteed by renderLiteral)");
    }
    return value.replace("'", "''");
  }

  private enum ParseState {
    NORMAL, SINGLE_QUOTE, DOUBLE_QUOTE, LINE_COMMENT, BLOCK_COMMENT
  }

  // --- Named Parameter Setters (JParq extension) ---
  /**
   * Binds a String value to all occurrences of the specified named parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setString(String parameterName, String value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setString(int parameterIndex, String x) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds an int value to all occurrences of the specified named parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setInt(String parameterName, int value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setInt(int parameterIndex, int x) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a long value to all occurrences of the specified named parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setLong(String parameterName, long value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setLong(int parameterIndex, long x) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a double value to all occurrences of the specified named parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setDouble(String parameterName, double value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setDouble(int parameterIndex, double x) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a float value to all occurrences of the specified named parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setFloat(String parameterName, float value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setFloat(int parameterIndex, float x) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a boolean value to all occurrences of the specified named parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setBoolean(String parameterName, boolean value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a {@link BigDecimal} value to all occurrences of the specified named
   * parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setBigDecimal(String parameterName, BigDecimal value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a {@link Date} value to all occurrences of the specified named
   * parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setDate(String parameterName, Date value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setDate(int parameterIndex, Date x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a {@link Time} value to all occurrences of the specified named
   * parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setTime(String parameterName, Time value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setTime(int parameterIndex, Time x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a {@link Timestamp} value to all occurrences of the specified named
   * parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setTimestamp(String parameterName, Timestamp value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a byte array to all occurrences of the specified named parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setBytes(String parameterName, byte[] value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds a value to all occurrences of the specified named parameter.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param value
   *          the value to apply
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setObject(String parameterName, Object value) throws SQLException {
    storeNamedParameter(parameterName, value);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) {
    storeParameter(parameterIndex, x);
  }

  /**
   * Binds {@code null} to all occurrences of the specified named parameter. The
   * SQL type is accepted for API parity but not used in evaluation.
   *
   * @param parameterName
   *          the name of the parameter without the ':' prefix
   * @param sqlType
   *          the SQL type of the parameter (unused)
   * @throws SQLException
   *           if the parameter name is unknown
   */
  public void setNull(String parameterName, int sqlType) throws SQLException {
    storeNamedParameter(parameterName, null);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) {
    storeParameter(parameterIndex, null);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) {
    storeParameter(parameterIndex, null);
  }

  @Override
  public void setNString(int parameterIndex, String value) {
    storeParameter(parameterIndex, value);
  }

  @Override
  public void clearParameters() {
    parameterValues.clear();
    boundStatementStale = true;
  }

  @Override
  public void setByte(int parameterIndex, byte x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) {
    storeParameter(parameterIndex, reader);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) {
    storeParameter(parameterIndex, reader);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) {
    storeParameter(parameterIndex, reader);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) {
    storeParameter(parameterIndex, value);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) {
    storeParameter(parameterIndex, value);
  }

  @Override
  public void setRef(int parameterIndex, Ref x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) {
    storeParameter(parameterIndex, inputStream);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) {
    storeParameter(parameterIndex, inputStream);
  }

  @Override
  public void setClob(int parameterIndex, Clob x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) {
    storeParameter(parameterIndex, reader);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) {
    storeParameter(parameterIndex, reader);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) {
    storeParameter(parameterIndex, value);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) {
    storeParameter(parameterIndex, reader);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) {
    storeParameter(parameterIndex, reader);
  }

  @Override
  public void setArray(int parameterIndex, Array x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setURL(int parameterIndex, URL x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) {
    storeParameter(parameterIndex, x);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) {
    storeParameter(parameterIndex, xmlObject);
  }

  /**
   * Queues the current parameter snapshot for batch execution. As this driver is
   * read-only, each batch entry executes the prepared query with the stored
   * parameters and reports {@link Statement#SUCCESS_NO_INFO} as the update count.
   *
   */
  @Override
  public void addBatch() {
    batchedParameters.add(snapshotParameters());
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Batch operations are not supported by this read-only driver.");
  }

  /**
   * Clears all queued batch parameter sets.
   */
  @Override
  public void clearBatch() {
    batchedParameters.clear();
  }

  /**
   * Executes the queued batch entries sequentially. Each entry reuses the
   * prepared query plan and closes its result set immediately after execution.
   *
   * @return an array containing {@link Statement#SUCCESS_NO_INFO} for each
   *         successful batch entry, or an empty array when no batch entries are
   *         queued
   * @throws SQLException
   *           if a batch entry fails to execute
   */
  @Override
  public int[] executeBatch() throws SQLException {
    if (batchedParameters.isEmpty()) {
      return new int[0];
    }
    int[] updateCounts = new int[batchedParameters.size()];
    int completed = 0;
    try {
      for (Map<Integer, Object> parameters : batchedParameters) {
        restoreParameters(parameters);
        try (ResultSet rs = executeQuery()) {
          updateCounts[completed] = SUCCESS_NO_INFO;
          rs.getMetaData(); // Access to avoid unused warning; result set closes immediately after
        }
        stmt.setCurrentRs(null);
        completed++;
      }
    } catch (SQLException e) {
      int[] partial = new int[completed];
      System.arraycopy(updateCounts, 0, partial, 0, completed);
      stmt.setCurrentRs(null);
      throw new BatchUpdateException("Failed to execute batch entry " + completed, e.getSQLState(), e.getErrorCode(),
          partial, e);
    } finally {
      clearBatch();
    }
    return updateCounts;
  }

  // --- Boilerplate (Standard PreparedStatement methods) ---

  @Override
  public ResultSet getResultSet() {
    return stmt.getResultSet();
  }

  @Override
  public boolean isClosed() {
    return closed || stmt.getConn().isClosed();
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
    return new BasicParameterMetaData(parameterCount);
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
  public void cancel() throws SQLException {
    if (boundStatement != null) {
      boundStatement.cancel();
    }
    stmt.cancel();
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
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }

  private static final class BasicParameterMetaData implements ParameterMetaData {
    private final int count;

    BasicParameterMetaData(int count) {
      this.count = count;
    }

    @Override
    public int getParameterCount() {
      return count;
    }

    @Override
    public int isNullable(int param) throws SQLException {
      validateIndex(param);
      return parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
      validateIndex(param);
      return true;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
      validateIndex(param);
      return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
      validateIndex(param);
      return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
      validateIndex(param);
      return Types.VARCHAR;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
      validateIndex(param);
      return "VARCHAR";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
      validateIndex(param);
      return String.class.getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
      validateIndex(param);
      return parameterModeIn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface.isInstance(this)) {
        return iface.cast(this);
      }
      throw new SQLException("No wrapper available for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return iface.isInstance(this);
    }

    private void validateIndex(int param) throws SQLException {
      if (param < 1 || param > count) {
        throw new SQLException("Parameter index out of range: " + param + " (1.." + count + ")");
      }
    }
  }
}
