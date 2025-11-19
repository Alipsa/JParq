package se.alipsa.jparq;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.AggregateFunctions;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.ColumnsUsed;
import se.alipsa.jparq.engine.JoinRecordReader;
import se.alipsa.jparq.engine.ParquetSchemas;
import se.alipsa.jparq.engine.QueryProcessor;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.engine.window.AvgWindow;
import se.alipsa.jparq.engine.window.CountWindow;
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
import se.alipsa.jparq.engine.window.WindowState;
import se.alipsa.jparq.helper.JParqUtil;
import se.alipsa.jparq.model.ResultSetAdapter;

/** An implementation of the java.sql.ResultSet interface. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqResultSet extends ResultSetAdapter {

  private final List<String> physicalColumnOrder; // may be null
  private final QueryProcessor qp;
  private GenericRecord current;
  private final List<String> columnOrder;
  private final List<String> canonicalColumnNames;
  private final String tableName;
  private final List<Expression> selectExpressions;
  private final SubqueryExecutor subqueryExecutor;
  private final List<String> queryQualifiers;
  private ValueExpressionEvaluator projectionEvaluator;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;
  private WindowState windowState;
  private final boolean aggregateQuery;
  private final List<List<Object>> aggregateRows;
  private final List<Integer> aggregateSqlTypes;
  private int aggregateRowIndex = -1;
  private boolean aggregateOnRow = false;
  private boolean closed = false;
  private int rowNum = 0;
  private boolean lastWasNull = false;

  /**
   * Create a {@link JParqResultSet} backed by materialized rows (used for UNION
   * queries and other precomputed datasets).
   *
   * @param tableName
   *          virtual table name reported via metadata
   * @param columnLabels
   *          column labels exposed to the caller
   * @param sqlTypes
   *          SQL types corresponding to each column label
   * @param rows
   *          materialized row data
   * @return a {@link JParqResultSet} instance representing the supplied rows
   */
  static JParqResultSet materializedResult(String tableName, List<String> columnLabels, List<Integer> sqlTypes,
      List<List<Object>> rows) {
    return new JParqResultSet(tableName, columnLabels, sqlTypes, rows);
  }

  /**
   * Constructor for JParqResultSet.
   *
   * @param reader
   *          record reader to read from
   * @param select
   *          the parsed select statement
   * @param tableName
   *          the name of the table
   * @param residual
   *          the residual WHERE expression (may be null)
   * @param columnOrder
   *          the projection column labels (aliases) or null
   * @param physicalColumnOrder
   *          the physical column names (may be null)
   * @param subqueryExecutor
   *          executor used to evaluate subqueries during row materialization
   * @throws SQLException
   *           if reading fails
   */
  public JParqResultSet(RecordReader reader, SqlParser.Select select, String tableName, Expression residual,
      List<String> columnOrder, // projection labels (aliases) or null
      List<String> physicalColumnOrder, SubqueryExecutor subqueryExecutor) // physical names (may be null)
      throws SQLException {
    this.tableName = tableName;
    this.selectExpressions = List.copyOf(select.expressions());
    this.subqueryExecutor = subqueryExecutor;
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
    this.queryQualifiers = List.copyOf(qualifiers);
    Map<String, Map<String, String>> qualifierMapping = Map.of();
    Map<String, String> unqualifiedMapping = Map.of();
    if (reader instanceof JoinRecordReader joinReader) {
      qualifierMapping = joinReader.qualifierColumnMapping();
      unqualifiedMapping = joinReader.unqualifiedColumnMapping();
    }
    this.qualifierColumnMapping = qualifierMapping;
    this.unqualifiedColumnMapping = unqualifiedMapping;
    Set<String> availableQualifiers = new LinkedHashSet<>(this.qualifierColumnMapping.keySet());
    if (availableQualifiers.isEmpty() && !this.queryQualifiers.isEmpty()) {
      for (String qualifier : this.queryQualifiers) {
        String normalized = JParqUtil.normalizeQualifier(qualifier);
        if (normalized != null && !normalized.isEmpty()) {
          availableQualifiers.add(normalized);
        }
      }
    }
    Expression effectiveResidual = pruneUnavailableQualifiers(residual, availableQualifiers);
    List<String> labels = (columnOrder != null ? new ArrayList<>(columnOrder) : new ArrayList<>());
    List<String> canonicalPhysical = canonicalizeProjection(select, physicalColumnOrder, qualifierMapping,
        unqualifiedMapping);
    List<String> canonicalLookup = canonicalPhysical == null ? null : List.copyOf(canonicalPhysical);
    List<String> requestedColumns = canonicalLookup != null ? canonicalLookup : select.columns();
    List<String> physical = physicalColumnOrder == null ? null : List.copyOf(physicalColumnOrder);

    AggregateFunctions.AggregatePlan aggregatePlan = AggregateFunctions.plan(select);
    if (aggregatePlan != null) {
      labels = new ArrayList<>(aggregatePlan.labels());
      physical = null;
      canonicalLookup = null;
      try {
        AggregateFunctions.AggregateResult result = AggregateFunctions.evaluate(reader, aggregatePlan,
            effectiveResidual, select.having(), select.orderBy(), subqueryExecutor, queryQualifiers,
            qualifierColumnMapping, unqualifiedColumnMapping);
        this.aggregateRows = new ArrayList<>(result.rows());
        this.aggregateSqlTypes = result.sqlTypes();
      } catch (Exception e) {
        throw new SQLException("Failed to compute aggregate query", e);
      }
      this.columnOrder = labels;
      this.physicalColumnOrder = physical;
      this.canonicalColumnNames = canonicalLookup;
      this.aggregateQuery = true;
      this.aggregateOnRow = false;
      this.aggregateRowIndex = -1;
      this.qp = null;
      this.current = null;
      this.rowNum = 0;
      this.windowState = WindowState.empty();
      return;
    }

    this.columnOrder = labels;
    this.physicalColumnOrder = physical;
    this.canonicalColumnNames = canonicalLookup;
    this.aggregateQuery = false;
    this.aggregateRows = null;
    this.aggregateSqlTypes = null;
    this.windowState = WindowState.empty();

    WindowPlan windowPlan = WindowFunctions.plan(selectExpressions);
    Map<String, Expression> orderByExpressions = extractOrderByExpressions(select, physicalColumnOrder);

    try {
      GenericRecord first = reader.read();
      if (first == null) {
        // No rows emitted after pushdown; still build metadata if explicit projection
        List<String> req = select.columns(); // e.g., ["id","value"] or ["*"]
        if (this.columnOrder.isEmpty() && !req.isEmpty() && !req.contains("*")) {
          this.columnOrder.addAll(req); // mutable, safe
        }
        List<String> distinctProjection = resolveDistinctColumns(select);
        QueryProcessor.Options options = QueryProcessor.Options.builder().distinct(select.distinct())
            .distinctColumns(distinctProjection).distinctBeforePreLimit(select.innerDistinct())
            .subqueryExecutor(subqueryExecutor).preLimit(select.preLimit()).preOrderBy(select.preOrderBy())
            .outerQualifiers(queryQualifiers).qualifierColumnMapping(qualifierColumnMapping)
            .unqualifiedColumnMapping(unqualifiedColumnMapping).preStageDistinctColumns(select.innerDistinctColumns())
            .offset(select.offset()).preOffset(select.preOffset()).windowPlan(windowPlan)
            .orderByExpressions(orderByExpressions);
        List<String> projectionColumns = requestedColumns;
        if (projectionColumns == null || projectionColumns.isEmpty()) {
          projectionColumns = select.columns();
        }
        this.qp = new QueryProcessor(reader, projectionColumns, /* where */ effectiveResidual, select.limit(), options);
        this.current = null;
        this.rowNum = 0;
        this.windowState = qp.windowState();
        return;
      }

      var schema = first.getSchema();
      var evaluator = new se.alipsa.jparq.engine.ExpressionEvaluator(schema, subqueryExecutor, queryQualifiers,
          qualifierColumnMapping, unqualifiedColumnMapping);
      final boolean match = effectiveResidual == null || evaluator.eval(effectiveResidual, first);

      // Compute physical projection from schema; only add if we don’t already have
      // labels
      List<String> proj = QueryProcessor.computeProjection(requestedColumns, schema);
      Set<String> requiredColumns = new LinkedHashSet<>(proj);
      requiredColumns.addAll(ColumnsUsed.inWhere(effectiveResidual));
      requiredColumns.addAll(ColumnsUsed.inWhere(select.having()));
      requiredColumns.addAll(SqlParser.collectQualifiedColumns(effectiveResidual, queryQualifiers));
      requiredColumns.addAll(SqlParser.collectQualifiedColumns(select.having(), queryQualifiers));
      for (Expression expression : selectExpressions) {
        requiredColumns.addAll(SqlParser.collectQualifiedColumns(expression, queryQualifiers));
      }
      if (windowPlan != null && !windowPlan.isEmpty()) {
        for (RowNumberWindow window : windowPlan.rowNumberWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
        }
        for (RankWindow window : windowPlan.rankWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
        }
        for (DenseRankWindow window : windowPlan.denseRankWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
        }
        for (PercentRankWindow window : windowPlan.percentRankWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
        }
        for (CumeDistWindow window : windowPlan.cumeDistWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
        }
        for (NtileWindow window : windowPlan.ntileWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
          Expression bucketExpression = window.bucketExpression();
          if (bucketExpression != null) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(bucketExpression, queryQualifiers));
          }
        }
        for (CountWindow window : windowPlan.countWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
          Expression argument = window.argument();
          if (argument != null) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(argument, queryQualifiers));
          }
        }
        for (SumWindow window : windowPlan.sumWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
          Expression argument = window.argument();
          if (argument != null) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(argument, queryQualifiers));
          }
        }
        for (AvgWindow window : windowPlan.avgWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
          Expression argument = window.argument();
          if (argument != null) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(argument, queryQualifiers));
          }
        }
        for (MaxWindow window : windowPlan.maxWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
          Expression argument = window.argument();
          if (argument != null) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(argument, queryQualifiers));
          }
        }
        for (MinWindow window : windowPlan.minWindows()) {
          for (Expression partition : window.partitionExpressions()) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(partition, queryQualifiers));
          }
          for (OrderByElement order : window.orderByElements()) {
            if (order != null && order.getExpression() != null) {
              requiredColumns.addAll(SqlParser.collectQualifiedColumns(order.getExpression(), queryQualifiers));
            }
          }
          Expression argument = window.argument();
          if (argument != null) {
            requiredColumns.addAll(SqlParser.collectQualifiedColumns(argument, queryQualifiers));
          }
        }
      }
      proj = new ArrayList<>(requiredColumns);
      if (this.columnOrder.isEmpty()) {
        this.columnOrder.addAll(proj); // keep mutable
      }

      var order = select.orderBy();
      boolean usePrefetchedAsCurrent = match && select.offset() == 0;
      if (order == null || order.isEmpty()) {
        int initialEmitted = usePrefetchedAsCurrent ? 1 : 0;
        List<String> distinctProjection = resolveDistinctColumns(select);
        QueryProcessor.Options options = QueryProcessor.Options.builder().schema(schema).initialEmitted(initialEmitted)
            .distinct(select.distinct()).distinctColumns(distinctProjection)
            .distinctBeforePreLimit(select.innerDistinct()).firstAlreadyRead(first).subqueryExecutor(subqueryExecutor)
            .preLimit(select.preLimit()).preOrderBy(select.preOrderBy())
            .preStageDistinctColumns(select.innerDistinctColumns()).outerQualifiers(queryQualifiers)
            .qualifierColumnMapping(qualifierColumnMapping).unqualifiedColumnMapping(unqualifiedColumnMapping)
            .offset(select.offset()).preOffset(select.preOffset()).windowPlan(windowPlan)
            .orderByExpressions(orderByExpressions);
        this.qp = new QueryProcessor(reader, proj, effectiveResidual, select.limit(), options);
        this.current = usePrefetchedAsCurrent ? first : qp.nextMatching();
        this.windowState = qp.windowState();
      } else {
        List<String> distinctProjection = resolveDistinctColumns(select);
        QueryProcessor.Options options = QueryProcessor.Options.builder().schema(schema).distinct(select.distinct())
            .distinctColumns(distinctProjection).distinctBeforePreLimit(select.innerDistinct()).orderBy(order)
            .firstAlreadyRead(first).subqueryExecutor(subqueryExecutor).preLimit(select.preLimit())
            .preOrderBy(select.preOrderBy()).preStageDistinctColumns(select.innerDistinctColumns())
            .outerQualifiers(queryQualifiers).qualifierColumnMapping(qualifierColumnMapping)
            .unqualifiedColumnMapping(unqualifiedColumnMapping).offset(select.offset()).preOffset(select.preOffset())
            .windowPlan(windowPlan).orderByExpressions(orderByExpressions);
        this.qp = new QueryProcessor(reader, proj, effectiveResidual, select.limit(), options);
        this.current = qp.nextMatching();
        this.windowState = qp.windowState();
      }
      this.rowNum = 0;
    } catch (Exception e) {
      throw new SQLException("Failed reading first parquet record", e);
    }
  }

  /**
   * Create a {@link JParqResultSet} from precomputed rows.
   *
   * @param tableName
   *          virtual table name to expose through metadata
   * @param columnLabels
   *          column labels returned to callers
   * @param sqlTypes
   *          SQL types corresponding to {@code columnLabels}
   * @param rows
   *          materialized row data backing the result set
   */
  private JParqResultSet(String tableName, List<String> columnLabels, List<Integer> sqlTypes, List<List<Object>> rows) {
    this.tableName = tableName;
    this.selectExpressions = List.of();
    this.subqueryExecutor = null;
    this.queryQualifiers = List.of();
    this.qualifierColumnMapping = Map.of();
    this.unqualifiedColumnMapping = Map.of();
    this.columnOrder = new ArrayList<>(columnLabels);
    this.canonicalColumnNames = null;
    this.physicalColumnOrder = null;
    this.aggregateQuery = true;
    this.aggregateRows = rows == null ? new ArrayList<>() : new ArrayList<>(rows);
    this.aggregateSqlTypes = sqlTypes == null ? List.of() : List.copyOf(sqlTypes);
    this.aggregateRowIndex = -1;
    this.aggregateOnRow = false;
    this.qp = null;
    this.current = null;
    this.projectionEvaluator = null;
    this.closed = false;
    this.rowNum = 0;
    this.lastWasNull = false;
    this.windowState = WindowState.empty();
  }

  @Override
  public boolean next() throws SQLException {
    if (closed) {
      throw new SQLException("ResultSet closed");
    }
    if (aggregateQuery) {
      if (aggregateRows == null || aggregateRows.isEmpty()) {
        aggregateOnRow = false;
        return false;
      }
      if (aggregateRowIndex + 1 >= aggregateRows.size()) {
        aggregateOnRow = false;
        return false;
      }
      aggregateRowIndex++;
      aggregateOnRow = true;
      rowNum = aggregateRowIndex + 1;
      return true;
    }
    try {
      if (rowNum == 0) {
        if (current == null) {
          return false;
        }
        rowNum = 1;
        return true;
      }
      current = qp.nextMatching();
      if (current == null) {
        return false;
      }
      rowNum++;
      return true;
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  private Object value(int idx) throws SQLException {
    if (aggregateQuery) {
      if (!aggregateOnRow || aggregateRowIndex < 0) {
        throw new SQLException("Call next() before getting values");
      }
      List<List<Object>> rows = aggregateRows == null ? List.of() : aggregateRows;
      if (rows.isEmpty() || aggregateRowIndex >= rows.size()) {
        throw new SQLException("No aggregate rows available");
      }
      List<Object> row = rows.get(aggregateRowIndex);
      if (idx < 1 || idx > row.size()) {
        throw new SQLException("Unknown column index: " + idx);
      }
      Object v = row.get(idx - 1);
      lastWasNull = (v == null);
      return v;
    }
    if (current == null) {
      throw new SQLException("Call next() before getting values");
    }

    Expression projectionExpr = projectionExpression(idx);
    if (projectionExpr != null && !(projectionExpr instanceof Column)) {
      ensureProjectionEvaluator(current);
      Object computed = projectionEvaluator == null ? null : projectionEvaluator.eval(projectionExpr, current);
      lastWasNull = (computed == null);
      return computed;
    }

    // projection name (may be an alias/label)
    String projectedName = columnOrder.get(idx - 1);

    // Try alias (label) directly first (covers engines that rewrap records by
    // label)
    String lookupName = projectedName;
    var field = current.getSchema().getField(lookupName);

    // If not found, fall back to the canonical column name used for schema lookups
    if (field == null) {
      String canonical = canonicalColumnName(idx);
      if (canonical != null && !canonical.equals(lookupName)) {
        lookupName = canonical;
        field = current.getSchema().getField(lookupName);
      }
    }

    if (field == null) {
      if (projectionExpr instanceof Column columnExpr) {
        ensureProjectionEvaluator(current);
        Object computed = projectionEvaluator == null ? null : projectionEvaluator.eval(columnExpr, current);
        lastWasNull = (computed == null);
        return computed;
      }
      throw new SQLException("Unknown column in current schema: " + projectedName);
    }

    Object rawValue = current.get(lookupName);
    Object raw = AvroCoercions.unwrap(rawValue, field.schema());
    lastWasNull = (raw == null);
    return raw;
  }

  private Expression projectionExpression(int idx) {
    if (selectExpressions.isEmpty()) {
      return null;
    }
    int i = idx - 1;
    if (i >= 0 && i < selectExpressions.size()) {
      return selectExpressions.get(i);
    }
    return null;
  }

  /**
   * Translate projection column names to the canonical field names emitted by an
   * {@link JoinRecordReader} when joining multiple tables.
   *
   * @param select
   *          parsed SELECT statement supplying the projection expressions
   * @param physicalColumnOrder
   *          physical column names recorded during parsing (may be {@code null}
   *          or empty)
   * @param qualifierMapping
   *          mapping from qualifier to canonical column names provided by the
   *          join reader
   * @param unqualifiedMapping
   *          mapping for unqualified column references that remain unique across
   *          the join
   * @return a list containing canonical column names when available; otherwise
   *         the original {@code physicalColumnOrder}
   */
  private static List<String> canonicalizeProjection(SqlParser.Select select, List<String> physicalColumnOrder,
      Map<String, Map<String, String>> qualifierMapping, Map<String, String> unqualifiedMapping) {
    if (physicalColumnOrder == null || physicalColumnOrder.isEmpty()) {
      return physicalColumnOrder;
    }
    if ((qualifierMapping == null || qualifierMapping.isEmpty())
        && (unqualifiedMapping == null || unqualifiedMapping.isEmpty())) {
      return physicalColumnOrder;
    }
    List<String> canonical = new ArrayList<>(physicalColumnOrder);
    List<Expression> expressions = select.expressions();
    List<String> labels = select.labels();
    int expressionCount = expressions == null ? 0 : expressions.size();
    int limit = Math.min(canonical.size(), expressionCount);
    for (int i = 0; i < limit; i++) {
      Expression expr = expressions.get(i);
      if (expr instanceof Column column) {
        canonical.set(i, canonicalColumnName(column, canonical.get(i), qualifierMapping, unqualifiedMapping));
      }
      if (canonical.get(i) == null) {
        String fallback = null;
        if (labels != null && i < labels.size()) {
          fallback = labels.get(i);
        }
        if (fallback == null && expr != null) {
          fallback = expr.toString();
        }
        canonical.set(i, fallback);
      }
    }
    for (int i = 0; i < canonical.size(); i++) {
      if (canonical.get(i) == null && labels != null && i < labels.size()) {
        canonical.set(i, labels.get(i));
      }
      if (canonical.get(i) == null) {
        canonical.set(i, "column_" + i);
      }
    }
    return Collections.unmodifiableList(canonical);
  }

  /**
   * Resolve the canonical column name for a single projection entry.
   *
   * @param column
   *          the column expression containing optional qualifier metadata
   * @param currentValue
   *          the existing physical column name discovered during parsing
   * @param qualifierMapping
   *          mapping from qualifier to canonical column names
   * @param unqualifiedMapping
   *          mapping for columns that remain unique without a qualifier
   * @return the canonical column name if available, otherwise the best known
   *         fallback
   */
  private static String canonicalColumnName(Column column, String currentValue,
      Map<String, Map<String, String>> qualifierMapping, Map<String, String> unqualifiedMapping) {
    String qualifier = resolveQualifier(column);
    String columnName = column.getColumnName();
    if (qualifier != null && qualifierMapping != null) {
      Map<String, String> mapping = qualifierMapping.get(qualifier.toLowerCase(Locale.ROOT));
      if (mapping != null) {
        String canonical = mapping.get(columnName.toLowerCase(Locale.ROOT));
        if (canonical != null) {
          return canonical;
        }
      }
    }
    if (unqualifiedMapping != null) {
      String canonical = unqualifiedMapping.get(columnName.toLowerCase(Locale.ROOT));
      if (canonical != null) {
        return canonical;
      }
    }
    if (currentValue != null) {
      return currentValue;
    }
    return columnName;
  }

  /**
   * Resolve the canonical column name used internally for schema lookups.
   *
   * @param index
   *          1-based column index requested by the caller
   * @return the canonical column name when known, otherwise {@code null}
   */
  private String canonicalColumnName(int index) {
    return ColumnNameLookup.canonicalName(canonicalColumnNames, physicalColumnOrder, columnOrder, index);
  }

  /**
   * Determine the columns participating in DISTINCT evaluation for the supplied
   * select statement.
   *
   * @param select
   *          select statement to inspect
   * @return list of column names or {@code null} when DISTINCT should consider
   *         the full projection
   */
  private static List<String> resolveDistinctColumns(SqlParser.Select select) {
    if (select == null) {
      return null;
    }
    List<String> columns = select.columnNames();
    if (columns == null || columns.isEmpty()) {
      return null;
    }
    if (columns.size() == 1 && "*".equals(columns.getFirst())) {
      return null;
    }
    return columns;
  }

  /**
   * Extract expressions from the SELECT list that may be referenced by ORDER BY
   * aliases.
   *
   * <p>
   * All projection expressions are captured so aliases that simply rename a
   * column are treated the same way as aliases referencing computed expressions.
   * This allows ORDER BY clauses to consistently evaluate aliases even when the
   * underlying column name changes after canonicalization (e.g. within joins).
   * The associated physical column name is also registered to provide a fallback
   * for parser stages that rewrite the ORDER BY reference to the original column.
   * </p>
   *
   * @param select
   *          select statement providing projection labels and expressions
   * @param physicalColumnOrder
   *          physical column names corresponding to the projection entries;
   *          {@code null} when unavailable
   * @return mapping of projection label to the corresponding expression; empty
   *         when no computed expressions are present
   */
  private static Map<String, Expression> extractOrderByExpressions(SqlParser.Select select,
      List<String> physicalColumnOrder) {
    if (select == null || select.expressions() == null || select.expressions().isEmpty()) {
      return Map.of();
    }
    List<Expression> expressions = select.expressions();
    List<String> labels = select.labels();
    int size = Math.min(expressions.size(), labels.size());
    Map<String, Expression> mapping = new HashMap<>();
    for (int i = 0; i < size; i++) {
      Expression expression = expressions.get(i);
      String label = labels.get(i);
      final String physical = getPhysicalColumnName(physicalColumnOrder, i);
      if (expression == null) {
        continue;
      }
      if (label == null || label.isBlank()) {
        label = null;
      }
      if (label != null) {
        mapping.putIfAbsent(label, expression);
        mapping.putIfAbsent(label.toLowerCase(Locale.ROOT), expression);
      }
      if (physical != null && !physical.isBlank()) {
        mapping.putIfAbsent(physical, expression);
        mapping.putIfAbsent(physical.toLowerCase(Locale.ROOT), expression);
      }
    }
    return mapping.isEmpty() ? Map.of() : Map.copyOf(mapping);
  }

  /**
   * Retrieves the physical column name at the specified index from the order
   * list.
   *
   * @param order
   *          list of physical column names; may be {@code null}
   * @param index
   *          the zero-based index of the column to retrieve
   * @return the physical column name at the given index, or {@code null} if the
   *         order list is {@code null}, empty, or the index is out of bounds
   */
  private static String getPhysicalColumnName(List<String> order, int index) {
    if (order == null || order.isEmpty() || index >= order.size()) {
      return null;
    }
    return order.get(index);
  }

  /**
   * Remove predicates that reference qualifiers not available to the current
   * query scope.
   *
   * @param expression
   *          the predicate expression to prune (may be {@code null})
   * @param availableQualifiers
   *          qualifiers that remain valid for the current reader
   * @return the pruned expression or {@code null} if no predicates remain
   */
  private static Expression pruneUnavailableQualifiers(Expression expression, Set<String> availableQualifiers) {
    if (expression == null) {
      return null;
    }
    Set<String> qualifiers = (availableQualifiers == null) ? Set.of() : availableQualifiers;
    return pruneExpression(expression, qualifiers);
  }

  /**
   * Recursively prune predicates referencing unavailable qualifiers while
   * preserving the structure of AND/parenthesized expressions when possible.
   *
   * @param expression
   *          expression to inspect
   * @param availableQualifiers
   *          normalized qualifiers that remain accessible
   * @return pruned expression or {@code null} when the predicate cannot be
   *         satisfied
   */
  private static Expression pruneExpression(Expression expression, Set<String> availableQualifiers) {
    if (expression == null) {
      return null;
    }
    if (expression instanceof AndExpression andExpression) {
      Expression left = pruneExpression(andExpression.getLeftExpression(), availableQualifiers);
      Expression right = pruneExpression(andExpression.getRightExpression(), availableQualifiers);
      if (left == null) {
        return right;
      }
      if (right == null) {
        return left;
      }
      andExpression.setLeftExpression(left);
      andExpression.setRightExpression(right);
      return andExpression;
    }
    if (expression instanceof Parenthesis parenthesis) {
      Expression inner = pruneExpression(parenthesis.getExpression(), availableQualifiers);
      if (inner == null) {
        return null;
      }
      parenthesis.setExpression(inner);
      return parenthesis;
    }
    Set<String> qualifiersInExpression = collectQualifiers(expression);
    if (qualifiersInExpression.isEmpty()) {
      return expression;
    }
    for (String qualifier : qualifiersInExpression) {
      if (!availableQualifiers.contains(qualifier)) {
        return null;
      }
    }
    return expression;
  }

  /**
   * Collect qualifiers referenced by the supplied expression.
   *
   * @param expression
   *          expression to analyse
   * @return set of normalized qualifiers referenced within the expression
   */
  private static Set<String> collectQualifiers(Expression expression) {
    if (expression == null) {
      return Set.of();
    }
    Set<String> qualifiers = new LinkedHashSet<>();
    expression.accept(new ExpressionVisitorAdapter<Void>() {
      @Override
      public <S> Void visit(Column column, S context) {
        String qualifier = resolveQualifier(column);
        if (qualifier != null) {
          String normalized = JParqUtil.normalizeQualifier(qualifier);
          if (normalized != null) {
            qualifiers.add(normalized);
          }
        }
        return super.visit(column, context);
      }
    });
    return qualifiers;
  }

  /**
   * Extract the effective qualifier for a {@link Column}, preferring an explicit
   * alias over the table name when available.
   *
   * @param column
   *          column expression to inspect
   * @return the qualifier string or {@code null} if none exists
   */
  private static String resolveQualifier(Column column) {
    Table table = column.getTable();
    if (table == null) {
      return null;
    }
    if (table.getAlias() != null && table.getAlias().getName() != null && !table.getAlias().getName().isBlank()) {
      return table.getAlias().getName();
    }
    if (table.getName() != null && !table.getName().isBlank()) {
      return table.getName();
    }
    return null;
  }

  private void ensureProjectionEvaluator(GenericRecord record) {
    if (projectionEvaluator == null && record != null && !selectExpressions.isEmpty()) {
      projectionEvaluator = new ValueExpressionEvaluator(record.getSchema(), subqueryExecutor, queryQualifiers,
          qualifierColumnMapping, unqualifiedColumnMapping, windowState);
    }
  }

  @Override
  public int findColumn(String label) throws SQLException {
    for (int i = 0; i < columnOrder.size(); i++) {
      if (columnOrder.get(i).equals(label)) {
        return i + 1;
      }
    }
    throw new SQLException("Unknown column: " + label);
  }

  @Override
  public ResultSetMetaData getMetaData() {
    if (aggregateQuery) {
      List<Integer> types = aggregateSqlTypes == null ? List.of() : aggregateSqlTypes;
      return new AggregateResultSetMetaData(columnOrder, types, tableName);
    }
    var schema = (current == null) ? null : current.getSchema();
    var normalized = ParquetSchemas.normalizeStringTypes(schema);
    return new JParqResultSetMetaData(normalized, columnOrder, physicalColumnOrder, canonicalColumnNames, tableName,
        selectExpressions);
  }

  @SuppressWarnings("PMD.EmptyCatchBlock")
  @Override
  public void close() throws SQLException {
    closed = true;
    try {
      if (qp != null) {
        qp.close();
      }
    } catch (Exception ignore) {
      // intentionally ignored
    }
  }

  @Override
  public boolean wasNull() {
    return lastWasNull;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    if (v == null) {
      return null;
    }
    if (v instanceof byte[] bytes) {
      return new String(bytes, StandardCharsets.UTF_8);
    }
    if (v instanceof ByteBuffer buffer) {
      ByteBuffer dup = buffer.duplicate();
      byte[] bytes = new byte[dup.remaining()];
      dup.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
    return v.toString();
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v != null && ((Boolean) v);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0 : ((Number) v).byteValue();
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0 : ((Number) v).shortValue();
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0 : ((Number) v).intValue();
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0 : ((Number) v).longValue();
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0f : ((Number) v).floatValue();
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return v == null ? 0d : ((Number) v).doubleValue();
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    if (v == null) {
      return null;
    }
    if (v instanceof BigDecimal bd) {
      return bd;
    }
    if (v instanceof Number n) {
      return new BigDecimal(n.toString());
    }
    return new BigDecimal(v.toString());
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    return (byte[]) v;
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    if (v == null) {
      return null;
    }
    if (v instanceof Date) {
      return (Date) v;
    }
    if (v instanceof Timestamp ts) {
      return new Date(ts.getTime());
    }
    if (v instanceof String s) {
      return Date.valueOf(s);
    }
    if (v instanceof Long l) {
      return new Date(l);
    }
    if (v instanceof Double d) {
      return new Date(d.longValue());
    }
    if (v instanceof LocalDateTime l) {
      return Date.valueOf(l.toLocalDate());
    }
    if (v instanceof LocalDate) {
      return Date.valueOf((LocalDate) v);
    }
    throw new SQLException("Unsupported date type: " + v.getClass().getName());
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    if (v == null) {
      return null;
    }
    if (v instanceof Timestamp) {
      return new Time(((Timestamp) v).getTime());
    }
    if (v instanceof Time) {
      return (Time) v;
    }
    if (v instanceof LocalTime lt) {
      return Time.valueOf(lt);
    }
    if (v instanceof String s) {
      return Time.valueOf(s);
    }
    if (v instanceof Long l) {
      return new Time(l);
    }
    if (v instanceof Double d) {
      return new Time(d.longValue());
    }
    throw new SQLException("Unsupported time type: " + v.getClass().getName());
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Object v = value(columnIndex);
    if (v == null) {
      return null;
    }
    if (v instanceof Timestamp t) {
      return t;
    }
    if (v instanceof Date d) {
      return new Timestamp(d.getTime());
    }
    if (v instanceof String s) {
      return Timestamp.valueOf(s);
    }
    if (v instanceof Long l) {
      return new Timestamp(l);
    }
    if (v instanceof Double d) {
      return new Timestamp(d.longValue());
    }
    if (v instanceof LocalDateTime l) {
      return Timestamp.valueOf(l);
    }
    throw new SQLException("Unsupported timestamp type: " + v.getClass().getName());
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return value(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public boolean isBeforeFirst() {
    return rowNum == 0;
  }

  @Override
  public boolean isFirst() {
    return rowNum == 1;
  }

  @Override
  public int getRow() {
    return rowNum;
  }

  @Override
  public int getFetchDirection() {
    return FETCH_FORWARD;
  }

  @Override
  public int getType() {
    return TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() {
    return CONCUR_READ_ONLY;
  }
}
