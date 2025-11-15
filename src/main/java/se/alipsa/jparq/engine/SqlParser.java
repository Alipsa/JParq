package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.parser.Token;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import se.alipsa.jparq.helper.JParqUtil;

/**
 * Handles parsing the SQL and translates that into something that the parquet
 * reader can understand.
 */
public final class SqlParser {
  private SqlParser() {
  }

  /**
   * Representation of a parsed SQL query supported by the engine.
   */
  public sealed interface Query permits Select, SetQuery {

    /**
     * The common table expressions that are visible to the query.
     *
     * @return an immutable list of {@link CommonTableExpression} definitions
     */
    List<CommonTableExpression> commonTableExpressions();
  }

  /**
   * ORDER BY key (simple: column + direction and optional qualifier).
   *
   * @param column
   *          the column name (unqualified physical column name)
   * @param asc
   *          true if ascending order, false if descending
   * @param qualifier
   *          optional table or alias qualifier associated with the column
   */
  public record OrderKey(String column, boolean asc, String qualifier) {
  }

  /**
   * A simple representation of a SELECT statement.
   *
   * @param labels
   *          display labels in projection order (alias if present, else column
   *          name)
   * @param columnNames
   *          physical column names in projection order (null if not a simple
   *          column)
   * @param table
   *          table name
   * @param tableAlias
   *          optional table alias (null if none)
   * @param where
   *          the where expression (with qualifiers stripped for single-table
   *          queries)
   * @param limit
   *          the limit value (-1 if none)
   * @param offset
   *          number of rows to skip after filtering (0 if not specified)
   * @param orderBy
   *          ORDER BY keys (empty if none)
   * @param distinct
   *          true if DISTINCT is specified in this SELECT or inherited from an
   *          inner SELECT
   * @param innerDistinct
   *          true if DISTINCT originates from an inner SELECT
   * @param innerDistinctColumns
   *          physical column names describing the DISTINCT key of the inner
   *          SELECT (empty if not applicable)
   * @param expressions
   *          the normalized SELECT expressions in projection order
   * @param expressionOrder
   *          zero-based indexes describing the order each expression appeared in
   *          the original SELECT list
   * @param qualifiedWildcards
   *          qualified wildcard entries encountered in the SELECT list (empty if
   *          none)
   * @param groupByExpressions
   *          expressions that participate in the GROUP BY clause (empty if no
   *          grouping)
   * @param groupingSets
   *          grouping set definitions referencing group expressions by index
   * @param having
   *          the HAVING expression (may be {@code null})
   * @param preLimit
   *          a limit that must be applied before outer ORDER BY logic (typically
   *          sourced from an inner SELECT)
   * @param preOffset
   *          number of rows to skip prior to enforcing pre-stage ORDER BY,
   *          DISTINCT, and LIMIT operations
   * @param preOrderBy
   *          ORDER BY keys that must be applied prior to the outer ORDER BY
   *          (typically inherited from an inner SELECT)
   * @param tableReferences
   *          ordered collection of tables referenced in the {@code FROM} clause,
   *          including any JOIN participants
   * @param commonTableExpressions
   *          the common table expressions that can be referenced from this query
   */
  public record Select(List<String> labels, List<String> columnNames, String table, String tableAlias, Expression where,
      int limit, int offset, List<OrderKey> orderBy, boolean distinct, boolean innerDistinct,
      List<String> innerDistinctColumns, List<Expression> expressions, List<Integer> expressionOrder,
      List<QualifiedWildcard> qualifiedWildcards, List<Expression> groupByExpressions, List<List<Integer>> groupingSets,
      Expression having, int preLimit, int preOffset, List<OrderKey> preOrderBy, List<TableReference> tableReferences,
      List<CommonTableExpression> commonTableExpressions) implements Query {

    /**
     * returns "*" if no explicit projection.
     *
     * @return list of column names or ["*"]
     */
    public List<String> columns() {
      return (columnNames == null || columnNames.isEmpty()) ? List.of("*") : columnNames;
    }

    /**
     * ORDER BY keys (never null).
     *
     * @return list of OrderKey objects
     */
    public List<OrderKey> orderBy() {
      // parseSelect already ensures List.copyOf() which prevents null.
      return orderBy;
    }

    /**
     * Determine whether the SELECT statement references more than one table in the
     * {@code FROM} clause.
     *
     * @return {@code true} if multiple tables participate in the query, otherwise
     *         {@code false}
     */
    public boolean hasMultipleTables() {
      return tableReferences != null && tableReferences.size() > 1;
    }
  }

  /**
   * Representation of a SQL set operation query (UNION, INTERSECT, EXCEPT).
   *
   * @param components
   *          ordered list of query components participating in the set operation
   * @param orderBy
   *          order specification applied to the combined result
   * @param limit
   *          optional limit applied after the set operation (-1 if not specified)
   * @param offset
   *          number of rows to skip from the combined result (0 if not specified)
   * @param commonTableExpressions
   *          the common table expressions that can be referenced from this query
   */
  public record SetQuery(List<SetComponent> components, List<SetOrder> orderBy, int limit, int offset,
      List<CommonTableExpression> commonTableExpressions) implements Query {
  }

  /**
   * A single component participating in a SQL set operation query.
   *
   * @param query
   *          parsed representation of the component query. This can be a plain
   *          {@link Select} statement or a nested {@link SetQuery} when
   *          parentheses enforce grouping
   * @param operator
   *          operator used to combine this component with the previous one
   * @param sql
   *          textual SQL used to execute the component query
   */
  public record SetComponent(Query query, SetOperator operator, String sql) {
  }

  /**
   * ORDER BY key for set operation queries supporting either column positions or
   * labels.
   *
   * @param columnIndex
   *          1-based column index when ORDER BY uses positional syntax,
   *          {@code null} otherwise
   * @param columnLabel
   *          column label when ORDER BY references a column name, {@code null}
   *          otherwise
   * @param asc
   *          {@code true} when ascending order is requested, {@code false} for
   *          descending
   */
  public record SetOrder(Integer columnIndex, String columnLabel, boolean asc) {
  }

  /**
   * Definition of a common table expression present in the query.
   *
   * @param name
   *          the exposed name of the CTE
   * @param columnAliases
   *          optional column aliases defined alongside the CTE name
   * @param query
   *          the parsed query that produces the rows of the CTE
   * @param sql
   *          textual representation of the CTE body
   */
  public record CommonTableExpression(String name, List<String> columnAliases, Query query, String sql) {
  }

  /**
   * Enumeration of supported set operators applied between SELECT components.
   */
  public enum SetOperator {
    /** The first SELECT statement in the set query. */
    FIRST,
    /** UNION using distinct semantics. */
    UNION,
    /** UNION ALL preserving duplicates. */
    UNION_ALL,
    /** INTERSECT using distinct semantics. */
    INTERSECT,
    /** INTERSECT preserving duplicates. */
    INTERSECT_ALL,
    /** EXCEPT using distinct semantics. */
    EXCEPT,
    /** EXCEPT ALL preserving duplicates. */
    EXCEPT_ALL
  }

  /**
   * Internal record to hold table name and alias.
   *
   * @param tableName
   *          the table name
   * @param tableAlias
   *          the table alias (may be null)
   * @param columnMapping
   *          mapping between projection labels and physical column names
   * @param innerSelect
   *          parsed representation of the subquery when {@code fromItem} is a
   *          derived table
   * @param subquerySql
   *          textual SQL used to materialize the derived table
   * @param lateral
   *          {@code true} when the from item is marked as {@code LATERAL}
   */
  private record FromInfo(String tableName, String tableAlias, Map<String, String> columnMapping, Select innerSelect,
      String subquerySql, CommonTableExpression commonTableExpression, ValueTableDefinition valueTable,
      UnnestDefinition unnest, boolean lateral) {
  }

  /**
   * Supported join types for explicit joins in the {@code FROM} clause.
   */
  public enum JoinType {
    /** The first table in the FROM clause. */
    BASE,
    /** An INNER JOIN participant. */
    INNER,
    /** A LEFT (OUTER) JOIN participant. */
    LEFT_OUTER,
    /** A RIGHT (OUTER) JOIN participant. */
    RIGHT_OUTER,
    /** A FULL (OUTER) JOIN participant. */
    FULL_OUTER,
    /**
     * A CROSS JOIN introduced via explicit {@code CROSS JOIN} or comma-separated
     * syntax.
     */
    CROSS
  }

  /**
   * Public representation of a table reference discovered in the {@code FROM}
   * clause.
   *
   * @param tableName
   *          the referenced table name
   * @param tableAlias
   *          optional alias used for the table (may be {@code null})
   * @param joinType
   *          type of join that introduced the table (BASE for the first table)
   * @param joinCondition
   *          expression defining the join condition (may be {@code null})
   * @param subquery
   *          parsed representation of the subquery backing this table reference
   *          (may be {@code null})
   * @param subquerySql
   *          textual SQL used to materialize the derived table (may be
   *          {@code null})
   * @param commonTableExpression
   *          the referenced common table expression when the table originates
   *          from a CTE definition (may be {@code null})
   * @param usingColumns
   *          list of column names supplied via a {@code USING} clause that
   *          reference this table (empty when no {@code USING} clause is present)
   * @param valueTable
   *          metadata describing an inline {@code VALUES} table constructor
   *          backing this reference when applicable (may be {@code null})
   * @param unnest
   *          metadata describing an {@code UNNEST} table function associated with
   *          this reference (may be {@code null})
   * @param lateral
   *          {@code true} when the table reference originates from a
   *          {@code LATERAL} clause
   */
  public record TableReference(String tableName, String tableAlias, JoinType joinType, Expression joinCondition,
      Select subquery, String subquerySql, CommonTableExpression commonTableExpression, List<String> usingColumns,
      ValueTableDefinition valueTable, UnnestDefinition unnest, boolean lateral) {
  }

  /**
   * Representation of a {@code VALUES} table constructor discovered in the
   * {@code FROM} clause or as a stand-alone query.
   *
   * @param rows
   *          ordered collection of row expressions supplied to the constructor;
   *          each inner list represents a single row
   * @param columnNames
   *          physical column names exposed by the value table in projection order
   */
  public record ValueTableDefinition(List<List<Expression>> rows, List<String> columnNames) {

    /**
     * Create defensive copies of the supplied row and column metadata.
     */
    public ValueTableDefinition {
      rows = rows == null
          ? List.of()
          : rows.stream().map(row -> row == null ? List.<Expression>of() : List.copyOf(row)).toList();
      columnNames = columnNames == null ? List.of() : List.copyOf(columnNames);
    }
  }

  /**
   * Representation of an {@code UNNEST} table function discovered in the
   * {@code FROM} clause.
   *
   * @param expression
   *          the array expression supplied to {@code UNNEST}
   * @param withOrdinality
   *          {@code true} when {@code WITH ORDINALITY} is requested
   * @param columnAliases
   *          optional list of column aliases defined alongside the table alias
   */
  public record UnnestDefinition(Expression expression, boolean withOrdinality, List<String> columnAliases) {

    /**
     * Create a defensive copy of the alias list when present.
     */
    public UnnestDefinition {
      columnAliases = columnAliases == null ? List.of() : List.copyOf(columnAliases);
    }
  }

  /**
   * Parse a simple SELECT SQL statement (single-table).
   *
   * @param sql
   *          the SQL string, optionally containing {@code --} line comments or
   *          {@code /* ... *&#47;} block comments
   * @return the parsed Select representation
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static Select parseSelect(String sql) {
    Query query = parseQuery(sql);
    if (query instanceof Select sel) {
      return sel;
    }
    throw new IllegalArgumentException("SQL statement does not represent a single SELECT query: " + sql);
  }

  /**
   * Parse a simple SELECT statement while allowing qualified wildcard projections
   * to remain in the AST. This helper is intended for analysis scenarios where
   * the caller needs access to the raw projection items prior to expansion.
   *
   * @param sql
   *          the SQL string, optionally containing {@code --} line comments or
   *          {@code /* ... *&#47;} block comments
   * @return the parsed {@link Select} representation including any qualified
   *         wildcard entries
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static Select parseSelectAllowQualifiedWildcards(String sql) {
    Query query = parseQuery(sql, true);
    if (query instanceof Select sel) {
      return sel;
    }
    throw new IllegalArgumentException("SQL statement does not represent a single SELECT query: " + sql);
  }

  /**
   * Parse a SQL query producing either a plain SELECT or a set operation based
   * query.
   *
   * @param sql
   *          the SQL string, optionally containing {@code --} line comments or
   *          block comments
   * @return a parsed {@link Query} representation
   */
  public static Query parseQuery(String sql) {
    return parseQuery(sql, true);
  }

  private static Query parseQuery(String sql, boolean allowQualifiedWildcards) {
    try {
      String normalizedSql = stripSqlComments(sql);
      net.sf.jsqlparser.statement.select.Select stmt = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil
          .parse(normalizedSql);
      List<CommonTableExpression> ctes = parseWithItems(stmt.getWithItemsList(), allowQualifiedWildcards);
      Map<String, CommonTableExpression> cteLookup = buildCteLookup(ctes);
      return parseSelectStatement(stmt, ctes, cteLookup, allowQualifiedWildcards);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse SQL: " + sql, e);
    }
  }

  private static Query parseSelectStatement(net.sf.jsqlparser.statement.select.Select stmt,
      List<CommonTableExpression> ctes, Map<String, CommonTableExpression> cteLookup, boolean allowQualifiedWildcards) {
    if (stmt instanceof ParenthesedSelect parenthesed) {
      return parseSelectStatement(parenthesed.getSelect(), ctes, cteLookup, allowQualifiedWildcards);
    }
    if (stmt instanceof SetOperationList setList) {
      return parseSetOperationList(setList, ctes, cteLookup, allowQualifiedWildcards);
    }
    if (stmt instanceof PlainSelect plain) {
      return parsePlainSelect(plain, ctes, cteLookup, allowQualifiedWildcards);
    }
    if (stmt instanceof Values values) {
      return parseValuesSelect(values, ctes, cteLookup, allowQualifiedWildcards);
    }
    throw new IllegalArgumentException("Unsupported SELECT statement: " + stmt);
  }

  private static List<CommonTableExpression> parseWithItems(List<WithItem<?>> withItems,
      boolean allowQualifiedWildcards) {
    if (withItems == null || withItems.isEmpty()) {
      return List.of();
    }
    List<CommonTableExpression> parsed = new ArrayList<>(withItems.size());
    List<CommonTableExpression> prior = new ArrayList<>();
    for (WithItem<?> item : withItems) {
      if (item == null) {
        continue;
      }
      ParenthesedSelect select = item.getSelect();
      if (select == null || select.getSelect() == null) {
        throw new IllegalArgumentException("Unsupported WITH item: " + item);
      }
      List<CommonTableExpression> available = List.copyOf(prior);
      Map<String, CommonTableExpression> availableLookup = buildCteLookup(available);
      Query query = parseSelectStatement(select.getSelect(), available, availableLookup, allowQualifiedWildcards);
      List<String> columnAliases = parseCteColumns(item.getWithItemList());
      String sql = select.toString();
      String name = item.getUnquotedAliasName();
      if (name == null || name.isBlank()) {
        name = item.getAliasName();
      }
      if (name == null || name.isBlank()) {
        throw new IllegalArgumentException("WITH item is missing a name: " + item);
      }
      CommonTableExpression cte = new CommonTableExpression(name, columnAliases, query, sql);
      parsed.add(cte);
      prior.add(cte);
    }
    return List.copyOf(parsed);
  }

  private static Map<String, CommonTableExpression> buildCteLookup(List<CommonTableExpression> ctes) {
    if (ctes == null || ctes.isEmpty()) {
      return Map.of();
    }
    Map<String, CommonTableExpression> lookup = new HashMap<>();
    for (CommonTableExpression cte : ctes) {
      if (cte == null || cte.name() == null) {
        continue;
      }
      String key = identifierKey(cte.name());
      if (key != null) {
        lookup.put(key, cte);
      }
    }
    return Map.copyOf(lookup);
  }

  private static List<String> parseCteColumns(List<SelectItem<?>> items) {
    if (items == null || items.isEmpty()) {
      return List.of();
    }
    List<String> columns = new ArrayList<>(items.size());
    for (SelectItem<?> item : items) {
      columns.add(item.toString().trim());
    }
    return List.copyOf(columns);
  }

  private static CommonTableExpression resolveCommonTableExpression(Table table,
      Map<String, CommonTableExpression> cteLookup) {
    if (table == null || cteLookup == null || cteLookup.isEmpty()) {
      return null;
    }
    String[] candidates = {
        table.getUnquotedName(), table.getFullyQualifiedName(), table.getName()
    };
    for (String candidate : candidates) {
      String key = identifierKey(candidate);
      if (key != null) {
        CommonTableExpression cte = cteLookup.get(key);
        if (cte != null) {
          return cte;
        }
      }
    }
    return null;
  }

  private static String identifierKey(String identifier) {
    if (identifier == null) {
      return null;
    }
    String trimmed = identifier.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    return trimmed.toLowerCase(Locale.ROOT);
  }

  private static Select parsePlainSelect(PlainSelect ps, List<CommonTableExpression> ctes,
      Map<String, CommonTableExpression> cteLookup, boolean allowQualifiedWildcards) {
    FromInfo fromInfo = parseFromItem(ps.getFromItem(), ctes, cteLookup, allowQualifiedWildcards);
    List<JoinInfo> joinInfos = parseJoins(ps.getJoins(), ctes, cteLookup, allowQualifiedWildcards);
    List<TableReference> tableRefs = buildTableReferences(fromInfo, joinInfos);
    final Projection projection = parseProjectionList(ps.getSelectItems(), fromInfo, tableRefs,
        allowQualifiedWildcards);

    Expression whereExpr = ps.getWhere();
    if (tableRefs.size() == 1) {
      stripQualifier(whereExpr, qualifiers(tableRefs));
    }
    int outerLimit = computeLimit(ps);
    int outerOffset = computeOffset(ps);
    Select inner = fromInfo.innerSelect();
    int innerLimit = inner == null ? -1 : inner.limit();
    int innerOffset = inner == null ? 0 : inner.offset();

    int limit;
    if (outerLimit >= 0 && innerLimit >= 0) {
      limit = Math.min(outerLimit, innerLimit);
    } else if (outerLimit >= 0) {
      limit = outerLimit;
    } else {
      limit = innerLimit;
    }

    int offset = outerOffset;
    if (innerOffset > 0) {
      offset += innerOffset;
    }

    List<OrderKey> orderKeys = computeOrderKeys(ps, projection.labels(), projection.physicalCols(), tableRefs);
    boolean outerRequestedOrder = orderKeys != null && !orderKeys.isEmpty();
    if ((orderKeys == null || orderKeys.isEmpty()) && inner != null) {
      orderKeys = inner.orderBy();
    }
    List<OrderKey> preOrderBy = List.of();
    if (inner != null && outerRequestedOrder) {
      List<OrderKey> innerOrder = inner.orderBy();
      if (innerOrder != null && !innerOrder.isEmpty() && orderKeys != innerOrder) {
        preOrderBy = innerOrder;
      }
    }

    final boolean outerDistinct = ps.getDistinct() != null;
    final boolean innerDistinct = inner != null && inner.distinct();

    int preLimit = -1;
    int preOffset = 0;
    if (innerLimit >= 0 && outerRequestedOrder) {
      preLimit = innerLimit;
    }
    if (innerOffset > 0 && (outerRequestedOrder || preLimit >= 0 || outerDistinct)) {
      preOffset = innerOffset;
    }

    List<String> labels = projection.labels();
    List<String> physicalCols = projection.physicalCols();
    List<Expression> expressions = projection.expressions();
    List<Integer> expressionOrder = projection.expressionOrder();

    if (projection.selectAll()) {
      if (fromInfo.innerSelect() != null && (tableRefs == null || tableRefs.size() <= 1)) {
        labels = fromInfo.innerSelect().labels();
        physicalCols = fromInfo.innerSelect().columnNames();
        expressions = fromInfo.innerSelect().expressions();
        expressionOrder = fromInfo.innerSelect().expressionOrder();
      } else {
        labels = List.of();
        physicalCols = List.of();
        expressions = List.of();
        expressionOrder = List.of();
      }
    }

    if (physicalCols == null) {
      physicalCols = List.of();
    }
    if (orderKeys == null) {
      orderKeys = List.of();
    }

    boolean distinct = outerDistinct || innerDistinct;

    Expression havingExpr = ps.getHaving();
    if (tableRefs.size() == 1) {
      stripQualifier(havingExpr, qualifiers(tableRefs));
    }

    Expression joinCondition = combineJoinConditions(joinInfos, tableRefs);
    Expression combinedWhere = combineExpressions(
        combineExpressions(fromInfo.innerSelect() == null ? null : fromInfo.innerSelect().where(), joinCondition),
        whereExpr);
    Expression combinedHaving = combineExpressions(
        fromInfo.innerSelect() == null ? null : fromInfo.innerSelect().having(), havingExpr);

    List<String> labelsCopy = List.copyOf(labels);
    List<String> physicalCopy = Collections.unmodifiableList(new ArrayList<>(physicalCols));
    List<OrderKey> orderCopy = List.copyOf(orderKeys);
    List<OrderKey> preOrderCopy = List.copyOf(preOrderBy);
    List<Expression> expressionCopy = List.copyOf(expressions);
    List<Integer> expressionOrderCopy = List.copyOf(expressionOrder);
    List<QualifiedWildcard> wildcardCopy = List.copyOf(projection.qualifiedWildcards());
    List<String> innerDistinctCols = innerDistinct && inner != null ? List.copyOf(inner.columnNames()) : List.of();

    GroupByInfo groupByInfo = parseGroupBy(ps.getGroupBy(), tableRefs);

    return new Select(labelsCopy, physicalCopy, fromInfo.tableName(), fromInfo.tableAlias(), combinedWhere, limit,
        offset, orderCopy, distinct, innerDistinct, innerDistinctCols, expressionCopy, expressionOrderCopy,
        wildcardCopy, groupByInfo.expressions(), groupByInfo.groupingSets(), combinedHaving, preLimit, preOffset,
        preOrderCopy, tableRefs, List.copyOf(ctes));
  }

  private static Select parseValuesSelect(Values values, List<CommonTableExpression> ctes,
      Map<String, CommonTableExpression> cteLookup, boolean allowQualifiedWildcards) {
    if (values == null) {
      throw new IllegalArgumentException("VALUES statement cannot be null");
    }
    FromInfo fromInfo = parseFromItem(values, ctes, cteLookup, allowQualifiedWildcards);
    if (fromInfo.valueTable() == null) {
      throw new IllegalArgumentException("VALUES statement did not produce a value table definition: " + values);
    }
    List<TableReference> tableRefs = buildTableReferences(fromInfo, List.of());
    List<String> columnNames = fromInfo.valueTable().columnNames();
    if (columnNames.isEmpty()) {
      throw new IllegalArgumentException(
          "VALUES statement must produce at least one column but produced zero: " + values);
    }
    List<String> labels = new ArrayList<>(columnNames);
    List<Expression> expressions = new ArrayList<>(columnNames.size());
    List<Integer> expressionOrder = new ArrayList<>(columnNames.size());
    for (int i = 0; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      Column column = new Column();
      column.setColumnName(columnName);
      String qualifier = fromInfo.tableAlias();
      if (qualifier != null && !qualifier.isBlank()) {
        Table table = new Table();
        table.setName(qualifier);
        column.setTable(table);
      }
      expressions.add(column);
      expressionOrder.add(i);
    }
    List<String> labelCopy = List.copyOf(labels);
    List<String> columnCopy = List.copyOf(columnNames);
    List<Expression> expressionCopy = List.copyOf(expressions);
    List<Integer> orderCopy = List.copyOf(expressionOrder);
    return new Select(labelCopy, columnCopy, fromInfo.tableName(), fromInfo.tableAlias(), null, -1, 0, List.of(), false,
        false, List.of(), expressionCopy, orderCopy, List.of(), List.of(), List.of(), null, -1, 0, List.of(), tableRefs,
        List.copyOf(ctes));
  }

  /**
   * Parse a {@link SetOperationList} into a {@link SetQuery} representation.
   *
   * @param list
   *          parsed set operation list produced by JSqlParser
   * @return a normalized {@link SetQuery} describing the components and modifiers
   */
  private static SetQuery parseSetOperationList(SetOperationList list, List<CommonTableExpression> ctes,
      Map<String, CommonTableExpression> cteLookup, boolean allowQualifiedWildcards) {
    List<net.sf.jsqlparser.statement.select.Select> selects = list.getSelects();
    List<SetOperation> operations = list.getOperations();
    if (selects == null || selects.isEmpty()) {
      throw new IllegalArgumentException("Set operation query must contain at least one SELECT statement");
    }
    List<SetComponent> components = new ArrayList<>();
    for (int i = 0; i < selects.size(); i++) {
      net.sf.jsqlparser.statement.select.Select body = selects.get(i);
      Query component = parseSelectStatement(body, ctes, cteLookup, allowQualifiedWildcards);
      SetOperator operator = SetOperator.FIRST;
      if (i > 0) {
        SetOperation op = operations.get(i - 1);
        if (op instanceof UnionOp unionOp) {
          operator = unionOp.isAll() ? SetOperator.UNION_ALL : SetOperator.UNION;
        } else if (op instanceof IntersectOp intersectOp) {
          operator = intersectOp.isAll() ? SetOperator.INTERSECT_ALL : SetOperator.INTERSECT;
        } else if (op instanceof ExceptOp exceptOp) {
          operator = exceptOp.isAll() ? SetOperator.EXCEPT_ALL : SetOperator.EXCEPT;
        } else {
          throw new IllegalArgumentException("Unsupported set operation: " + op);
        }
      }
      components.add(new SetComponent(component, operator, body.toString()));
    }
    List<SetOrder> orderBy = parseSetOrderBy(list.getOrderByElements());
    int limit = resolveLimit(list.getLimit(), list.getFetch());
    int offset = extractOffset(list.getOffset());
    return new SetQuery(List.copyOf(components), orderBy, limit, offset, List.copyOf(ctes));
  }

  /**
   * Parse ORDER BY elements applied to a set operation query.
   *
   * @param orderByElements
   *          raw ORDER BY elements from the parser
   * @return normalized {@link SetOrder} definitions
   */
  private static List<SetOrder> parseSetOrderBy(List<OrderByElement> orderByElements) {
    if (orderByElements == null || orderByElements.isEmpty()) {
      return List.of();
    }
    List<SetOrder> orders = new ArrayList<>(orderByElements.size());
    for (OrderByElement element : orderByElements) {
      Expression expression = element.getExpression();
      Integer columnIndex = null;
      String columnLabel = null;
      if (expression instanceof LongValue value) {
        columnIndex = value.getBigIntegerValue().intValue();
      } else if (expression instanceof Column column) {
        columnLabel = column.getColumnName();
      } else {
        throw new IllegalArgumentException("Set operation ORDER BY supports column index or name only: " + expression);
      }
      boolean asc = !element.isAscDescPresent() || element.isAsc();
      orders.add(new SetOrder(columnIndex, columnLabel, asc));
    }
    return List.copyOf(orders);
  }

  /**
   * Extract the LIMIT value from a parsed clause.
   *
   * @param limit
   *          LIMIT clause (may be {@code null})
   * @return limit value, or {@code -1} when not specified
   */
  private static int extractLimit(Limit limit) {
    if (limit == null || limit.getRowCount() == null) {
      return -1;
    }
    Expression rowCountExpr = limit.getRowCount();
    if (rowCountExpr instanceof LongValue lv) {
      return lv.getBigIntegerValue().intValue();
    }
    return Integer.parseInt(rowCountExpr.toString());
  }

  /**
   * Extract the row-count from a FETCH clause.
   *
   * @param fetch
   *          FETCH clause (may be {@code null})
   * @return the requested number of rows, {@code -1} when not specified
   */
  private static int extractFetch(Fetch fetch) {
    if (fetch == null) {
      return -1;
    }
    Expression expression = fetch.getExpression();
    if (expression == null) {
      return 1;
    }
    if (expression instanceof LongValue lv) {
      return lv.getBigIntegerValue().intValue();
    }
    return Integer.parseInt(expression.toString());
  }

  /**
   * Resolve the effective LIMIT by combining SQL LIMIT and FETCH clauses.
   *
   * @param limit
   *          LIMIT clause (may be {@code null})
   * @param fetch
   *          FETCH clause (may be {@code null})
   * @return the effective limit, or {@code -1} when neither clause is specified
   */
  private static int resolveLimit(Limit limit, Fetch fetch) {
    int limitValue = extractLimit(limit);
    int fetchValue = extractFetch(fetch);
    if (limitValue >= 0 && fetchValue >= 0) {
      return Math.min(limitValue, fetchValue);
    }
    return limitValue >= 0 ? limitValue : fetchValue;
  }

  /**
   * Extract the OFFSET value from a parsed clause.
   *
   * @param offset
   *          OFFSET clause (may be {@code null})
   * @return offset value, or {@code 0} when not specified
   */
  private static int extractOffset(Offset offset) {
    if (offset == null || offset.getOffset() == null) {
      return 0;
    }
    Expression expr = offset.getOffset();
    if (expr instanceof LongValue lv) {
      return lv.getBigIntegerValue().intValue();
    }
    try {
      return Integer.parseInt(expr.toString());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid OFFSET value: '" + expr + "'", e);
    }
  }

  // === Parsing Helper Methods =================================================

  /**
   * Internal record to hold the projection (SELECT list) results.
   */
  private record Projection(List<String> labels, List<String> physicalCols, List<Expression> expressions,
      List<Integer> expressionOrder, List<QualifiedWildcard> qualifiedWildcards, boolean selectAll) {
  }

  /**
   * Parses the FROM clause, ensuring it's a single table and extracts its
   * name/alias.
   */
  private static FromInfo parseFromItem(FromItem fromItem, List<CommonTableExpression> ctes,
      Map<String, CommonTableExpression> cteLookup, boolean allowQualifiedWildcards) {
    return parseFromItem(fromItem, ctes, cteLookup, allowQualifiedWildcards, false);
  }

  private static FromInfo parseFromItem(FromItem fromItem, List<CommonTableExpression> ctes,
      Map<String, CommonTableExpression> cteLookup, boolean allowQualifiedWildcards, boolean lateralHint) {
    boolean lateral = lateralHint;
    if (fromItem instanceof Table t) {
      String tableName = t.getName();
      String tableAlias = (t.getAlias() != null) ? t.getAlias().getName() : null;
      CommonTableExpression cte = resolveCommonTableExpression(t, cteLookup);
      Map<String, String> mapping = Map.of();
      Select innerSelect = null;
      if (cte != null) {
        if (cte.query() instanceof Select selectQuery) {
          innerSelect = selectQuery;
          mapping = buildColumnMapping(selectQuery);
        }
        if (cte.columnAliases() != null && !cte.columnAliases().isEmpty()) {
          Map<String, String> aliasMapping = new HashMap<>();
          for (String alias : cte.columnAliases()) {
            if (alias != null && !alias.isBlank()) {
              aliasMapping.put(alias, alias);
            }
          }
          if (!aliasMapping.isEmpty()) {
            mapping = Map.copyOf(aliasMapping);
          }
        }
      }
      return new FromInfo(tableName, tableAlias, mapping, innerSelect, null, cte, null, null, lateral);
    }
    if (fromItem instanceof ParenthesedFromItem parenthesed) {
      FromItem inner = parenthesed.getFromItem();
      FromInfo innerInfo = parseFromItem(inner, ctes, cteLookup, allowQualifiedWildcards, lateral);
      return applyAlias(innerInfo, parenthesed.getAlias());
    }
    if (fromItem instanceof LateralSubSelect lateralSub) {
      lateral = true;
      PlainSelect innerPlain = lateralSub.getPlainSelect();
      Alias aliasNode = lateralSub.getAlias();
      return parseSubSelect(innerPlain, aliasNode, ctes, cteLookup, allowQualifiedWildcards, lateral);
    }
    if (fromItem instanceof ParenthesedSelect sub) {
      PlainSelect innerPlain = sub.getPlainSelect();
      Alias aliasNode = sub.getAlias();
      return parseSubSelect(innerPlain, aliasNode, ctes, cteLookup, allowQualifiedWildcards, lateral);
    }
    if (fromItem instanceof TableFunction tableFunction) {
      boolean tableFunctionLateral = lateral || "lateral".equalsIgnoreCase(tableFunction.getPrefix());
      return parseTableFunction(tableFunction, tableFunctionLateral);
    }
    if (fromItem instanceof Values valuesItem) {
      return parseValuesFromItem(valuesItem, lateral);
    }
    throw new IllegalArgumentException("Unsupported FROM item: " + fromItem);
  }

  private static FromInfo parseSubSelect(PlainSelect innerPlain, Alias aliasNode, List<CommonTableExpression> ctes,
      Map<String, CommonTableExpression> cteLookup, boolean allowQualifiedWildcards, boolean lateral) {
    if (innerPlain == null) {
      throw new IllegalArgumentException("Only plain SELECT subqueries are supported");
    }
    Select innerSelect = parsePlainSelect(innerPlain, ctes, cteLookup, allowQualifiedWildcards);
    String alias = null;
    if (aliasNode != null && aliasNode.getName() != null && !aliasNode.getName().isBlank()) {
      alias = aliasNode.getName();
    } else if (innerSelect.tableAlias() != null && !innerSelect.tableAlias().isBlank()) {
      alias = innerSelect.tableAlias();
    } else {
      alias = innerSelect.table();
    }
    Map<String, String> mapping = buildColumnMapping(innerSelect);
    String subquerySql = innerPlain.toString();
    return new FromInfo(innerSelect.table(), alias, mapping, innerSelect, subquerySql, null, null, null, lateral);
  }

  private static FromInfo parseTableFunction(TableFunction tableFunction, boolean lateral) {
    Function function = tableFunction.getFunction();
    if (function == null || function.getName() == null) {
      throw new IllegalArgumentException("Unsupported table function in FROM clause: " + tableFunction);
    }
    Function effectiveFunction = unwrapTableWrapper(function, tableFunction);
    String name = effectiveFunction.getName();
    if (!"unnest".equalsIgnoreCase(name)) {
      throw new IllegalArgumentException("Only UNNEST table functions are supported: " + tableFunction);
    }
    ExpressionList<?> parameters = effectiveFunction.getParameters();
    List<Expression> expressions = parameters == null
        ? List.of()
        : parameters.stream().filter(Expression.class::isInstance).map(Expression.class::cast)
            .collect(Collectors.toUnmodifiableList());
    if (expressions.isEmpty()) {
      throw new IllegalArgumentException("UNNEST requires an array expression: " + tableFunction);
    }
    if (expressions.size() != 1) {
      throw new IllegalArgumentException("UNNEST accepts exactly one argument: " + tableFunction);
    }
    Expression expression = expressions.get(0);
    Alias alias = tableFunction.getAlias();
    String aliasName = alias == null ? null : alias.getName();
    if (aliasName == null || aliasName.isBlank()) {
      throw new IllegalArgumentException("UNNEST must specify a table alias: " + tableFunction);
    }
    List<String> columnAliases = extractAliasColumns(alias);
    boolean withOrdinality = tableFunction.getWithClause() != null
        && "ORDINALITY".equalsIgnoreCase(tableFunction.getWithClause().trim());
    UnnestDefinition unnest = new UnnestDefinition(expression, withOrdinality, columnAliases);
    return new FromInfo(null, aliasName, Map.of(), null, null, null, null, unnest, lateral);
  }

  private static Function unwrapTableWrapper(Function function, TableFunction tableFunction) {
    if (!"table".equalsIgnoreCase(function.getName())) {
      return function;
    }
    ExpressionList<?> parameters = function.getParameters();
    if (parameters == null || parameters.isEmpty()) {
      throw new IllegalArgumentException("TABLE wrapper requires an inner function: " + tableFunction);
    }
    if (parameters.size() != 1) {
      throw new IllegalArgumentException("TABLE wrapper accepts exactly one argument: " + tableFunction);
    }
    Object parameter = parameters.get(0);
    if (!(parameter instanceof Function innerFunction)) {
      throw new IllegalArgumentException("TABLE wrapper must contain a function call: " + tableFunction);
    }
    return innerFunction;
  }

  private static FromInfo parseValuesFromItem(Values values, boolean lateral) {
    ExpressionList<?> expressions = values.getExpressions();
    if (expressions == null || expressions.isEmpty()) {
      throw new IllegalArgumentException("VALUES constructor must specify at least one row: " + values);
    }
    List<List<Expression>> rows = new ArrayList<>();
    int columnCount = -1;
    for (Object expression : expressions) {
      List<Expression> row = new ArrayList<>();
      if (expression instanceof ExpressionList<?> list) {
        for (Object element : list) {
          if (element instanceof Expression expr) {
            row.add(expr);
          }
        }
      } else if (expression instanceof Expression expr) {
        row.add(expr);
      }
      if (row.isEmpty()) {
        throw new IllegalArgumentException("VALUES row cannot be empty: " + expression);
      }
      if (columnCount < 0) {
        columnCount = row.size();
      } else if (columnCount != row.size()) {
        throw new IllegalArgumentException("VALUES rows must all contain " + columnCount + " columns: " + values);
      }
      rows.add(List.copyOf(row));
    }
    Alias aliasNode = values.getAlias();
    List<String> aliasColumns = extractAliasColumns(aliasNode);
    if (!aliasColumns.isEmpty() && aliasColumns.size() != columnCount) {
      throw new IllegalArgumentException("VALUES alias declares " + aliasColumns.size()
          + " columns but constructor produces " + columnCount + " columns: " + values);
    }
    List<String> columnNames;
    if (!aliasColumns.isEmpty()) {
      columnNames = aliasColumns;
    } else {
      List<String> generated = new ArrayList<>(columnCount);
      for (int i = 0; i < columnCount; i++) {
        generated.add("column" + (i + 1));
      }
      columnNames = List.copyOf(generated);
    }
    Map<String, String> mapping = new LinkedHashMap<>();
    for (String columnName : columnNames) {
      mapping.put(columnName, columnName);
    }
    String aliasName = aliasNode == null ? null : aliasNode.getName();
    String tableName = (aliasName != null && !aliasName.isBlank()) ? aliasName : "values";
    ValueTableDefinition valueTable = new ValueTableDefinition(rows, columnNames);
    return new FromInfo(tableName, aliasName, Map.copyOf(mapping), null, null, null, valueTable, null, lateral);
  }

  private static FromInfo applyAlias(FromInfo info, Alias aliasNode) {
    if (info == null || aliasNode == null) {
      return info;
    }
    String aliasName = aliasNode.getName();
    if (aliasName != null) {
      aliasName = aliasName.trim();
      if (aliasName.isEmpty()) {
        aliasName = null;
      }
    }
    Map<String, String> mapping = info.columnMapping() == null ? Map.of() : info.columnMapping();
    ValueTableDefinition valueTable = info.valueTable();
    List<String> aliasColumns = extractAliasColumns(aliasNode);
    if (!aliasColumns.isEmpty()) {
      if (valueTable != null) {
        if (valueTable.columnNames().size() != aliasColumns.size()) {
          throw new IllegalArgumentException("VALUES alias declares " + aliasColumns.size()
              + " columns but constructor produces " + valueTable.columnNames().size() + " columns");
        }
        valueTable = new ValueTableDefinition(valueTable.rows(), aliasColumns);
        Map<String, String> aliasMapping = new LinkedHashMap<>();
        for (String columnName : aliasColumns) {
          aliasMapping.put(columnName, columnName);
        }
        mapping = Map.copyOf(aliasMapping);
      } else if (info.innerSelect() != null && info.innerSelect().columnNames() != null
          && !info.innerSelect().columnNames().isEmpty()) {
        List<String> physical = info.innerSelect().columnNames();
        if (physical.size() != aliasColumns.size()) {
          throw new IllegalArgumentException("Alias column count does not match derived table projection");
        }
        Map<String, String> aliasMapping = new LinkedHashMap<>();
        for (int i = 0; i < aliasColumns.size(); i++) {
          String aliasColumn = aliasColumns.get(i);
          String physicalColumn = physical.get(i);
          if (physicalColumn == null || physicalColumn.isBlank()) {
            physicalColumn = aliasColumn;
          }
          aliasMapping.put(aliasColumn, physicalColumn);
        }
        mapping = Map.copyOf(aliasMapping);
      } else {
        Map<String, String> aliasMapping = new LinkedHashMap<>();
        for (String columnName : aliasColumns) {
          aliasMapping.put(columnName, columnName);
        }
        mapping = Map.copyOf(aliasMapping);
      }
    }

    String tableName = aliasName != null ? aliasName : info.tableName();
    String tableAlias = aliasName != null ? aliasName : info.tableAlias();
    return new FromInfo(tableName, tableAlias, mapping, info.innerSelect(), info.subquerySql(),
        info.commonTableExpression(), valueTable, info.unnest(), info.lateral());
  }

  private static List<String> extractAliasColumns(Alias alias) {
    if (alias == null || alias.getAliasColumns() == null || alias.getAliasColumns().isEmpty()) {
      return List.of();
    }
    List<String> columns = new ArrayList<>(alias.getAliasColumns().size());
    for (Alias.AliasColumn column : alias.getAliasColumns()) {
      if (column == null) {
        continue;
      }
      String name = column.name;
      if (name == null) {
        continue;
      }
      String trimmed = name.trim();
      if (!trimmed.isEmpty()) {
        columns.add(trimmed);
      }
    }
    return List.copyOf(columns);
  }

  private static Map<String, String> buildColumnMapping(Select select) {
    if (select == null) {
      return Map.of();
    }
    List<String> labels = select.labels();
    List<String> physical = select.columnNames();
    if (labels == null || physical == null || labels.isEmpty()) {
      return Map.of();
    }
    Map<String, String> mapping = new HashMap<>();
    int count = Math.min(labels.size(), physical.size());
    for (int i = 0; i < count; i++) {
      String label = labels.get(i);
      String phys = physical.get(i);
      if (label != null) {
        String mapped = phys == null || phys.isBlank() ? label : phys;
        mapping.put(label, mapped);
      }
    }
    return Map.copyOf(mapping);
  }

  /**
   * Parses the SELECT list, resolving aliases and determining physical column
   * names.
   */
  private static Projection parseProjectionList(List<SelectItem<?>> selectItems, FromInfo fromInfo,
      List<TableReference> tableRefs, boolean allowQualifiedWildcards) {
    List<String> labels = new ArrayList<>();
    List<String> physicalCols = new ArrayList<>();
    List<Expression> expressions = new ArrayList<>();
    List<QualifiedWildcard> qualifiedWildcards = new ArrayList<>();
    List<Integer> expressionOrder = new ArrayList<>();

    boolean selectAll = false;
    int selectIndex = 0;
    for (SelectItem<?> item : selectItems) {
      String text = item.toString().trim();
      Expression expr = item.getExpression();

      if (expr instanceof AllTableColumns tableColumns) {
        String qualifier = resolveQualifiedWildcardQualifier(tableColumns, text);
        SourcePosition position = extractSourcePosition(item);
        qualifiedWildcards.add(new QualifiedWildcard(qualifier, position, selectIndex));
        selectIndex++;
        if (!allowQualifiedWildcards) {
          throw new IllegalArgumentException("Qualified * (table.*) not supported yet: " + text);
        }
        continue;
      }
      if (expr instanceof AllColumns) {
        selectAll = true;
        break;
      }

      if (tableRefs.size() == 1) {
        stripQualifier(expr, qualifiers(tableRefs));
      }

      final Alias aliasObj = item.getAlias();
      final String alias = aliasObj == null ? null : aliasObj.getName();

      String label;
      String underlying = null;

      if (expr instanceof Column col) {
        underlying = col.getColumnName();
        String mapped = fromInfo.columnMapping().get(underlying);
        if (mapped != null) {
          underlying = mapped;
        }
        if (alias != null && !alias.isEmpty()) {
          label = alias;
        } else {
          label = col.getColumnName();
        }
      } else {
        if (alias != null && !alias.isEmpty()) {
          label = alias;
        } else {
          label = expr.toString();
        }
        underlying = label;
      }

      labels.add(label);
      physicalCols.add(underlying);
      expressions.add(expr);
      expressionOrder.add(selectIndex);
      selectIndex++;
    }

    if (selectAll) {
      labels = List.of();
      physicalCols = List.of();
      expressions = List.of();
      expressionOrder = List.of();
    }

    return new Projection(labels, physicalCols, expressions, List.copyOf(expressionOrder),
        List.copyOf(qualifiedWildcards), selectAll);
  }

  /**
   * Describes a column produced when expanding a qualified wildcard projection.
   *
   * @param label
   *          column label that should appear in the result set metadata
   * @param physicalName
   *          canonical physical column name used when accessing the underlying
   *          schema
   */
  public record QualifiedExpansionColumn(String label, String physicalName) {

    /**
     * Create a new expansion column definition.
     *
     * @param label
     *          column label that should appear in the result set metadata
     * @param physicalName
     *          canonical physical column name used when accessing the underlying
     *          schema
     */
    public QualifiedExpansionColumn {
      if (physicalName == null || physicalName.isBlank()) {
        throw new IllegalArgumentException("physicalName must not be null or blank");
      }
      if (label == null || label.isBlank()) {
        label = physicalName;
      }
    }
  }

  /**
   * Expand any qualified wildcard entries ({@code table.*}) into explicit column
   * projections using the supplied qualifier mapping.
   *
   * @param select
   *          the parsed select statement that may contain qualified wildcards
   * @param qualifierColumns
   *          mapping from qualifier to the ordered column definitions exposed by
   *          that qualifier
   * @return a {@link Select} instance where each qualified wildcard has been
   *         replaced with concrete column projections
   */
  public static Select expandQualifiedWildcards(Select select,
      Map<String, List<QualifiedExpansionColumn>> qualifierColumns) {
    if (select == null || select.qualifiedWildcards().isEmpty()) {
      return select;
    }
    if (qualifierColumns == null || qualifierColumns.isEmpty()) {
      throw new IllegalArgumentException("Qualified wildcard expansion requires available column metadata");
    }

    Map<String, List<QualifiedExpansionColumn>> normalized = normaliseQualifierColumns(qualifierColumns);
    List<OrderedSelectItem> ordered = new ArrayList<>();

    List<String> labels = select.labels();
    List<String> columnNames = select.columnNames();
    List<Expression> expressions = select.expressions();
    List<Integer> expressionOrder = select.expressionOrder();

    for (int i = 0; i < expressions.size(); i++) {
      Expression expression = expressions.get(i);
      int orderIndex = (expressionOrder != null && i < expressionOrder.size()) ? expressionOrder.get(i) : i;
      String label = (labels != null && i < labels.size()) ? labels.get(i) : null;
      String physical = (columnNames != null && i < columnNames.size()) ? columnNames.get(i) : null;
      ordered.add(new OrderedSelectItem(orderIndex, 0, label, physical, expression));
    }

    int fallbackIndex = expressions.size();
    for (QualifiedWildcard wildcard : select.qualifiedWildcards()) {
      String qualifier = wildcard.qualifier();
      List<QualifiedExpansionColumn> columns = resolveQualifierColumns(qualifier, normalized);
      if (columns == null || columns.isEmpty()) {
        throw new IllegalArgumentException("Qualified wildcard references unknown qualifier: " + qualifier);
      }
      int orderIndex = wildcard.selectListIndex() >= 0 ? wildcard.selectListIndex() : fallbackIndex;
      if (wildcard.selectListIndex() < 0) {
        fallbackIndex++;
      }
      int subIndex = 0;
      for (QualifiedExpansionColumn columnInfo : columns) {
        String label = columnInfo.label();
        String physicalName = columnInfo.physicalName();
        Column column = new Column();
        column.setColumnName(label);
        if (qualifier != null && !qualifier.isBlank()) {
          Table table = new Table();
          table.setName(qualifier);
          column.setTable(table);
        }
        ordered.add(new OrderedSelectItem(orderIndex, subIndex, label, physicalName, column));
        subIndex++;
      }
    }

    ordered.sort(Comparator.comparingInt(OrderedSelectItem::order).thenComparingInt(OrderedSelectItem::subOrder));

    List<String> newLabels = new ArrayList<>(ordered.size());
    List<String> newPhysical = new ArrayList<>(ordered.size());
    List<Expression> newExpressions = new ArrayList<>(ordered.size());
    List<Integer> newExpressionOrder = new ArrayList<>(ordered.size());

    for (int i = 0; i < ordered.size(); i++) {
      OrderedSelectItem item = ordered.get(i);
      newLabels.add(item.label());
      newPhysical.add(item.physical());
      newExpressions.add(item.expression());
      newExpressionOrder.add(i);
    }

    List<String> labelCopy = List.copyOf(newLabels);
    List<String> physicalCopy = Collections.unmodifiableList(new ArrayList<>(newPhysical));
    List<Expression> expressionCopy = List.copyOf(newExpressions);
    List<Integer> orderCopy = List.copyOf(newExpressionOrder);

    return new Select(labelCopy, physicalCopy, select.table(), select.tableAlias(), select.where(), select.limit(),
        select.offset(), select.orderBy(), select.distinct(), select.innerDistinct(), select.innerDistinctColumns(),
        expressionCopy, orderCopy, List.of(), select.groupByExpressions(), select.groupingSets(), select.having(),
        select.preLimit(), select.preOffset(), select.preOrderBy(), select.tableReferences(),
        select.commonTableExpressions());
  }

  private static Map<String, List<QualifiedExpansionColumn>> normaliseQualifierColumns(
      Map<String, List<QualifiedExpansionColumn>> qualifierColumns) {
    Map<String, List<QualifiedExpansionColumn>> normalized = new LinkedHashMap<>();
    for (Map.Entry<String, List<QualifiedExpansionColumn>> entry : qualifierColumns.entrySet()) {
      String key = entry.getKey();
      List<QualifiedExpansionColumn> value = entry.getValue();
      if (key == null || value == null || value.isEmpty()) {
        continue;
      }
      List<QualifiedExpansionColumn> copy = List.copyOf(value);
      normalized.putIfAbsent(key, copy);
      String lower = key.toLowerCase(Locale.ROOT);
      normalized.putIfAbsent(lower, copy);
    }
    return normalized;
  }

  private static List<QualifiedExpansionColumn> resolveQualifierColumns(String qualifier,
      Map<String, List<QualifiedExpansionColumn>> normalized) {
    if (qualifier == null) {
      return normalized.get(null);
    }
    List<QualifiedExpansionColumn> direct = normalized.get(qualifier);
    if (direct != null) {
      return direct;
    }
    return normalized.get(qualifier.toLowerCase(Locale.ROOT));
  }

  private record OrderedSelectItem(int order, int subOrder, String label, String physical, Expression expression) {
  }

  private static String resolveQualifiedWildcardQualifier(AllTableColumns tableColumns, String originalText) {
    if (tableColumns == null) {
      throw new IllegalArgumentException("Qualified wildcard is missing table reference: " + originalText);
    }
    Table table = tableColumns.getTable();
    if (table != null) {
      Alias alias = table.getAlias();
      if (alias != null && alias.getName() != null && !alias.getName().isBlank()) {
        return stripCompositeIdentifierQuotes(alias.getName());
      }
      String fqn = table.getFullyQualifiedName();
      if (fqn != null && !fqn.isBlank()) {
        return stripCompositeIdentifierQuotes(fqn);
      }
      String name = table.getName();
      if (name != null && !name.isBlank()) {
        return stripCompositeIdentifierQuotes(name);
      }
    }
    String text = tableColumns.toString();
    if (text != null) {
      String trimmed = text.trim();
      if (trimmed.endsWith(".*")) {
        String qualifier = trimmed.substring(0, trimmed.length() - 2).trim();
        if (!qualifier.isEmpty()) {
          return stripCompositeIdentifierQuotes(qualifier);
        }
      }
      if (!trimmed.isEmpty() && !".*".equals(trimmed)) {
        return stripCompositeIdentifierQuotes(trimmed);
      }
    }
    throw new IllegalArgumentException("Qualified wildcard is missing a qualifier: " + originalText);
  }

  private static String stripCompositeIdentifierQuotes(String identifier) {
    if (identifier == null) {
      return null;
    }
    String trimmed = identifier.trim();
    if (trimmed.isEmpty()) {
      return trimmed;
    }
    StringBuilder result = new StringBuilder();
    StringBuilder segment = new StringBuilder();
    boolean inDoubleQuote = false;
    boolean inBacktick = false;
    boolean inBracket = false;
    for (int i = 0; i < trimmed.length(); i++) {
      char ch = trimmed.charAt(i);
      if (ch == '"' && !inBacktick && !inBracket) {
        inDoubleQuote = !inDoubleQuote;
        segment.append(ch);
        continue;
      }
      if (ch == '`' && !inDoubleQuote && !inBracket) {
        inBacktick = !inBacktick;
        segment.append(ch);
        continue;
      }
      if (ch == '[' && !inDoubleQuote && !inBacktick && !inBracket) {
        inBracket = true;
        segment.append(ch);
        continue;
      }
      if (ch == ']' && inBracket) {
        inBracket = false;
        segment.append(ch);
        continue;
      }
      if (ch == '.' && !inDoubleQuote && !inBacktick && !inBracket) {
        appendIdentifierSegment(result, segment);
        result.append('.');
        segment.setLength(0);
      } else {
        segment.append(ch);
      }
    }
    appendIdentifierSegment(result, segment);
    return result.toString();
  }

  private static void appendIdentifierSegment(StringBuilder target, StringBuilder segment) {
    String part = segment.toString().trim();
    if (part.isEmpty()) {
      return;
    }
    target.append(stripIdentifierQuotes(part));
  }

  private static String stripIdentifierQuotes(String identifier) {
    if (identifier == null) {
      return null;
    }
    String trimmed = identifier.trim();
    if (trimmed.length() >= 2) {
      if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("`") && trimmed.endsWith("`"))) {
        return trimmed.substring(1, trimmed.length() - 1);
      }
      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        return trimmed.substring(1, trimmed.length() - 1);
      }
    }
    return trimmed;
  }

  private static SourcePosition extractSourcePosition(SelectItem<?> item) {
    if (item == null) {
      return null;
    }
    SimpleNode node = item.getASTNode();
    if (node != null) {
      Token token = node.jjtGetFirstToken();
      if (token != null) {
        int line = token.beginLine;
        int column = token.beginColumn;
        if (line > 0 && column > 0) {
          return new SourcePosition(line, column);
        }
      }
    }
    return null;
  }

  private static Expression combineExpressions(Expression left, Expression right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    return new AndExpression(left, right);
  }

  // LIMIT
  private static int computeLimit(PlainSelect ps) {
    return resolveLimit(ps.getLimit(), ps.getFetch());
  }

  private static int computeOffset(PlainSelect ps) {
    Offset off = ps.getOffset();
    if (off != null && off.getOffset() != null) {
      Expression expr = off.getOffset();
      if (expr instanceof LongValue lv) {
        return lv.getBigIntegerValue().intValue();
      }
      try {
        return Integer.parseInt(expr.toString());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid OFFSET value: '" + expr + "'", e);
      }
    }
    return 0;
  }

  /**
   * Compute the {@link OrderKey}s (ORDER BY columns and sort directions) for a
   * parsed {@link PlainSelect} statement.
   *
   * <p>
   * This method normalizes and resolves ORDER BY expressions to their
   * corresponding physical column names, ensuring that sorting is performed on
   * actual data fields rather than aliases or qualified names. For example, in a
   * query like:
   * </p>
   *
   * <pre>{@code
   * SELECT mpg AS miles_per_gallon, model
   * FROM mtcars
   * ORDER BY miles_per_gallon DESC
   * }</pre>
   *
   * <p>
   * The ORDER BY expression {@code miles_per_gallon} is resolved to the
   * underlying physical column {@code mpg}, so that downstream components (such
   * as {@code QueryProcessor}) can correctly sort records using the actual field
   * name.
   * </p>
   *
   * <p>
   * The method also strips any table qualifiers (e.g. {@code t.col -> col}) for
   * single-table queries.
   * </p>
   *
   * @param ps
   *          the {@link PlainSelect} representing the parsed SQL statement
   * @param labels
   *          the projection labels (aliases or column names as exposed in the
   *          SELECT list)
   * @param physicalCols
   *          the underlying physical column names corresponding to {@code labels}
   * @param tableRefs
   *          table references participating in the query (used to normalise
   *          qualifiers)
   * @return a list of {@link OrderKey} objects representing the resolved ORDER BY
   *         keys; an empty list if the query has no ORDER BY clause
   */
  private static List<OrderKey> computeOrderKeys(PlainSelect ps, List<String> labels, List<String> physicalCols,
      List<TableReference> tableRefs) {
    List<OrderKey> orderKeys = new ArrayList<>();
    List<OrderByElement> ob = ps.getOrderByElements();
    if (ob != null) {
      // Build alias->physical map from the current projection lists we just built
      // - labels.get(i) = projection label (alias if present, else column name)
      // - physicalCols.get(i) = underlying physical column name (null if not a simple
      // column)
      Map<String, String> aliasToPhysical = new HashMap<>();
      for (int i = 0; i < labels.size(); i++) {
        String lab = labels.get(i);
        String phys = (i < physicalCols.size()) ? physicalCols.get(i) : null;
        if (lab != null && phys != null && !lab.isBlank() && !phys.isBlank()) {
          aliasToPhysical.put(lab, phys);
        }
      }

      for (OrderByElement e : ob) {
        Expression ex = e.getExpression();
        String qualifier = null;

        // normalize qualifiers (t.col -> col) for single-table
        if (tableRefs.size() == 1) {
          stripQualifier(ex, qualifiers(tableRefs));
        }

        // Extract the ORDER BY token as text
        String keyText;
        if (ex instanceof Column c) {
          keyText = c.getColumnName();
          if (tableRefs.size() > 1) {
            qualifier = extractQualifier(c);
          }
        } else if (ex instanceof StringValue sv) {
          keyText = sv.getValue();
        } else {
          keyText = ex.toString();
        }

        // If it's an alias used in SELECT, map to the physical column name
        String physicalKey = aliasToPhysical.getOrDefault(keyText, keyText);

        boolean asc = !e.isAscDescPresent() || e.isAsc(); // default ASC
        orderKeys.add(new OrderKey(physicalKey, asc, qualifier));
      }
    }
    return orderKeys;
  }

  private static String extractQualifier(Column column) {
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

  // === Qualifier Stripping =================================================

  /**
   * Remove SQL comments from the supplied query string while preserving string
   * literals.
   *
   * <p>
   * The parser used by the engine does not retain comments, but unstripped
   * comments can still interfere with parsing when they appear in leading or
   * inline positions. This helper processes the SQL text and strips both
   * {@code --} line comments and {@code /* ... *&#47;} block comments so that
   * {@link CCJSqlParserUtil} receives a clean statement. Content that appears
   * inside single-quoted or double-quoted literals is preserved verbatim.
   * </p>
   *
   * @param sql
   *          the raw SQL text that may contain comments
   * @return the SQL text with comments removed
   */
  @SuppressWarnings("PMD.CyclomaticComplexity")
  private static String stripSqlComments(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }

    StringBuilder result = new StringBuilder(sql.length());
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean inLineComment = false;
    boolean inBlockComment = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';

      if (inLineComment) {
        if (c == '\n') {
          result.append(c);
          inLineComment = false;
        } else if (c == '\r') {
          // Swallow carriage return but keep line comment state until newline
        }
        continue;
      }
      if (inBlockComment) {
        if (c == '\n') {
          result.append(c);
        }
        if (c == '*' && next == '/') {
          inBlockComment = false;
          i++;
        }
        continue;
      }

      if (inSingleQuote) {
        result.append(c);
        if (c == '\'' && next == '\'') {
          result.append(next);
          i++;
        } else if (c == '\'') {
          inSingleQuote = false;
        }
        continue;
      }
      if (inDoubleQuote) {
        result.append(c);
        if (c == '"' && next == '"') {
          result.append(next);
          i++;
        } else if (c == '"') {
          inDoubleQuote = false;
        }
        continue;
      }

      if (c == '-' && next == '-') {
        inLineComment = true;
        i++;
        continue;
      }
      if (c == '/' && next == '*') {
        inBlockComment = true;
        i++;
        continue;
      }
      if (c == '\'') {
        inSingleQuote = true;
        result.append(c);
        continue;
      }
      if (c == '"') {
        inDoubleQuote = true;
        result.append(c);
        continue;
      }

      result.append(c);
    }

    return result.toString();
  }

  /**
   * Strip the table qualifier for a single-table query when the qualifier matches
   * either the real table name or the declared alias—leaving Column as
   * unqualified. This keeps downstream evaluation simple (operate on column names
   * only).
   */
  private static void stripQualifier(Expression expr, List<String> allowedQualifiers) {
    if (expr == null) {
      return;
    }

    List<String> allowed = allowedQualifiers == null ? List.of() : allowedQualifiers;

    expr.accept(new ExpressionVisitorAdapter<Void>() {
      @Override
      public <S> Void visit(Column column, S context) {
        Table t = column.getTable();
        if (t != null) {
          String q = t.getName();
          if (q != null) {
            boolean known = allowed.stream().anyMatch(name -> name.equalsIgnoreCase(q));
            if (known) {
              column.setTable(null);
            }
          }
        }
        return super.visit(column, context);
      }

      @Override
      public <S> Void visit(ParenthesedSelect subSelect, S context) {
        // Preserve qualifiers in sub queries to support correlated references.
        // Intentionally do not traverse into the sub select.
        return null;
      }
    });
  }

  /**
   * Collect column names that reference any of the supplied qualifiers.
   *
   * @param expr
   *          the expression to inspect
   * @param qualifiers
   *          table names or aliases to match
   * @return a set of column names that reference one of the qualifiers
   */
  public static Set<String> collectQualifiedColumns(Expression expr, List<String> qualifiers) {
    if (expr == null || qualifiers == null || qualifiers.isEmpty()) {
      return Set.of();
    }
    Set<String> normalized = qualifiers.stream().map(JParqUtil::normalizeQualifier)
        .filter(s -> s != null && !s.isEmpty()).collect(Collectors.toCollection(LinkedHashSet::new));
    if (normalized.isEmpty()) {
      return Set.of();
    }
    Set<String> columns = new LinkedHashSet<>();
    expr.accept(new ExpressionVisitorAdapter<Void>() {
      @Override
      public <S> Void visit(Column column, S context) {
        Table table = column.getTable();
        if (table == null) {
          if (normalized.size() == 1) {
            columns.add(column.getColumnName());
          }
          return super.visit(column, context);
        } else {
          String[] candidates = {
              table.getUnquotedName(), table.getFullyQualifiedName(), table.getName()
          };
          for (String candidate : candidates) {
            String normalizedCandidate = JParqUtil.normalizeQualifier(candidate);
            if (normalizedCandidate != null && normalized.contains(normalizedCandidate)) {
              columns.add(column.getColumnName());
              break;
            }
          }
        }
        return super.visit(column, context);
      }

      @Override
      public <S> Void visit(AnyComparisonExpression any, S context) {
        net.sf.jsqlparser.statement.select.Select subSelect = any.getSelect();
        if (subSelect != null) {
          subSelect.accept(this, context);
        }
        return super.visit(any, context);
      }

      @Override
      public <S> Void visit(ParenthesedSelect select, S context) {
        CorrelatedSubqueryRewriter.Result rewritten = CorrelatedSubqueryRewriter.rewrite(select, qualifiers,
            name -> null);
        columns.addAll(rewritten.correlatedColumns());
        return super.visit(select, context);
      }

      @Override
      public <S> Void visit(net.sf.jsqlparser.statement.select.Select select, S context) {
        CorrelatedSubqueryRewriter.Result rewritten = CorrelatedSubqueryRewriter.rewrite(select, qualifiers,
            name -> null);
        columns.addAll(rewritten.correlatedColumns());
        return super.visit(select, context);
      }
    });
    return Set.copyOf(columns);
  }

  private static GroupByInfo parseGroupBy(GroupByElement groupBy, List<TableReference> tableRefs) {
    if (groupBy == null) {
      return new GroupByInfo(List.of(), List.of());
    }

    List<String> allowedQualifiers = tableRefs.size() == 1 ? qualifiers(tableRefs) : List.of();

    Map<String, Integer> indexByExpression = new LinkedHashMap<>();
    List<Expression> expressions = new ArrayList<>();
    List<Integer> baseGrouping = new ArrayList<>();
    List<Integer> rollupOrder = new ArrayList<>();
    List<Integer> cubeOrder = new ArrayList<>();
    boolean encounteredRollup = false;
    boolean encounteredCube = false;

    ExpressionList<?> expressionList = groupBy.getGroupByExpressionList();
    if (expressionList != null && !expressionList.isEmpty()) {
      for (Expression expr : expressionList) {
        if (expr instanceof Function func) {
          String name = func.getName();
          if ("ROLLUP".equalsIgnoreCase(name)) {
            if (encounteredRollup) {
              throw new IllegalArgumentException("Multiple ROLLUP clauses in GROUP BY are not supported");
            }
            if (encounteredCube) {
              throw new IllegalArgumentException("ROLLUP cannot be combined with CUBE in GROUP BY");
            }
            encounteredRollup = true;
            ExpressionList<?> parameters = func.getParameters();
            if (parameters == null || parameters.isEmpty()) {
              throw new IllegalArgumentException("ROLLUP requires at least one grouping expression");
            }
            if (!allowedQualifiers.isEmpty()) {
              for (Expression paramObj : parameters) {
                stripQualifier(paramObj, allowedQualifiers);
              }
            }
            for (Expression param : parameters) {
              int index = registerGroupingExpression(param, expressions, indexByExpression);
              rollupOrder.add(index);
            }
          } else if ("CUBE".equalsIgnoreCase(name)) {
            if (encounteredCube) {
              throw new IllegalArgumentException("Multiple CUBE clauses in GROUP BY are not supported");
            }
            if (encounteredRollup) {
              throw new IllegalArgumentException("CUBE cannot be combined with ROLLUP in GROUP BY");
            }
            encounteredCube = true;
            ExpressionList<?> parameters = func.getParameters();
            if (parameters == null || parameters.isEmpty()) {
              throw new IllegalArgumentException("CUBE requires at least one grouping expression");
            }
            if (!allowedQualifiers.isEmpty()) {
              for (Expression paramObj : parameters) {
                stripQualifier(paramObj, allowedQualifiers);
              }
            }
            for (Expression param : parameters) {
              int index = registerGroupingExpression(param, expressions, indexByExpression);
              cubeOrder.add(index);
            }
          } else {
            if (encounteredRollup || encounteredCube) {
              throw new IllegalArgumentException(
                  "Special grouping elements such as ROLLUP or CUBE must be the last items in GROUP BY");
            }
            if (!allowedQualifiers.isEmpty()) {
              stripQualifier(expr, allowedQualifiers);
            }
            int index = registerGroupingExpression(expr, expressions, indexByExpression);
            baseGrouping.add(index);
          }
        } else {
          if (encounteredRollup || encounteredCube) {
            throw new IllegalArgumentException(
                "Special grouping elements such as ROLLUP or CUBE must be the last items in GROUP BY");
          }
          if (!allowedQualifiers.isEmpty()) {
            stripQualifier(expr, allowedQualifiers);
          }
          int index = registerGroupingExpression(expr, expressions, indexByExpression);
          baseGrouping.add(index);
        }
      }
    }

    List<List<Integer>> groupingSets = new ArrayList<>();
    List<ExpressionList<Expression>> groupingSetElements = groupBy.getGroupingSets();
    if (encounteredRollup) {
      if (groupingSetElements != null && !groupingSetElements.isEmpty()) {
        throw new IllegalArgumentException("ROLLUP cannot be combined with GROUPING SETS");
      }
      for (int size = rollupOrder.size(); size >= 0; size--) {
        List<Integer> indexes = new ArrayList<>(baseGrouping);
        for (int i = 0; i < size; i++) {
          indexes.add(rollupOrder.get(i));
        }
        groupingSets.add(List.copyOf(indexes));
      }
    } else if (encounteredCube) {
      if (groupingSetElements != null && !groupingSetElements.isEmpty()) {
        throw new IllegalArgumentException("CUBE cannot be combined with GROUPING SETS");
      }
      List<Integer> cubeDimensions = resolveCubeDimensions(baseGrouping, cubeOrder);
      generateCubeGroupingSets(groupingSets, new ArrayList<>(baseGrouping), cubeDimensions, 0);
    } else if (groupingSetElements != null && !groupingSetElements.isEmpty()) {
      for (ExpressionList<Expression> groupingSet : groupingSetElements) {
        List<Integer> indexes = new ArrayList<>(baseGrouping);
        if (groupingSet != null && !groupingSet.isEmpty()) {
          for (Expression expr : groupingSet) {
            if (!allowedQualifiers.isEmpty()) {
              stripQualifier(expr, allowedQualifiers);
            }
            int index = registerGroupingExpression(expr, expressions, indexByExpression);
            indexes.add(index);
          }
        }
        groupingSets.add(List.copyOf(indexes));
      }
    } else if (!baseGrouping.isEmpty()) {
      groupingSets.add(List.copyOf(baseGrouping));
    }

    return new GroupByInfo(List.copyOf(expressions), List.copyOf(groupingSets));
  }

  private static int registerGroupingExpression(Expression expression, List<Expression> expressions,
      Map<String, Integer> indexByExpression) {
    String key = expression == null ? null : expression.toString();
    if (key == null) {
      throw new IllegalArgumentException("GROUP BY expression cannot be null");
    }
    Integer existing = indexByExpression.get(key);
    if (existing != null) {
      return existing;
    }
    int index = expressions.size();
    expressions.add(expression);
    indexByExpression.put(key, index);
    return index;
  }

  private static List<Integer> resolveCubeDimensions(List<Integer> baseGrouping, List<Integer> cubeOrder) {
    if (cubeOrder.isEmpty()) {
      return List.of();
    }
    Set<Integer> baseIndexes = Set.copyOf(baseGrouping);
    LinkedHashSet<Integer> filtered = new LinkedHashSet<>();
    for (Integer index : cubeOrder) {
      if (index == null || baseIndexes.contains(index)) {
        continue;
      }
      filtered.add(index);
    }
    return List.copyOf(filtered);
  }

  private static void generateCubeGroupingSets(List<List<Integer>> target, List<Integer> current,
      List<Integer> cubeOrder, int position) {
    if (position == cubeOrder.size()) {
      target.add(List.copyOf(current));
      return;
    }

    current.add(cubeOrder.get(position));
    generateCubeGroupingSets(target, current, cubeOrder, position + 1);
    current.removeLast();
    generateCubeGroupingSets(target, current, cubeOrder, position + 1);
  }

  private record GroupByInfo(List<Expression> expressions, List<List<Integer>> groupingSets) {
  }

  private record JoinInfo(FromInfo table, Expression condition, JoinType joinType, List<String> usingColumns) {
  }

  private static List<JoinInfo> parseJoins(List<Join> joins, List<CommonTableExpression> ctes,
      Map<String, CommonTableExpression> cteLookup, boolean allowQualifiedWildcards) {
    if (joins == null || joins.isEmpty()) {
      return List.of();
    }
    List<JoinInfo> joinInfos = new ArrayList<>(joins.size());
    for (Join join : joins) {
      if (join.isNatural()) {
        throw new IllegalArgumentException("NATURAL JOIN is not supported");
      }
      JoinType joinType;
      if (join.isFull()) {
        joinType = JoinType.FULL_OUTER;
      } else if (join.isRight() || (join.isOuter() && join.isRight())) {
        joinType = JoinType.RIGHT_OUTER;
      } else if (join.isLeft() || join.isOuter()) {
        joinType = JoinType.LEFT_OUTER;
      } else if (join.isCross() || join.isSimple()) {
        joinType = JoinType.CROSS;
      } else {
        joinType = JoinType.INNER;
      }
      if (joinType == JoinType.CROSS && join.getOnExpressions() != null && !join.getOnExpressions().isEmpty()) {
        throw new IllegalArgumentException("CROSS JOIN cannot specify an ON condition");
      }
      if (joinType == JoinType.CROSS && join.getUsingColumns() != null && !join.getUsingColumns().isEmpty()) {
        throw new IllegalArgumentException("CROSS JOIN cannot specify a USING clause");
      }
      if (join.isOuter() && joinType != JoinType.LEFT_OUTER && joinType != JoinType.RIGHT_OUTER
          && joinType != JoinType.FULL_OUTER) {
        throw new IllegalArgumentException("Unsupported outer join type");
      }
      FromItem rightItem = join.getRightItem();
      boolean joinLateral = rightItem instanceof LateralSubSelect;
      if (!joinLateral && rightItem instanceof TableFunction tableFunction) {
        joinLateral = "lateral".equalsIgnoreCase(tableFunction.getPrefix());
      }
      FromInfo info = parseFromItem(rightItem, ctes, cteLookup, allowQualifiedWildcards, joinLateral);
      Expression condition = null;
      if (join.getOnExpressions() != null) {
        for (Expression on : join.getOnExpressions()) {
          condition = combineExpressions(condition, on);
        }
      }
      List<String> usingColumns = parseUsingColumns(join);
      if (!usingColumns.isEmpty() && condition != null) {
        throw new IllegalArgumentException("JOIN cannot specify both USING and ON clauses");
      }
      joinInfos.add(new JoinInfo(info, condition, joinType, usingColumns));
    }
    return List.copyOf(joinInfos);
  }

  private static List<TableReference> buildTableReferences(FromInfo base, List<JoinInfo> joins) {
    List<TableReference> refs = new ArrayList<>();
    refs.add(new TableReference(base.tableName(), base.tableAlias(), JoinType.BASE, null, base.innerSelect(),
        base.subquerySql(), base.commonTableExpression(), List.of(), base.valueTable(), base.unnest(), base.lateral()));
    if (joins != null) {
      for (JoinInfo join : joins) {
        FromInfo info = join.table();
        Expression usingCondition = buildUsingCondition(join.usingColumns(), info);
        Expression combinedCondition = combineExpressions(join.condition(), usingCondition);
        refs.add(new TableReference(info.tableName(), info.tableAlias(), join.joinType(), combinedCondition,
            info.innerSelect(), info.subquerySql(), info.commonTableExpression(), join.usingColumns(),
            info.valueTable(), info.unnest(), info.lateral()));
      }
    }
    return List.copyOf(refs);
  }

  /**
   * Build an {@code ON} expression equivalent for a {@code USING} clause by
   * comparing identically named columns between the accumulated join input and
   * the new join participant.
   *
   * @param usingColumns
   *          the column names supplied via {@code USING}
   * @param join
   *          metadata describing the right-hand table of the join
   * @return an equality expression that enforces the {@code USING} semantics, or
   *         {@code null} when no {@code USING} columns are present
   */
  private static Expression buildUsingCondition(List<String> usingColumns, FromInfo join) {
    if (usingColumns == null || usingColumns.isEmpty()) {
      return null;
    }
    String rightQualifier = join.tableAlias();
    if (rightQualifier == null || rightQualifier.isBlank()) {
      rightQualifier = join.tableName();
    }
    Expression combined = null;
    for (String columnName : usingColumns) {
      if (columnName == null || columnName.isBlank()) {
        continue;
      }
      Column leftColumn = new Column();
      leftColumn.setColumnName(columnName);

      Column rightColumn = new Column();
      if (rightQualifier != null && !rightQualifier.isBlank()) {
        Table table = new Table();
        table.setName(rightQualifier);
        rightColumn.setTable(table);
      }
      rightColumn.setColumnName(columnName);

      EqualsTo equals = new EqualsTo(leftColumn, rightColumn);
      combined = combineExpressions(combined, equals);
    }
    return combined;
  }

  /**
   * Extract column names from a {@code USING} clause definition.
   *
   * @param join
   *          the join definition parsed by JSqlParser
   * @return immutable list of column names referenced by {@code USING}
   */
  private static List<String> parseUsingColumns(Join join) {
    if (join == null || join.getUsingColumns() == null || join.getUsingColumns().isEmpty()) {
      return List.of();
    }
    List<String> columns = new ArrayList<>(join.getUsingColumns().size());
    for (Column column : join.getUsingColumns()) {
      if (column == null || column.getColumnName() == null) {
        continue;
      }
      columns.add(column.getColumnName());
    }
    return List.copyOf(columns);
  }

  private static List<String> qualifiers(List<TableReference> tableRefs) {
    if (tableRefs == null || tableRefs.isEmpty()) {
      return List.of();
    }
    List<String> qualifiers = new ArrayList<>();
    for (TableReference ref : tableRefs) {
      if (ref.tableName() != null && !ref.tableName().isBlank()) {
        qualifiers.add(ref.tableName());
      }
      if (ref.tableAlias() != null && !ref.tableAlias().isBlank()) {
        qualifiers.add(ref.tableAlias());
      }
    }
    return qualifiers;
  }

  private static Expression combineJoinConditions(List<JoinInfo> joinInfos, List<TableReference> tableRefs) {
    if (joinInfos == null || joinInfos.isEmpty()) {
      return null;
    }
    Expression combined = null;
    List<String> qualifierList = qualifiers(tableRefs);
    for (JoinInfo join : joinInfos) {
      if (join.joinType() == JoinType.LEFT_OUTER || join.joinType() == JoinType.RIGHT_OUTER
          || join.joinType() == JoinType.FULL_OUTER) {
        continue;
      }
      Expression condition = join.condition();
      if (condition != null) {
        if (tableRefs.size() == 1) {
          stripQualifier(condition, qualifierList);
        }
        combined = combineExpressions(combined, condition);
      }
    }
    return combined;
  }

}
