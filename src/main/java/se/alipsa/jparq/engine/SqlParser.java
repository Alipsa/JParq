package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
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
   * ORDER BY key (simple: column + direction).
   *
   * @param column
   *          the column name (unqualified, physical column name)
   * @param asc
   *          true if ascending order, false if descending
   */
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
   * @param groupByExpressions
   *          expressions that participate in the GROUP BY clause (empty if no
   *          grouping)
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
   */
  public record Select(List<String> labels, List<String> columnNames, String table, String tableAlias, Expression where,
      int limit, int offset, List<OrderKey> orderBy, boolean distinct, boolean innerDistinct,
      List<String> innerDistinctColumns, List<Expression> expressions, List<Expression> groupByExpressions,
      Expression having, int preLimit, int preOffset, List<OrderKey> preOrderBy, List<TableReference> tableReferences) {

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
   * Internal record to hold table name and alias.
   *
   * @param tableName
   *          the table name
   * @param tableAlias
   *          the table alias (may be null)
   * @return the FromInfo record
   */
  private record FromInfo(String tableName, String tableAlias, Map<String, String> columnMapping, Select innerSelect) {
  }

  /**
   * Public representation of a table reference discovered in the {@code FROM}
   * clause.
   *
   * @param tableName
   *          the referenced table name
   * @param tableAlias
   *          optional alias used for the table (may be {@code null})
   */
  public record TableReference(String tableName, String tableAlias) {
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
    try {
      String normalizedSql = stripSqlComments(sql);
      net.sf.jsqlparser.statement.select.Select stmt = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil
          .parse(normalizedSql);
      PlainSelect ps = stmt.getPlainSelect();
      return parsePlainSelect(ps);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse SQL: " + sql, e);
    }
  }

  private static Select parsePlainSelect(PlainSelect ps) {
    FromInfo fromInfo = parseFromItem(ps.getFromItem());
    List<JoinInfo> joinInfos = parseJoins(ps.getJoins());
    List<TableReference> tableRefs = buildTableReferences(fromInfo, joinInfos);
    final Projection projection = parseProjectionList(ps.getSelectItems(), fromInfo, tableRefs);

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

    if (projection.selectAll()) {
      if (fromInfo.innerSelect() != null) {
        labels = fromInfo.innerSelect().labels();
        physicalCols = fromInfo.innerSelect().columnNames();
        expressions = fromInfo.innerSelect().expressions();
      } else {
        labels = List.of();
        physicalCols = List.of();
        expressions = List.of();
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

    List<Expression> groupByExpressions = parseGroupBy(ps.getGroupBy(), tableRefs);

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
    List<String> innerDistinctCols = innerDistinct && inner != null ? List.copyOf(inner.columnNames()) : List.of();

    return new Select(labelsCopy, physicalCopy, fromInfo.tableName(), fromInfo.tableAlias(), combinedWhere, limit,
        offset, orderCopy, distinct, innerDistinct, innerDistinctCols, expressionCopy, groupByExpressions,
        combinedHaving, preLimit, preOffset, preOrderCopy, tableRefs);
  }

  // === Parsing Helper Methods =================================================

  /**
   * Internal record to hold the projection (SELECT list) results.
   */
  private record Projection(List<String> labels, List<String> physicalCols, List<Expression> expressions,
      boolean selectAll) {
  }

  /**
   * Parses the FROM clause, ensuring it's a single table and extracts its
   * name/alias.
   */
  private static FromInfo parseFromItem(FromItem fromItem) {
    if (fromItem instanceof Table t) {
      String tableName = t.getName();
      String tableAlias = (t.getAlias() != null) ? t.getAlias().getName() : null;
      return new FromInfo(tableName, tableAlias, Map.of(), null);
    }
    if (fromItem instanceof net.sf.jsqlparser.statement.select.Select sub) {
      PlainSelect innerPlain = sub.getPlainSelect();
      if (innerPlain == null) {
        throw new IllegalArgumentException("Only plain SELECT subqueries are supported");
      }
      Select innerSelect = parsePlainSelect(innerPlain);
      String alias = sub.getAlias() != null ? sub.getAlias().getName() : innerSelect.tableAlias();
      if (alias == null) {
        alias = innerSelect.table();
      }
      Map<String, String> mapping = buildColumnMapping(innerSelect);
      return new FromInfo(innerSelect.table(), alias, mapping, innerSelect);
    }
    throw new IllegalArgumentException("Unsupported FROM item: " + fromItem);
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
      if (label != null && phys != null) {
        mapping.put(label, phys);
      }
    }
    return Map.copyOf(mapping);
  }

  /**
   * Parses the SELECT list, resolving aliases and determining physical column
   * names.
   */
  private static Projection parseProjectionList(List<SelectItem<?>> selectItems, FromInfo fromInfo,
      List<TableReference> tableRefs) {
    List<String> labels = new ArrayList<>();
    List<String> physicalCols = new ArrayList<>();
    List<Expression> expressions = new ArrayList<>();

    boolean selectAll = false;
    for (SelectItem<?> item : selectItems) {
      final String text = item.toString().trim();

      if ("*".equals(text)) {
        selectAll = true;
        break;
      }
      if (text.endsWith(".*")) {
        throw new IllegalArgumentException("Qualified * (table.*) not supported yet: " + text);
      }

      final Expression expr = item.getExpression();
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
      }

      labels.add(label);
      physicalCols.add(underlying);
      expressions.add(expr);
    }

    if (selectAll) {
      labels = List.of();
      physicalCols = List.of();
      expressions = List.of();
    }

    return new Projection(labels, physicalCols, expressions, selectAll);
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
    Limit lim = ps.getLimit();
    if (lim != null && lim.getRowCount() != null) {
      // NOTE: Using LongValue instead of String.toString() for robustness
      Expression rowCountExpr = lim.getRowCount();
      if (rowCountExpr instanceof LongValue lv) {
        return lv.getBigIntegerValue().intValue();
      }
      // Fallback for non-LongValue expressions, though rare for LIMIT
      return Integer.parseInt(rowCountExpr.toString());
    }
    return -1;
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
            } else {
              throw new IllegalArgumentException("Unknown table qualifier '" + q + "'");
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
        if (table != null) {
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

  private static List<Expression> parseGroupBy(GroupByElement groupBy, List<TableReference> tableRefs) {
    ExpressionList<?> expressionList = groupBy == null ? null : groupBy.getGroupByExpressionList();
    if (expressionList == null || expressionList.isEmpty()) {
      return List.of();
    }
    List<Expression> expressions = new ArrayList<>(expressionList.size());
    for (Expression expr : expressionList) {
      if (tableRefs.size() == 1) {
        stripQualifier(expr, qualifiers(tableRefs));
      }
      expressions.add(expr);
    }
    return List.copyOf(expressions);
  }

  private record JoinInfo(FromInfo table, Expression condition, boolean simple) {
  }

  private static List<JoinInfo> parseJoins(List<Join> joins) {
    if (joins == null || joins.isEmpty()) {
      return List.of();
    }
    List<JoinInfo> joinInfos = new ArrayList<>(joins.size());
    for (Join join : joins) {
      boolean simple = join.isSimple();
      boolean inner = join.isInner();
      if (!simple && !inner) {
        throw new IllegalArgumentException("Only INNER JOIN is supported");
      }
      if (join.isOuter() || join.isLeft() || join.isRight() || join.isFull()) {
        throw new IllegalArgumentException("Only INNER JOIN is supported");
      }
      FromInfo info = parseFromItem(join.getRightItem());
      Expression condition = null;
      if (join.getOnExpressions() != null) {
        for (Expression on : join.getOnExpressions()) {
          condition = combineExpressions(condition, on);
        }
      }
      joinInfos.add(new JoinInfo(info, condition, simple));
    }
    return List.copyOf(joinInfos);
  }

  private static List<TableReference> buildTableReferences(FromInfo base, List<JoinInfo> joins) {
    List<TableReference> refs = new ArrayList<>();
    refs.add(new TableReference(base.tableName(), base.tableAlias()));
    if (joins != null) {
      for (JoinInfo join : joins) {
        FromInfo info = join.table();
        refs.add(new TableReference(info.tableName(), info.tableAlias()));
      }
    }
    return List.copyOf(refs);
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
