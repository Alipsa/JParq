package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

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
  public record OrderKey(String column, boolean asc) {
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
   * @param orderBy
   *          ORDER BY keys (empty if none)
   */
  public record Select(List<String> labels, List<String> columnNames, String table, String tableAlias, Expression where,
      int limit, List<OrderKey> orderBy) {

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
      // NOTE: Removed the redundant 'orderBy == null ? List.of() : orderBy' check
      // as parseSelect already ensures List.copyOf() which prevents null.
      return orderBy;
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
  private record FromInfo(String tableName, String tableAlias) {
  }

  /**
   * Parse a simple SELECT SQL statement (single-table).
   *
   * @param sql
   *          the SQL string
   * @return the parsed Select representation
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static Select parseSelect(String sql) {
    try {
      net.sf.jsqlparser.statement.select.Select stmt = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil
          .parse(sql);
      PlainSelect ps = stmt.getPlainSelect();

      // 1. FROM: Get the table name and alias
      FromInfo fromInfo = parseFromItem(ps.getFromItem());

      // 2. SELECT list: Get labels and physical columns
      Projection projection = parseProjectionList(ps.getSelectItems(), fromInfo);

      // 3. WHERE: Strip qualifiers for single-table queries
      Expression whereExpr = ps.getWhere();
      stripQualifier(whereExpr, fromInfo.tableName(), fromInfo.tableAlias());

      // 4. LIMIT
      int limit = computeLimit(ps);

      // 5. ORDER BY: Compute and resolve to physical columns
      List<OrderKey> orderKeys = computeOrderKeys(ps, projection.labels(), projection.physicalCols(),
          fromInfo.tableName(), fromInfo.tableAlias());

      return new Select(List.copyOf(projection.labels()), List.copyOf(projection.physicalCols()), fromInfo.tableName(),
          fromInfo.tableAlias(), whereExpr, limit, List.copyOf(orderKeys));
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse SQL: " + sql, e);
    }
  }

  // === Parsing Helper Methods =================================================

  /**
   * Internal record to hold the projection (SELECT list) results.
   */
  private record Projection(List<String> labels, List<String> physicalCols) {
  }

  /**
   * Parses the FROM clause, ensuring it's a single table and extracts its
   * name/alias.
   */
  private static FromInfo parseFromItem(FromItem fromItem) {
    if (!(fromItem instanceof Table t)) {
      throw new IllegalArgumentException("Only single-table SELECT is supported");
    }
    final String tableName = t.getName();
    final String tableAlias = (t.getAlias() != null) ? t.getAlias().getName() : null;
    return new FromInfo(tableName, tableAlias);
  }

  /**
   * Parses the SELECT list, resolving aliases and determining physical column
   * names.
   */
  private static Projection parseProjectionList(List<SelectItem<?>> selectItems, FromInfo fromInfo) {
    List<String> labels = new ArrayList<>();
    List<String> physicalCols = new ArrayList<>();

    boolean selectAll = false;
    for (SelectItem<?> item : selectItems) {
      final String text = item.toString().trim();

      // Handle SELECT * and reject table.*
      if ("*".equals(text)) {
        selectAll = true;
        break;
      }
      if (text.endsWith(".*")) {
        throw new IllegalArgumentException("Qualified * (table.*) not supported yet: " + text);
      }

      final Expression expr = item.getExpression(); // JSqlParser 5.x
      // Normalize qualifiers for single-table: t.col -> col
      stripQualifier(expr, fromInfo.tableName(), fromInfo.tableAlias());

      final Alias aliasObj = item.getAlias();
      final String alias = aliasObj == null ? null : aliasObj.getName();

      String label;
      String underlying = null;

      if (expr instanceof Column col) {
        underlying = col.getColumnName();
        if (alias != null && !alias.isEmpty()) {
          label = alias;
        } else {
          label = underlying;
        }
      } else {
        // Keep label stable; allow alias for projection labeling.
        if (alias != null && !alias.isEmpty()) {
          label = alias;
        } else {
          label = expr.toString();
        }
      }

      labels.add(label);
      physicalCols.add(underlying);
    }

    if (selectAll) {
      // Defer to engine to expand to actual columns; keep old contract: columns() ==
      // ["*"]
      labels = List.of(); // empty list => columns() returns ["*"]
      physicalCols = List.of(); // keep in sync
    }

    return new Projection(labels, physicalCols);
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
   * @param tableName
   *          the table name in the FROM clause
   * @param tableAlias
   *          the alias of the table, if any (may be {@code null})
   * @return a list of {@link OrderKey} objects representing the resolved ORDER BY
   *         keys; an empty list if the query has no ORDER BY clause
   */
  private static List<OrderKey> computeOrderKeys(PlainSelect ps, List<String> labels, List<String> physicalCols,
      String tableName, String tableAlias) {
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

        // normalize qualifiers (t.col -> col) for single-table
        stripQualifier(ex, tableName, tableAlias);

        // Extract the ORDER BY token as text
        String keyText;
        if (ex instanceof Column c) {
          keyText = c.getColumnName();
        } else if (ex instanceof StringValue sv) {
          keyText = sv.getValue();
        } else {
          keyText = ex.toString();
        }

        // If it's an alias used in SELECT, map to the physical column name
        String physicalKey = aliasToPhysical.getOrDefault(keyText, keyText);

        boolean asc = !e.isAscDescPresent() || e.isAsc(); // default ASC
        orderKeys.add(new OrderKey(physicalKey, asc));
      }
    }
    return orderKeys;
  }

  // === Qualifier Stripping =================================================

  /**
   * Strip the table qualifier for a single-table query when the qualifier matches
   * either the real table name or the declared alias—leaving Column as
   * unqualified. This keeps downstream evaluation simple (operate on column names
   * only).
   */
  private static void stripQualifier(Expression expr, String tableName, String tableAlias) {
    if (expr == null) {
      return;
    }

    // Allow both the real table name and alias (if any)
    List<String> allowed = new ArrayList<>();
    allowed.add(tableName);
    if (tableAlias != null && !tableAlias.isBlank()) {
      allowed.add(tableAlias);
    }

    expr.accept(new ExpressionVisitorAdapter<Void>() {
      @Override
      public void visit(Column column) {
        Table t = column.getTable();
        if (t != null) {
          String q = t.getName();
          if (q != null) {
            boolean known = allowed.stream().anyMatch(name -> name.equalsIgnoreCase(q));
            if (known) {
              column.setTable(null); // drop qualifier for single-table queries
            } else {
              throw new IllegalArgumentException("Unknown table qualifier '" + q + "' for single-table SELECT");
            }
          }
        }
        // no need to call super.visit(column)
      }
    });
  }

}