package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

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
   * @param distinct
   *          true if SELECT DISTINCT is used
   */
  public record Select(List<String> labels, List<String> columnNames, String table, String tableAlias, Expression where,
      int limit, List<OrderKey> orderBy, boolean distinct) {
    /**
     * Returns "\*" if no explicit projection.
     *
     * @return the selected columns or ["\*"] if select all
     */
    public List<String> columns() {
      if (columnNames == null || columnNames.isEmpty()) {
        return List.of("*");
      } else {
        return columnNames;
      }
    }

    /**
     * ORDER BY keys (never null).
     *
     * @return the order by keys
     */
    public List<OrderKey> orderBy() {
      return orderBy;
    }
  }

  private record FromInfo(String tableName, String tableAlias) {
  }

  /**
   * Parse a simple SELECT SQL statement (single-table).
   *
   * @param sql
   *          the SQL string
   * @return the parsed Select descriptor
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static Select parseSelect(String sql) {
    try {
      net.sf.jsqlparser.statement.select.Select stmt = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil
          .parse(sql);
      PlainSelect ps = stmt.getPlainSelect();

      boolean isDistinct = ps.getDistinct() != null;

      FromInfo fromInfo = parseFromItem(ps.getFromItem());

      Projection projection = parseProjectionList(ps.getSelectItems(), fromInfo);

      Expression whereExpr = ps.getWhere();
      stripQualifier(whereExpr, fromInfo.tableName(), fromInfo.tableAlias());

      int limit = computeLimit(ps);

      List<OrderKey> orderKeys = computeOrderKeys(ps, projection.labels(), projection.physicalCols(),
          fromInfo.tableName(), fromInfo.tableAlias());

      validateDistinctOrderBy(isDistinct, orderKeys, projection);

      return new Select(List.copyOf(projection.labels()), List.copyOf(projection.physicalCols()), fromInfo.tableName(),
          fromInfo.tableAlias(), whereExpr, limit, List.copyOf(orderKeys), isDistinct);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse SQL: " + sql, e);
    }
  }

  private record Projection(List<String> labels, List<String> physicalCols) {
  }

  private static FromInfo parseFromItem(FromItem fromItem) {
    if (!(fromItem instanceof Table t)) {
      throw new IllegalArgumentException("Only single-table SELECT is supported");
    }
    String tableName = t.getName();
    String tableAlias = t.getAlias() != null ? t.getAlias().getName() : null;
    return new FromInfo(tableName, tableAlias);
  }

  private static Projection parseProjectionList(List<SelectItem<?>> selectItems, FromInfo fromInfo) {
    List<String> labels = new ArrayList<>();
    List<String> physicalCols = new ArrayList<>();

    boolean selectAll = false;
    for (SelectItem<?> item : selectItems) {
      String text = item.toString().trim();

      if ("*".equals(text)) {
        selectAll = true;
        break;
      }
      if (text.endsWith(".*")) {
        throw new IllegalArgumentException("Qualified * (table.*) not supported yet: " + text);
      }

      Expression expr = item.getExpression();
      stripQualifier(expr, fromInfo.tableName(), fromInfo.tableAlias());

      Alias aliasObj = item.getAlias();
      String alias = aliasObj == null ? null : aliasObj.getName();

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
      labels = List.of();
      physicalCols = List.of();
    }

    return new Projection(labels, physicalCols);
  }

  private static int computeLimit(PlainSelect ps) {
    Limit lim = ps.getLimit();
    if (lim != null && lim.getRowCount() != null) {
      Expression rowCountExpr = lim.getRowCount();
      if (rowCountExpr instanceof LongValue lv) {
        return lv.getBigIntegerValue().intValue();
      } else {
        return Integer.parseInt(rowCountExpr.toString());
      }
    }
    return -1;
  }

  private static List<OrderKey> computeOrderKeys(PlainSelect ps, List<String> labels, List<String> physicalCols,
      String tableName, String tableAlias) {
    List<OrderKey> orderKeys = new ArrayList<>();
    List<OrderByElement> ob = ps.getOrderByElements();
    if (ob != null) {
      Map<String, String> aliasToPhysical = new HashMap<>();
      for (int i = 0; i < labels.size(); i++) {
        String lab = labels.get(i);
        String phys = i < physicalCols.size() ? physicalCols.get(i) : null;
        if (lab != null && phys != null && !lab.isBlank() && !phys.isBlank()) {
          aliasToPhysical.put(lab, phys);
        }
      }

      for (OrderByElement e : ob) {
        Expression ex = e.getExpression();

        stripQualifier(ex, tableName, tableAlias);

        String keyText;
        if (ex instanceof Column c) {
          keyText = c.getColumnName();
        } else if (ex instanceof StringValue sv) {
          keyText = sv.getValue();
        } else {
          keyText = ex.toString();
        }

        String physicalKey = aliasToPhysical.getOrDefault(keyText, keyText);

        boolean asc = !e.isAscDescPresent() || e.isAsc();
        orderKeys.add(new OrderKey(physicalKey, asc));
      }
    }
    return orderKeys;
  }

  private static void validateDistinctOrderBy(boolean isDistinct, List<OrderKey> orderKeys, Projection projection) {
    if (!isDistinct || orderKeys == null || orderKeys.isEmpty()) {
      return;
    }

    boolean isSelectAll = projection.labels().isEmpty() && projection.physicalCols().isEmpty();
    if (isSelectAll) {
      return;
    }

    Set<String> allowed = new HashSet<>();
    allowed.addAll(projection.labels());
    for (String phys : projection.physicalCols()) {
      if (phys != null) {
        allowed.add(phys);
      }
    }

    for (OrderKey ok : orderKeys) {
      if (!allowed.contains(ok.column())) {
        throw new IllegalArgumentException(
            "ORDER BY column '" + ok.column() + "' must appear in the SELECT list when DISTINCT is used");
      }
    }
  }

  /**
   * Strip the table qualifier for a single-table query when the qualifier matches
   * either the real table name or the declared alias—leaving Column as
   * unqualified.
   *
   * @param expr
   *          the expression to normalize
   * @param tableName
   *          the actual table name
   * @param tableAlias
   *          the optional table alias
   */
  private static void stripQualifier(Expression expr, String tableName, String tableAlias) {
    if (expr == null) {
      return;
    }

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
              column.setTable(null);
            } else {
              throw new IllegalArgumentException("Unknown table qualifier '" + q + "' for single-table SELECT");
            }
          }
        }
      }
    });
  }
}
