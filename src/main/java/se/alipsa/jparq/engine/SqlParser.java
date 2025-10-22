package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
    /** returns "*" if no explicit projection. */
    public List<String> columns() {
      return (columnNames == null || columnNames.isEmpty()) ? List.of("*") : columnNames;
    }
    /** ORDER BY keys (never null). */
    public List<OrderKey> orderBy() {
      return orderBy == null ? List.of() : orderBy;
    }
  }

  /** Parse a simple SELECT SQL statement (single-table). */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static Select parseSelect(String sql) {
    try {
      net.sf.jsqlparser.statement.select.Select stmt = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil
          .parse(sql);
      PlainSelect ps = stmt.getPlainSelect();

      // FROM: single table only
      if (!(ps.getFromItem() instanceof Table t)) {
        throw new IllegalArgumentException("Only single-table SELECT is supported");
      }
      final String tableName = t.getName();
      final String tableAlias = (t.getAlias() != null) ? t.getAlias().getName() : null;

      // SELECT list: support column aliases and *
      List<String> labels = new ArrayList<>();
      List<String> physicalCols = new ArrayList<>();

      // For ORDER BY alias support: alias -> expression (track what user aliased)
      Map<String, Expression> selectAliasToExpr = new LinkedHashMap<>();

      boolean selectAll = false;
      for (SelectItem<?> item : ps.getSelectItems()) {
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
        stripQualifier(expr, tableName, tableAlias);

        final Alias aliasObj = item.getAlias();
        final String alias = aliasObj == null ? null : aliasObj.getName();

        String label;
        String underlying = null;

        if (expr instanceof Column col) {
          underlying = col.getColumnName();
          if (alias != null && !alias.isEmpty()) {
            label = alias;
            selectAliasToExpr.put(alias, col);
          } else {
            label = underlying;
          }
        } else {
          // Keep label stable; allow alias for projection labeling.
          if (alias != null && !alias.isEmpty()) {
            label = alias;
            selectAliasToExpr.put(alias, expr);
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

      // WHERE: keep raw Expression but strip qualifiers (single-table)
      Expression whereExpr = ps.getWhere();
      stripQualifier(whereExpr, tableName, tableAlias);

      // LIMIT
      int limit = -1;
      Limit lim = ps.getLimit();
      if (lim != null && lim.getRowCount() != null) {
        limit = Integer.parseInt(lim.getRowCount().toString());
      }

      // ORDER BY (resolve alias -> physical column name so QueryProcessor sees
      // physical keys)
      List<OrderKey> orderKeys = new ArrayList<>();
      List<OrderByElement> ob = ps.getOrderByElements();
      if (ob != null) {
        // Build alias->physical map from the current projection lists we just built
        // - labels.get(i) = projection label (alias if present, else column name)
        // - physicalCols.get(i) = underlying physical column name (null if not a simple
        // column)
        Map<String, String> aliasToPhysical = new java.util.HashMap<>();
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

      return new Select(List.copyOf(labels), List.copyOf(physicalCols), tableName, tableAlias, whereExpr, limit,
          List.copyOf(orderKeys));
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse SQL: " + sql, e);
    }
  }

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

  @SuppressWarnings({
      "PMD.AvoidDecimalLiteralsInBigDecimalConstructor", "PMD.EmptyCatchBlock", "checkstyle:NeedBraces"
  })
  static Object toLiteral(Expression e) {
    if (e instanceof NullValue) {
      return null;
    }
    if (e instanceof StringValue sv) {
      return sv.getValue();
    }
    if (e instanceof LongValue lv) {
      return lv.getBigIntegerValue().longValue();
    }
    if (e instanceof DoubleValue dv) {
      return new BigDecimal(dv.getValue());
    }
    if (e instanceof SignedExpression se) {
      return toSignedLiteral(se);
    }
    if (e instanceof BooleanValue bv) {
      return bv.getValue();
    }
    if (e instanceof DateValue dv) {
      return dv.getValue(); // java.sql.Date
    }
    if (e instanceof TimestampValue tv) {
      return tv.getValue(); // java.sql.Timestamp
    }
    try {
      return new BigDecimal(e.toString());
    } catch (Exception ignore) {
    }
    return e.toString();
  }

  @SuppressWarnings("PMD.EmptyCatchBlock")
  private static Object toSignedLiteral(SignedExpression se) {
    Object inner = toLiteral(se.getExpression());
    if (inner instanceof Number n) {
      var bd = new BigDecimal(n.toString());
      return se.getSign() == '-' ? bd.negate() : bd;
    }
    if (inner instanceof String s) {
      try {
        var bd = new BigDecimal(s);
        return se.getSign() == '-' ? bd.negate() : bd;
      } catch (Exception ignore) {
      }
    }
    return (se.getSign() == '-') ? ("-" + inner) : inner;
  }
}
