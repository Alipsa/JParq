package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.*;
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
   *          the column name
   * @param asc
   *          true if ascending order, false if descending
   */
  public record OrderKey(String column, boolean asc) {
  }

  /**
   * A simple representation of a SELECT statement.
   *
   * @param columns
   *          the selected columns
   * @param table
   *          the table name
   * @param where
   *          the where expression
   * @param limit
   *          the limit value (-1 if none)
   * @param orderBy
   *          ORDER BY keys (empty if none)
   */
  public record Select(List<String> columns, String table, Expression where, int limit, List<OrderKey> orderBy) {

    /** Get the columns, or * if none specified. */
    public List<String> columns() {
      return (columns == null || columns.isEmpty()) ? List.of("*") : columns;
    }

    /** ORDER BY keys (never null). */
    public List<OrderKey> orderBy() {
      return orderBy == null ? List.of() : orderBy;
    }
  }

  /** Parse a simple SELECT SQL statement. */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static Select parseSelect(String sql) {
    try {
      net.sf.jsqlparser.statement.select.Select stmt = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil
          .parse(sql);
      PlainSelect ps = stmt.getPlainSelect();

      // FROM: single table only
      if (!(ps.getFromItem() instanceof Table table)) {
        throw new IllegalArgumentException("Only single-table SELECT is supported");
      }

      // SELECT list (JSQLParser 5.3)
      List<String> columns = new ArrayList<>();
      boolean selectAll = false;
      for (SelectItem<?> item : ps.getSelectItems()) {
        String text = item.toString().trim();
        if ("*".equals(text)) {
          selectAll = true;
          break;
        }
        if (text.endsWith(".*")) {
          throw new IllegalArgumentException("Qualified * (table.*) not supported yet: " + text);
        }
        Expression expr = item.getExpression();
        if (expr instanceof Column col) {
          columns.add(col.getColumnName());
        } else if (expr instanceof AllColumns) {
          selectAll = true;
          break;
        } else {
          throw new IllegalArgumentException("Only simple column projections are supported: " + text);
        }
      }
      if (selectAll) {
        columns = List.of("*");
      }

      // WHERE: keep raw Expression (enables OR, parentheses, etc.)
      Expression whereExpr = ps.getWhere();

      // LIMIT
      int limit = -1;
      Limit lim = ps.getLimit();
      if (lim != null && lim.getRowCount() != null) {
        limit = Integer.parseInt(lim.getRowCount().toString());
      }

      // ORDER BY
      List<OrderKey> orderKeys = new ArrayList<>();
      List<OrderByElement> ob = ps.getOrderByElements();
      if (ob != null) {
        for (OrderByElement e : ob) {
          Expression ex = e.getExpression();
          if (!(ex instanceof Column col)) {
            throw new IllegalArgumentException("ORDER BY supports simple column names only: " + ex);
          }
          // JSQLParser 5.3: default ASC if no ASC/DESC specified
          boolean asc = !e.isAscDescPresent() || e.isAsc();
          orderKeys.add(new OrderKey(col.getColumnName(), asc));
        }
      }

      return new Select(columns, table.getName(), whereExpr, limit, List.copyOf(orderKeys));
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse SQL: " + sql, e);
    }
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
