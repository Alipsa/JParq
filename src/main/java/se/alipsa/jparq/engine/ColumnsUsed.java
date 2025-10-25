package se.alipsa.jparq.engine;

import java.util.LinkedHashSet;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/** Collects column names referenced by an expression. */
public final class ColumnsUsed {

  private ColumnsUsed() {
  }

  /**
   * Extract all column names referenced in a SQL {@code WHERE} expression.
   *
   * <p>
   * This method traverses the provided
   * {@link net.sf.jsqlparser.expression.Expression} tree and collects the names
   * of all {@link net.sf.jsqlparser.schema.Column} instances that appear in the
   * expression. The returned set preserves the order in which the columns are
   * first encountered.
   *
   * <p>
   * It is typically used for projection pushdown or other optimizations where it
   * is necessary to know which columns are actually used in a query filter.
   *
   * <p>
   * <b>Example:</b>
   * 
   * <pre>{@code
   * Expression where = CCJSqlParserUtil.parseCondExpression("age > 30 AND name = 'Alice'");
   * Set<String> cols = ColumnsUsed.inWhere(where);
   * // cols = ["age", "name"]
   * }</pre>
   *
   * @param where
   *          the expression (may be null)
   * @return set of column names referenced by the expression (empty if none)
   */
  public static Set<String> inWhere(Expression where) {
    if (where == null) {
      return Set.of();
    }
    LinkedHashSet<String> cols = new LinkedHashSet<>();
    ExpressionDeParser walker = new ExpressionDeParser() {
      @Override
      public void visit(Column column) {
        cols.add(column.getColumnName());
      }
    };
    where.accept(walker);
    return cols;
  }
}
