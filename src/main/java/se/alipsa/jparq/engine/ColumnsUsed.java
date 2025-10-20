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
