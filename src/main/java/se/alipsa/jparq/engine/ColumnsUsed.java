package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.TranscodingFunction;
import net.sf.jsqlparser.schema.Column;
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
    return collect(where, true);
  }

  /**
   * Collects column names in an expression without entering nested query scopes.
   *
   * @param expression
   *          expression to inspect, or null
   * @return column names belonging to the current query scope
   */
  public static Set<String> inScope(Expression expression) {
    return collect(expression, false);
  }

  /**
   * Retains table qualifiers while collecting references in the current scope.
   *
   * @param expression
   *          expression to inspect, or null
   * @return column nodes without references inside nested queries
   */
  public static List<Column> referencesInScope(Expression expression) {
    List<Column> columns = new ArrayList<>();
    walk(expression, false, columns::add);
    return List.copyOf(columns);
  }

  /**
   * Walks an expression with the requested query-scope boundary.
   *
   * @param where
   *          expression to inspect
   * @param includeSubqueries
   *          whether nested query expressions should be visited
   * @return referenced column names
   */
  private static Set<String> collect(Expression where, boolean includeSubqueries) {
    LinkedHashSet<String> cols = new LinkedHashSet<>();
    walk(where, includeSubqueries, column -> cols.add(column.getColumnName()));
    return cols;
  }

  /**
   * Visits columns with an explicit query-scope boundary.
   *
   * @param where
   *          expression to inspect
   * @param includeSubqueries
   *          whether nested expressions should be visited
   * @param consumer
   *          receiver for each column reference
   */
  private static void walk(Expression where, boolean includeSubqueries, Consumer<Column> consumer) {
    if (where == null) {
      return;
    }
    ExpressionVisitorAdapter<Void> walker = new ExpressionVisitorAdapter<Void>() {
      @Override
      public <S> Void visit(net.sf.jsqlparser.statement.select.Select select, S context) {
        return includeSubqueries ? super.visit(select, context) : null;
      }

      @Override
      public <S> Void visit(net.sf.jsqlparser.statement.select.ParenthesedSelect select, S context) {
        return includeSubqueries ? super.visit(select, context) : null;
      }

      @Override
      public <S> Void visit(Column column, S context) {
        if (!SqlParser.isTimeKeyword(column.getColumnName())) {
          consumer.accept(column);
        }
        return null;
      }

      @Override
      public <S> Void visit(TranscodingFunction convert, S context) {
        SqlParser.ConvertProcessingResult result = SqlParser.analyzeConvertFunction(convert);
        if (result.syntheticColumnName() != null) {
          consumer.accept(new Column(result.syntheticColumnName()));
          return null;
        }
        if (result.processInner() && convert != null) {
          Expression expr = convert.getExpression();
          if (expr != null) {
            expr.accept(this, context);
          }
        }
        return null;
      }
    };
    where.accept(walker);
  }
}
