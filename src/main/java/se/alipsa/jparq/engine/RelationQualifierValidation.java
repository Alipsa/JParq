package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.Values;

/**
 * Checks lexical relation qualifier visibility before any rows are evaluated.
 */
final class RelationQualifierValidation {
  /** Prevents utility class instantiation. */
  private RelationQualifierValidation() {
  }

  /**
   * Validates all query scopes, including subqueries that execution might skip.
   *
   * @param query
   *          parsed statement with original identifier quoting
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 for an unknown or hidden qualifier
   */
  static void validate(Select query) {
    visitSelect(query, List.of());
  }

  /**
   * Checks a query with the relation names visible from enclosing queries.
   *
   * @param query
   *          query to inspect
   * @param outer
   *          enclosing relation qualifiers
   */
  private static void visitSelect(Select query, List<String> outer) {
    if (query.getWithItemsList() != null) {
      query.getWithItemsList().forEach(cte -> visitSelect(cte.getSelect(), outer));
    }
    if (query instanceof ParenthesedSelect nested) {
      visitSelect(nested.getSelect(), outer);
    } else if (query instanceof SetOperationList set) {
      set.getSelects().forEach(branch -> visitSelect(branch, outer));
      if (set.getOrderByElements() != null) {
        set.getOrderByElements().forEach(key -> visitExpression(key.getExpression(), outer));
      }
    } else if (query instanceof Values values) {
      visitExpression(values.getExpressions(), outer);
    } else if (query instanceof PlainSelect plain) {
      visitPlainSelect(plain, outer);
    }
  }

  /**
   * Checks inputs in join order, then expressions with all local inputs visible.
   *
   * @param query
   *          SELECT body
   * @param outer
   *          enclosing relation qualifiers
   */
  private static void visitPlainSelect(PlainSelect query, List<String> outer) {
    List<String> visible = new ArrayList<>(outer);
    visitInput(query.getFromItem(), outer, visible);
    if (query.getJoins() != null) {
      for (var join : query.getJoins()) {
        visitInput(join.getFromItem(), outer, visible);
        join.getOnExpressions().forEach(on -> visitExpression(on, visible));
      }
    }
    query.getSelectItems().forEach(item -> {
      if (item.getExpression() instanceof AllTableColumns) {
        requireVisibleQualifier(SqlParser.wildcardQualifierTokens(item), "*", visible);
      } else {
        visitExpression(item.getExpression(), visible);
      }
    });
    visitExpression(query.getWhere(), visible);
    visitExpression(query.getHaving(), visible);
    visitExpression(query.getQualify(), visible);
    if (query.getGroupBy() != null) {
      visitExpression(query.getGroupBy().getGroupByExpressionList(), visible);
      if (query.getGroupBy().getGroupingSets() != null) {
        query.getGroupBy().getGroupingSets().forEach(group -> visitExpression(group, visible));
      }
    }
    if (query.getOrderByElements() != null) {
      query.getOrderByElements().forEach(key -> visitExpression(key.getExpression(), visible));
    }
    if (query.getDistinct() != null && query.getDistinct().getOnSelectItems() != null) {
      query.getDistinct().getOnSelectItems().forEach(item -> visitExpression(item.getExpression(), visible));
    }
    if (query.getWindowDefinitions() != null) {
      query.getWindowDefinitions()
          .forEach(window -> window.getAllExpressions().forEach(expression -> visitExpression(expression, visible)));
    }
  }

  /**
   * Checks a source and registers only its SQL-visible names. LATERAL sources can
   * also see preceding inputs in the same query.
   *
   * @param input
   *          source, or null for a table-free query
   * @param outer
   *          enclosing query qualifiers
   * @param visible
   *          qualifiers including preceding inputs, updated with this input
   */
  private static void visitInput(FromItem input, List<String> outer, List<String> visible) {
    if (input == null) {
      return;
    }
    if (input instanceof ParenthesedSelect nested) {
      visitSelect(nested, input instanceof LateralSubSelect ? visible : outer);
    } else if (input instanceof ParenthesedFromItem group) {
      List<String> grouped = new ArrayList<>(visible);
      visitInput(group.getFromItem(), outer, grouped);
      if (group.getJoins() != null) {
        for (var join : group.getJoins()) {
          visitInput(join.getFromItem(), outer, grouped);
          join.getOnExpressions().forEach(on -> visitExpression(on, grouped));
        }
      }
      if (input.getAlias() == null) {
        visible.addAll(grouped.subList(visible.size(), grouped.size()));
        return;
      }
    } else if (input instanceof TableFunction function) {
      visitExpression(function.getFunction(), visible);
    }
    if (input.getAlias() != null && !input.getAlias().getName().isBlank()) {
      visible.add(input.getAlias().getName());
    } else if (input instanceof Table table) {
      visible.add(table.getName());
      visible.add(table.getFullyQualifiedName());
    }
  }

  /**
   * Visits expressions while giving each nested SELECT its own lexical scope.
   *
   * @param expression
   *          expression to inspect, or null
   * @param visible
   *          local and enclosing qualifiers
   */
  private static void visitExpression(Expression expression, List<String> visible) {
    if (expression == null) {
      return;
    }
    expression.accept(new JSqlParserSupport.NamedArgumentVisitor() {
      /** {@inheritDoc} */
      @Override
      public <S> Void visit(Column column, S context) {
        RelationQualifiers.restore(column);
        requireVisibleQualifier(RelationQualifiers.qualifier(column), column.getColumnName(), visible);
        return null;
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(Select nested, S context) {
        visitSelect(nested, visible);
        return null;
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(ParenthesedSelect nested, S context) {
        return visit((Select) nested, context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(AnyComparisonExpression comparison, S context) {
        visitSelect(comparison.getSelect(), visible);
        return null;
      }
    });
  }

  /**
   * Ensures a qualified reference names a table or alias visible in the current
   * scope.
   *
   * @param qualifier
   *          optional complete table qualifier from the SQL statement
   * @param column
   *          SQL column identifier
   * @param visible
   *          relation names visible in the current scope, with identifier quoting
   *          preserved
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 when the qualifier is unknown
   */
  private static void requireVisibleQualifier(String qualifier, String column, List<String> visible) {
    if (qualifier == null || qualifier.isBlank()
        || visible.stream().anyMatch(name -> RelationQualifiers.matches(qualifier, name))) {
      return;
    }
    throw SqlExceptions.syntaxFailure("Unknown table qualifier '" + qualifier + "' for column '" + column + "'");
  }

}
