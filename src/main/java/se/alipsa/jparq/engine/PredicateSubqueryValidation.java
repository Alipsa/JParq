package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.select.WithItem;

/** Validates scalar predicate subquery degrees without executing their rows. */
public final class PredicateSubqueryValidation {
  /** Prevents utility class instantiation. */
  private PredicateSubqueryValidation() {
  }

  /**
   * Checks predicate subqueries before filtering or grouping can skip them.
   *
   * @param query
   *          outer query plan
   * @param columns
   *          source column resolver, returning null for unknown sources
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 for an invalid scalar subquery degree
   */
  public static void validate(SqlParser.Select query, Function<Table, Set<String>> columns) {
    List<Expression> expressions = new ArrayList<>(query.expressions());
    expressions.add(query.where());
    expressions.add(query.having());
    expressions.addAll(query.groupByExpressions());
    query.orderBy().forEach(key -> expressions.add(key.expression()));
    visitExpressions(expressions, columns, Map.of());
  }

  /**
   * Visits predicates in a lexical scope, including unevaluated CASE branches.
   *
   * @param expressions
   *          expressions in the current scope
   * @param columns
   *          source schema resolver
   * @param ctes
   *          visible local CTE declarations
   */
  private static void visitExpressions(List<Expression> expressions, Function<Table, Set<String>> columns,
      Map<Identifier, WithItem<?>> ctes) {
    var visitor = new ExpressionVisitorAdapter<Void>() {
      @Override
      public <S> Void visit(InExpression in, S context) {
        if (in.getRightExpression() instanceof Select nested
            && !(ExpressionEvaluator.unwrapParenthesis(in.getLeftExpression()) instanceof ExpressionList<?>)) {
          requireSingleColumn(nested, columns, ctes);
        }
        return super.visit(in, context);
      }

      @Override
      public <S> Void visit(AnyComparisonExpression any, S context) {
        requireSingleColumn(any.getSelect(), columns, ctes);
        return visit(any.getSelect(), context);
      }

      @Override
      public <S> Void visit(Select nested, S context) {
        visitSelect(nested, columns, ctes);
        return null;
      }

      @Override
      public <S> Void visit(ParenthesedSelect nested, S context) {
        return visit((Select) nested, context);
      }
    };
    for (Expression expression : expressions) {
      if (expression != null) {
        expression.accept(visitor, null);
      }
    }
  }

  /**
   * Recurses into nested queries without evaluating their projections.
   *
   * @param select
   *          nested query
   * @param columns
   *          source schema resolver
   * @param inherited
   *          inherited CTE declarations
   */
  private static void visitSelect(Select select, Function<Table, Set<String>> columns,
      Map<Identifier, WithItem<?>> inherited) {
    Map<Identifier, WithItem<?>> ctes = declarations(select, inherited);
    if (select.getWithItemsList() != null) {
      select.getWithItemsList().forEach(cte -> visitSelect(cte.getSelect(), columns, ctes));
    }
    if (select instanceof ParenthesedSelect nested) {
      visitSelect(nested.getSelect(), columns, ctes);
    } else if (select instanceof SetOperationList set) {
      set.getSelects().forEach(branch -> visitSelect(branch, columns, ctes));
    } else if (select instanceof Values values) {
      visitExpressions(new ArrayList<>(values.getExpressions()), columns, ctes);
    } else if (select instanceof PlainSelect plain) {
      List<Expression> expressions = new ArrayList<>();
      plain.getSelectItems().forEach(item -> expressions.add(item.getExpression()));
      expressions.add(plain.getWhere());
      expressions.add(plain.getHaving());
      if (plain.getGroupBy() != null && plain.getGroupBy().getGroupByExpressionList() != null) {
        expressions.addAll(plain.getGroupBy().getGroupByExpressionList());
      }
      if (plain.getOrderByElements() != null) {
        plain.getOrderByElements().forEach(key -> expressions.add(key.getExpression()));
      }
      visitInput(plain.getFromItem(), columns, ctes);
      if (plain.getJoins() != null) {
        for (Join join : plain.getJoins()) {
          expressions.addAll(join.getOnExpressions());
          visitInput(join.getRightItem(), columns, ctes);
        }
      }
      visitExpressions(expressions, columns, ctes);
    }
  }

  /**
   * Visits derived inputs so skipped predicates in nested scopes are checked.
   *
   * @param input
   *          input to inspect, or null
   * @param columns
   *          source schema resolver
   * @param ctes
   *          visible CTE declarations
   */
  private static void visitInput(FromItem input, Function<Table, Set<String>> columns,
      Map<Identifier, WithItem<?>> ctes) {
    if (input instanceof ParenthesedSelect nested) {
      visitSelect(nested, columns, ctes);
    } else if (input instanceof ParenthesedFromItem group) {
      visitInput(group.getFromItem(), columns, ctes);
      if (group.getJoins() != null) {
        for (Join join : group.getJoins()) {
          visitExpressions(new ArrayList<>(join.getOnExpressions()), columns, ctes);
          visitInput(join.getRightItem(), columns, ctes);
        }
      }
    }
  }

  /**
   * Rejects a known non-scalar degree; unresolved sources retain runtime checks.
   *
   * @param select
   *          predicate subquery
   * @param columns
   *          source schema resolver
   * @param ctes
   *          visible CTE declarations
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 when the degree differs from one
   */
  private static void requireSingleColumn(Select select, Function<Table, Set<String>> columns,
      Map<Identifier, WithItem<?>> ctes) {
    Integer count = degree(select, columns, ctes);
    if (count != null && count != 1) {
      throw SqlExceptions.syntaxFailure("Subquery must return exactly one column: " + select);
    }
  }

  /**
   * Counts output positions, preserving duplicate projection labels.
   *
   * @param select
   *          query whose degree is needed
   * @param columns
   *          source schema resolver
   * @param inherited
   *          inherited CTE declarations
   * @return output degree, or null if a wildcard source cannot be resolved
   */
  private static Integer degree(Select select, Function<Table, Set<String>> columns,
      Map<Identifier, WithItem<?>> inherited) {
    Map<Identifier, WithItem<?>> ctes = declarations(select, inherited);
    if (select instanceof ParenthesedSelect nested) {
      return degree(nested.getSelect(), columns, ctes);
    }
    if (select instanceof SetOperationList set) {
      return degree(set.getSelects().getFirst(), columns, ctes);
    }
    if (select instanceof Values values) {
      ExpressionList<?> expressions = values.getExpressions();
      if (expressions == null || expressions.isEmpty()) {
        return 0;
      }
      return expressions.getFirst() instanceof ExpressionList<?> row ? row.size() : expressions.size();
    }
    if (!(select instanceof PlainSelect plain)) {
      return null;
    }
    int count = 0;
    for (var item : plain.getSelectItems()) {
      Expression expression = item.getExpression();
      if (expression instanceof AllColumns) {
        String qualifier = expression instanceof AllTableColumns all ? all.getTable().getName() : null;
        Integer width = inputDegree(plain.getFromItem(), plain.getJoins(), qualifier, columns, ctes);
        if (width == null) {
          return null;
        }
        count += width;
      } else {
        count++;
      }
    }
    return count;
  }

  /**
   * Counts wildcard input positions across joins, honoring USING coalescence.
   *
   * @param first
   *          first input
   * @param joins
   *          joined inputs, or null
   * @param qualifier
   *          wildcard qualifier, or null for every input
   * @param columns
   *          source schema resolver
   * @param ctes
   *          visible CTE declarations
   * @return input degree, or null for an unresolved input
   */
  private static Integer inputDegree(FromItem first, List<Join> joins, String qualifier,
      Function<Table, Set<String>> columns, Map<Identifier, WithItem<?>> ctes) {
    Integer count = sourceDegree(first, qualifier, columns, ctes);
    if (count == null) {
      return null;
    }
    if (joins != null) {
      for (Join join : joins) {
        Integer width = sourceDegree(join.getRightItem(), qualifier, columns, ctes);
        if (width == null) {
          return null;
        }
        count += width;
        if (qualifier == null && join.getUsingColumns() != null) {
          count -= join.getUsingColumns().size();
        }
      }
    }
    return count;
  }

  /**
   * Resolves one source without reading its rows or evaluating expressions.
   *
   * @param input
   *          source input
   * @param qualifier
   *          requested qualifier, or null
   * @param columns
   *          source schema resolver
   * @param ctes
   *          visible CTE declarations
   * @return source degree, zero for another qualifier, or null if unknown
   */
  private static Integer sourceDegree(FromItem input, String qualifier, Function<Table, Set<String>> columns,
      Map<Identifier, WithItem<?>> ctes) {
    if (input == null) {
      return 0;
    }
    String name = input.getAlias() != null
        ? input.getAlias().getName()
        : input instanceof Table table ? table.getName() : null;
    if (qualifier != null && !Identifier.of(qualifier).matches(Identifier.of(name))) {
      return 0;
    }
    if (input.getAlias() != null && input.getAlias().getAliasColumns() != null
        && !input.getAlias().getAliasColumns().isEmpty()) {
      return input.getAlias().getAliasColumns().size();
    }
    if (input instanceof ParenthesedSelect nested) {
      return degree(nested, columns, ctes);
    }
    if (input instanceof Values values) {
      return degree(values, columns, ctes);
    }
    if (input instanceof ParenthesedFromItem group) {
      return inputDegree(group.getFromItem(), group.getJoins(), null, columns, ctes);
    }
    if (input instanceof Table table) {
      WithItem<?> cte = Identifier.of(table.getName()).resolve(ctes);
      if (cte != null) {
        if (cte.getWithItemList() != null && !cte.getWithItemList().isEmpty()) {
          return cte.getWithItemList().size();
        }
        Map<Identifier, WithItem<?>> preceding = new LinkedHashMap<>(ctes);
        preceding.remove(Identifier.of(cte.getAliasName()));
        return degree(cte.getSelect(), columns, preceding);
      }
      Set<String> names = columns.apply(table);
      return names == null ? null : names.size();
    }
    return null;
  }

  /**
   * Applies local WITH shadowing to an inherited lexical scope.
   *
   * @param select
   *          declaring query
   * @param inherited
   *          inherited declarations
   * @return visible declarations
   */
  private static Map<Identifier, WithItem<?>> declarations(Select select, Map<Identifier, WithItem<?>> inherited) {
    Map<Identifier, WithItem<?>> declarations = new LinkedHashMap<>(inherited);
    if (select.getWithItemsList() != null) {
      select.getWithItemsList().forEach(cte -> declarations.put(Identifier.of(cte.getAliasName()), cte));
    }
    return declarations;
  }
}
