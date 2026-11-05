package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.select.WithItem;
import org.apache.avro.Schema;

/**
 * Validates ARRAY declarations throughout SQL queries without evaluating rows.
 */
public final class ArrayQueryValidation {
  /** Prevents utility class construction. */
  private ArrayQueryValidation() {
  }

  /**
   * Checks every clause and nested scope, including scopes execution can skip.
   *
   * @param query
   *          parsed SQL query
   * @param tables
   *          source schema resolver, returning null for unknown sources
   * @throws IllegalArgumentException
   *           carrying a JDBC cause for invalid declarations
   */
  public static void validate(Select query, Function<Table, Schema> tables) {
    visitSelect(query, new Scope(column -> null, tables, Map.of(), new HashSet<>()));
  }

  /**
   * Checks an expression using bound local columns and independently scoped
   * nested queries.
   *
   * @param expression
   *          SQL expression, possibly null
   * @param columns
   *          bound column schema resolver
   * @param tables
   *          nested input schema resolver
   * @throws IllegalArgumentException
   *           carrying a JDBC cause for invalid declarations
   */
  public static void validate(Expression expression, Function<Column, Schema> columns, Function<Table, Schema> tables) {
    visitExpression(expression, new Scope(columns, tables, Map.of(), new HashSet<>()));
  }

  /**
   * Resolves a scalar projection without inheriting another query's inputs.
   *
   * @param query
   *          scalar subquery
   * @return declared projection schema, or null when unknown
   */
  public static Schema projectionSchema(Select query) {
    return projectedValue(query, new Scope(column -> null, table -> null, Map.of(), new HashSet<>()));
  }

  /**
   * Resolves an expression and its scalar subqueries in their lexical scopes.
   *
   * @param expression
   *          expression to resolve
   * @param scope
   *          local scope
   * @return known schema, or null
   */
  private static Schema resolve(Expression expression, Scope scope) {
    return ArrayExpressionSchemas.resolve(expression, scope.columns(), nested -> projectedValue(nested, scope));
  }

  /**
   * Resolves a single projected value with a recursion guard.
   *
   * @param query
   *          scalar query
   * @param outer
   *          enclosing scope
   * @return projected schema, or null when unknown
   */
  private static Schema projectedValue(Select query, Scope outer) {
    if (!outer.resolving().add(query)) {
      return null;
    }
    try {
      Scope inherited = withDeclarations(query, outer);
      if (query instanceof ParenthesedSelect nested) {
        return projectedValue(nested.getSelect(), inherited);
      }
      if (query instanceof PlainSelect plain && plain.getSelectItems().size() == 1) {
        return resolve(plain.getSelectItems().getFirst().getExpression(), localScope(plain, inherited));
      }
      return null;
    } finally {
      outer.resolving().remove(query);
    }
  }

  /**
   * Visits expressions while replacing the column scope at each nested SELECT.
   *
   * @param expression
   *          expression to inspect, possibly null
   * @param scope
   *          current lexical scope
   */
  private static void visitExpression(Expression expression, Scope scope) {
    if (expression == null) {
      return;
    }
    expression.accept(new JSqlParserSupport.NamedArgumentVisitor() {
      @Override
      public <S> Void visit(ArrayConstructor array, S context) {
        resolve(array, scope);
        return super.visit(array, context);
      }

      @Override
      public <S> Void visit(net.sf.jsqlparser.expression.Function function, S context) {
        if ("ARRAY".equalsIgnoreCase(function.getName())) {
          resolve(function, scope);
        }
        return super.visit(function, context);
      }

      @Override
      public <S> Void visit(Select nested, S context) {
        visitSelect(nested, scope);
        return null;
      }

      @Override
      public <S> Void visit(ParenthesedSelect nested, S context) {
        return visit((Select) nested, context);
      }

      @Override
      public <S> Void visit(AnyComparisonExpression comparison, S context) {
        visitSelect(comparison.getSelect(), scope);
        return null;
      }
    });
  }

  /**
   * Validates a query body and its WITH declarations.
   *
   * @param query
   *          query body
   * @param outer
   *          enclosing scope
   */
  private static void visitSelect(Select query, Scope outer) {
    Scope inherited = withDeclarations(query, outer);
    if (query.getWithItemsList() != null) {
      query.getWithItemsList().forEach(cte -> visitSelect(cte.getSelect(), inherited));
    }
    if (query instanceof ParenthesedSelect nested) {
      visitSelect(nested.getSelect(), inherited);
    } else if (query instanceof SetOperationList set) {
      set.getSelects().forEach(branch -> visitSelect(branch, inherited));
    } else if (query instanceof Values values) {
      visitExpression(values.getExpressions(), inherited);
    } else if (query instanceof PlainSelect plain) {
      Scope local = localScope(plain, inherited);
      visitInput(plain.getFromItem(), local);
      if (plain.getJoins() != null) {
        plain.getJoins().forEach(join -> {
          visitInput(join.getRightItem(), local);
          join.getOnExpressions().forEach(on -> visitExpression(on, local));
        });
      }
      plain.getSelectItems().forEach(item -> visitExpression(item.getExpression(), local));
      visitExpression(plain.getWhere(), local);
      visitExpression(plain.getHaving(), local);
      visitExpression(plain.getQualify(), local);
      if (plain.getGroupBy() != null) {
        visitExpression(plain.getGroupBy().getGroupByExpressionList(), local);
        if (plain.getGroupBy().getGroupingSets() != null) {
          plain.getGroupBy().getGroupingSets().forEach(group -> visitExpression(group, local));
        }
      }
      if (plain.getDistinct() != null && plain.getDistinct().getOnSelectItems() != null) {
        plain.getDistinct().getOnSelectItems().forEach(item -> visitExpression(item.getExpression(), local));
      }
      if (plain.getWindowDefinitions() != null) {
        plain.getWindowDefinitions()
            .forEach(window -> window.getAllExpressions().forEach(expression -> visitExpression(expression, local)));
      }
      if (plain.getOrderByElements() != null) {
        plain.getOrderByElements().forEach(key -> visitExpression(key.getExpression(), local));
      }
    }
    if (!(query instanceof PlainSelect) && query.getOrderByElements() != null) {
      query.getOrderByElements().forEach(key -> visitExpression(key.getExpression(), inherited));
    }
  }

  /**
   * Validates nested inputs, grouped joins, and table-function arguments.
   *
   * @param input
   *          input to inspect, possibly null
   * @param scope
   *          visible declarations
   */
  private static void visitInput(FromItem input, Scope scope) {
    if (input instanceof Select select) {
      visitSelect(select, scope);
    } else if (input instanceof ParenthesedFromItem group) {
      visitInput(group.getFromItem(), scope);
      if (group.getJoins() != null) {
        group.getJoins().forEach(join -> {
          visitInput(join.getRightItem(), scope);
          join.getOnExpressions().forEach(on -> visitExpression(on, scope));
        });
      }
    } else if (input instanceof TableFunction function) {
      visitExpression(function.getFunction(), scope);
    }
  }

  /**
   * Applies local WITH shadowing without resolving recursive declarations yet.
   *
   * @param query
   *          declaring query
   * @param outer
   *          enclosing scope
   * @return scope with local declarations
   */
  private static Scope withDeclarations(Select query, Scope outer) {
    Map<Identifier, WithItem<?>> ctes = new HashMap<>(outer.ctes());
    if (query.getWithItemsList() != null) {
      query.getWithItemsList().forEach(cte -> ctes.put(Identifier.of(cte.getAliasName()), cte));
    }
    return new Scope(outer.columns(), outer.tables(), ctes, outer.resolving());
  }

  /**
   * Resolves local columns before correlated outer references.
   *
   * @param query
   *          current SELECT
   * @param outer
   *          enclosing scope
   * @return local scope
   */
  private static Scope localScope(PlainSelect query, Scope outer) {
    return new Scope(column -> {
      Binding match = new Binding(false, null);
      for (FromItem input : inputs(query)) {
        Binding binding = sourceColumn(input, column, outer);
        if (binding.present()) {
          if (match.present()) {
            return null;
          }
          match = binding;
        }
      }
      return match.present() ? match.schema() : outer.columns().apply(column);
    }, outer.tables(), outer.ctes(), outer.resolving());
  }

  /**
   * Lists inputs in join order.
   *
   * @param query
   *          SELECT body
   * @return input list, excluding null inputs
   */
  private static List<FromItem> inputs(PlainSelect query) {
    List<FromItem> result = new ArrayList<>();
    if (query.getFromItem() != null) {
      result.add(query.getFromItem());
    }
    if (query.getJoins() != null) {
      query.getJoins().forEach(join -> result.add(join.getRightItem()));
    }
    return result;
  }

  /**
   * Resolves one input without borrowing same-named outer columns.
   *
   * @param input
   *          local input
   * @param column
   *          requested column
   * @param scope
   *          enclosing declarations
   * @return whether the source owns the column and its known schema
   */
  private static Binding sourceColumn(FromItem input, Column column, Scope scope) {
    String qualifier = column.getTable() == null ? null : column.getTable().getName();
    String name = input.getAlias() != null
        ? input.getAlias().getName()
        : input instanceof Table table ? table.getName() : null;
    if (qualifier != null && !Identifier.of(qualifier).matches(Identifier.of(name))) {
      return new Binding(false, null);
    }
    List<String> aliases = input.getAlias() != null && input.getAlias().getAliasColumns() != null
        ? input.getAlias().getAliasColumns().stream().map(alias -> alias.name).toList()
        : List.of();
    if (input instanceof Table table) {
      WithItem<?> cte = Identifier.of(table.getName()).resolve(scope.ctes());
      if (cte != null) {
        List<String> outputAliases = !aliases.isEmpty()
            ? aliases
            : cte.getWithItemList() == null ? List.of() : cte.getWithItemList().stream().map(Object::toString).toList();
        return projectedColumn(cte.getSelect(), column.getColumnName(), outputAliases, scope);
      }
      Schema schema = scope.tables().apply(table);
      if (schema == null) {
        return new Binding(true, null);
      }
      if (!aliases.isEmpty()) {
        for (int i = 0; i < aliases.size() && i < schema.getFields().size(); i++) {
          if (Identifier.of(column.getColumnName()).matches(Identifier.of(aliases.get(i)))) {
            return new Binding(true, schema.getFields().get(i).schema());
          }
        }
        return new Binding(false, null);
      }
      for (Schema.Field field : schema.getFields()) {
        if (Identifier.of(column.getColumnName()).matches(Identifier.of(field.name()))) {
          return new Binding(true, field.schema());
        }
      }
      return new Binding(false, null);
    }
    if (input instanceof Select select) {
      return projectedColumn(select, column.getColumnName(), aliases, scope);
    }
    return new Binding(true, null);
  }

  /**
   * Resolves a derived or CTE projection in its own input scope.
   *
   * @param query
   *          projected query
   * @param name
   *          output column name
   * @param aliases
   *          positional output aliases
   * @param outer
   *          enclosing declarations
   * @return matching declaration, or an unresolved binding
   */
  private static Binding projectedColumn(Select query, String name, List<String> aliases, Scope outer) {
    if (!outer.resolving().add(query)) {
      return new Binding(true, null);
    }
    try {
      Scope inherited = withDeclarations(query, outer);
      if (query instanceof ParenthesedSelect nested) {
        return projectedColumn(nested.getSelect(), name, aliases, inherited);
      }
      if (query instanceof PlainSelect plain) {
        Scope local = localScope(plain, inherited);
        for (int i = 0; i < plain.getSelectItems().size(); i++) {
          var item = plain.getSelectItems().get(i);
          Expression expression = item.getExpression();
          if (expression instanceof AllColumns) {
            return new Binding(true, local.columns().apply(new Column(name)));
          }
          String label = i < aliases.size()
              ? aliases.get(i)
              : item.getAlias() != null
                  ? item.getAlias().getName()
                  : expression instanceof Column column ? column.getColumnName() : expression.toString();
          if (Identifier.of(name).matches(Identifier.of(label))) {
            return new Binding(true, resolve(expression, local));
          }
        }
        return new Binding(false, null);
      }
      return new Binding(true, null);
    } finally {
      outer.resolving().remove(query);
    }
  }

  /**
   * A lexical column resolver with local CTEs and a recursive-resolution guard.
   *
   * @param columns
   *          visible column declarations
   * @param tables
   *          physical or materialized source schema resolver
   * @param ctes
   *          visible WITH declarations
   * @param resolving
   *          projections currently being resolved
   */
  private record Scope(Function<Column, Schema> columns, Function<Table, Schema> tables,
      Map<Identifier, WithItem<?>> ctes, Set<Select> resolving) {
  }

  /**
   * Distinguishes an unknown local type from an absent local column.
   *
   * @param present
   *          whether the source owns or may own the column
   * @param schema
   *          known declaration, possibly null
   */
  private record Binding(boolean present, Schema schema) {
  }
}
