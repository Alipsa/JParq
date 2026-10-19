package se.alipsa.jparq.engine;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.IsUnknownExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.Values;

/**
 * Restores IN precedence throughout a statement before any SQL scopes are
 * extracted or serialized.
 */
public final class SqlExpressionNormalizer {
  private final Set<Select> visited = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Map<Expression, Expression> absorbedHeads = new IdentityHashMap<>();
  private final Children children = new Children();

  private SqlExpressionNormalizer() {
  }

  /**
   * Normalizes all statement scopes and stores replacement roots on their owners.
   *
   * @param statement
   *          parsed SELECT, including CTEs and set operations
   */
  public static void normalize(Select statement) {
    new SqlExpressionNormalizer().select(statement);
  }

  /**
   * Traverses a SELECT and its relational children once.
   *
   * @param statement
   *          query scope
   */
  private void select(Select statement) {
    if (statement == null || !visited.add(statement)) {
      return;
    }
    if (statement.getWithItemsList() != null) {
      statement.getWithItemsList().forEach(item -> select(item.getSelect()));
    }
    if (statement.getOrderByElements() != null) {
      statement.getOrderByElements().forEach(order -> order.setExpression(expression(order.getExpression())));
    }
    if (statement instanceof ParenthesedSelect parenthesized) {
      select(parenthesized.getSelect());
    } else if (statement instanceof SetOperationList set) {
      set.getSelects().forEach(this::select);
    } else if (statement instanceof PlainSelect plain) {
      plain.setWhere(expression(plain.getWhere()));
      plain.setHaving(expression(plain.getHaving()));
      plain.setQualify(expression(plain.getQualify()));
      plain.getSelectItems().forEach(this::item);
      if (plain.getGroupBy() != null) {
        list(plain.getGroupBy().getGroupByExpressionList());
      }
      from(plain.getFromItem());
      joins(plain.getJoins());
    } else if (statement instanceof Values values) {
      list(values.getExpressions());
    }
  }

  /**
   * Traverses nested FROM scopes.
   *
   * @param source
   *          FROM item, or null
   */
  private void from(FromItem source) {
    if (source instanceof ParenthesedSelect nested) {
      select(nested);
    } else if (source instanceof ParenthesedFromItem nested) {
      from(nested.getFromItem());
      joins(nested.getJoins());
    }
  }

  /**
   * Normalizes ON roots and join inputs.
   *
   * @param joins
   *          join list, or null
   */
  private void joins(java.util.List<Join> joins) {
    if (joins == null) {
      return;
    }
    for (Join join : joins) {
      if (join.getOnExpressions() != null) {
        join.setOnExpressions(join.getOnExpressions().stream().map(this::expression).toList());
      }
      from(join.getRightItem());
    }
  }

  /**
   * Replaces a SELECT item's root while preserving its alias.
   *
   * @param item
   *          SELECT item
   * @param <T>
   *          original expression type
   */
  @SuppressWarnings("unchecked")
  private <T extends Expression> void item(SelectItem<T> item) {
    item.setExpression((T) expression(item.getExpression()));
  }

  /**
   * Replaces roots in a mutable expression list.
   *
   * @param expressions
   *          list, or null
   * @param <T>
   *          expression element type
   */
  @SuppressWarnings("unchecked")
  private <T extends Expression> void list(ExpressionList<T> expressions) {
    if (expressions != null) {
      for (int index = 0; index < expressions.size(); index++) {
        expressions.set(index, (T) expression(expressions.get(index)));
      }
    }
  }

  /**
   * Repairs an absorbed Boolean tail and recursively normalizes expression
   * children.
   *
   * @param original
   *          expression root, or null
   * @return replacement root
   */
  private Expression expression(Expression original) {
    if (original == null) {
      return null;
    }
    Expression result = original;
    if (original instanceof InExpression in && booleanOperator(in.getRightExpression())) {
      BinaryExpression tail = (BinaryExpression) in.getRightExpression();
      BinaryExpression leading = tail;
      while (booleanOperator(leading.getLeftExpression())) {
        leading = (BinaryExpression) leading.getLeftExpression();
      }
      Expression operand = leading.getLeftExpression();
      if (operand instanceof ParenthesedSelect || operand instanceof ParenthesedExpressionList<?>) {
        in.setRightExpression(operand);
        leading.setLeftExpression(in);
        result = tail;
        absorbedHeads.put(tail, in);
      }
    }
    if (LegacyParenthesisSupport.isLegacyParenthesis(result)) {
      LegacyParenthesisSupport.replace(result, expression(LegacyParenthesisSupport.extract(result)));
    } else {
      result.accept(children, null);
      if (result instanceof AndExpression and) {
        result = liftAbsorbed(and, and.getRightExpression());
      } else if (result instanceof NotExpression not) {
        result = liftAbsorbed(not, not.getExpression());
      }
    }
    return result;
  }

  /**
   * Moves an absorbed tail above the parent that precedes its IN predicate.
   * Explicit parentheses have their own root and therefore never appear in the
   * map.
   *
   * @param parent
   *          AND or NOT preceding the absorbed tail
   * @param operand
   *          normalized child that may contain an absorbed tail
   * @return the tail with the parent applied only to its leading predicate, or
   *         the parent
   */
  private Expression liftAbsorbed(Expression parent, Expression operand) {
    Expression head = absorbedHeads.get(operand);
    if (head == null) {
      return parent;
    }
    BinaryExpression leading = (BinaryExpression) operand;
    while (leading.getLeftExpression() != head) {
      leading = (BinaryExpression) leading.getLeftExpression();
    }
    if (parent instanceof AndExpression and) {
      and.setRightExpression(head);
    } else if (parent instanceof NotExpression not) {
      not.setExpression(head);
    }
    leading.setLeftExpression(parent);
    // A parent originating from an earlier IN absorption retains that IN as its
    // head.
    absorbedHeads.put(operand, absorbedHeads.getOrDefault(parent, parent));
    return operand;
  }

  /**
   * Identifies operators that can be absorbed after an IN operand.
   *
   * @param value
   *          expression
   * @return whether this is AND or OR
   */
  private static boolean booleanOperator(Expression value) {
    return value instanceof AndExpression || value instanceof OrExpression;
  }

  /**
   * Updates child owners rather than discarding normalized roots returned by
   * visitors.
   */
  private final class Children extends ExpressionVisitorAdapter<Void> {
    @Override
    protected <S> Void visitBinaryExpression(BinaryExpression binary, S context) {
      binary.setLeftExpression(expression(binary.getLeftExpression()));
      binary.setRightExpression(expression(binary.getRightExpression()));
      return null;
    }

    @Override
    public <S> Void visit(InExpression in, S context) {
      in.setLeftExpression(expression(in.getLeftExpression()));
      in.setRightExpression(expression(in.getRightExpression()));
      return null;
    }

    @Override
    public <S> Void visit(NotExpression not, S context) {
      not.setExpression(expression(not.getExpression()));
      return null;
    }

    @Override
    public <S> Void visit(Function function, S context) {
      list(function.getParameters());
      return super.visit(function, context);
    }

    @Override
    public <S> Void visit(AnalyticExpression analytic, S context) {
      analytic.setExpression(expression(analytic.getExpression()));
      analytic.setOffset(expression(analytic.getOffset()));
      analytic.setDefaultValue(expression(analytic.getDefaultValue()));
      analytic.setFilterExpression(expression(analytic.getFilterExpression()));
      return super.visit(analytic, context);
    }

    @Override
    public <S> Void visit(SignedExpression signed, S context) {
      signed.setExpression(expression(signed.getExpression()));
      return null;
    }

    @Override
    public <S> Void visit(IsBooleanExpression predicate, S context) {
      predicate.setLeftExpression(expression(predicate.getLeftExpression()));
      return null;
    }

    @Override
    public <S> Void visit(IsNullExpression predicate, S context) {
      predicate.setLeftExpression(expression(predicate.getLeftExpression()));
      return null;
    }

    @Override
    public <S> Void visit(IsUnknownExpression predicate, S context) {
      predicate.setLeftExpression(expression(predicate.getLeftExpression()));
      return null;
    }

    @Override
    public <S> Void visit(Between between, S context) {
      between.setLeftExpression(expression(between.getLeftExpression()));
      between.setBetweenExpressionStart(expression(between.getBetweenExpressionStart()));
      between.setBetweenExpressionEnd(expression(between.getBetweenExpressionEnd()));
      return null;
    }

    @Override
    public <S> Void visit(CastExpression cast, S context) {
      cast.setLeftExpression(expression(cast.getLeftExpression()));
      return null;
    }

    @Override
    public <S> Void visit(CaseExpression cases, S context) {
      cases.setSwitchExpression(expression(cases.getSwitchExpression()));
      cases.setElseExpression(expression(cases.getElseExpression()));
      cases.getWhenClauses().forEach(clause -> clause.accept(this, context));
      return null;
    }

    @Override
    public <S> Void visit(WhenClause clause, S context) {
      clause.setWhenExpression(expression(clause.getWhenExpression()));
      clause.setThenExpression(expression(clause.getThenExpression()));
      return null;
    }

    @Override
    public <S> Void visit(ExpressionList<? extends Expression> expressions, S context) {
      list(expressions);
      return null;
    }

    @Override
    public <S> Void visit(ParenthesedSelect nested, S context) {
      select(nested);
      return null;
    }

    @Override
    public <S> Void visit(Select nested, S context) {
      select(nested);
      return null;
    }
  }
}
