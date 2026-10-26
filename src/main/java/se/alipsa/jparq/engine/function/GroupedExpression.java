package se.alipsa.jparq.engine.function;

import static se.alipsa.jparq.engine.SqlExceptions.syntaxFailure;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import se.alipsa.jparq.engine.CorrelatedBindings;
import se.alipsa.jparq.engine.JSqlParserSupport;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;

/**
 * Evaluates scalar expressions with typed bindings for completed group values.
 */
final class GroupedExpression {
  private static final String BINDING_KEY = "grouped-expression";
  private final List<Expression> bindings = new ArrayList<>();
  private final Expression expression;
  private final String sourceDescription;
  private final Schema schema = Schema.createRecord("grouped", null, null, false, List.of());
  private final ValueExpressionEvaluator evaluator = new ValueExpressionEvaluator(schema);

  /**
   * Compiles a scalar expression, replacing group-dependent operands with slots.
   * Predicate subqueries bind their whole predicate so their query results retain
   * set semantics rather than being reduced to scalar values.
   *
   * @param source
   *          source expression
   * @param bind
   *          identifies operands supplied by the group evaluator
   * @throws IllegalArgumentException
   *           if compilation or operand validation fails
   */
  GroupedExpression(Expression source, Predicate<Expression> bind) {
    sourceDescription = SqlExceptions.describe(source);
    StringBuilder sql = new StringBuilder();
    source.accept(bindingDeparser(sql, bind, bindings), null);
    try {
      expression = CCJSqlParserUtil.parseExpression(sql.toString());
      rejectUnboundOperands(expression);
    } catch (net.sf.jsqlparser.JSQLParserException e) {
      throw new IllegalArgumentException("Cannot compile grouped expression: " + source, e);
    }
  }

  /**
   * Rejects group-dependent operands left behind by incomplete deparser
   * traversal.
   *
   * @param compiled
   *          rewritten scalar expression
   * @throws IllegalArgumentException
   *           if a column, aggregate, or subquery remains unbound
   */
  private static void rejectUnboundOperands(Expression compiled) {
    compiled.accept(new JSqlParserSupport.NamedArgumentVisitor() {
      @Override
      public <S> Void visit(Column column, S context) {
        throw syntaxFailure("Unbound column in grouped expression: " + column);
      }

      @Override
      public <S> Void visit(net.sf.jsqlparser.expression.Function function, S context) {
        if (AggregateFunctions.isAggregateName(function.getName())) {
          throw syntaxFailure("Unbound aggregate in grouped expression: " + function);
        }
        return super.visit(function, context);
      }

      @Override
      public <S> Void visit(AnyComparisonExpression comparison, S context) {
        return visit(comparison.getSelect(), context);
      }

      @Override
      public <S> Void visit(Select select, S context) {
        throw syntaxFailure("Unbound subquery in grouped expression: " + select);
      }

      @Override
      public <S> Void visit(ParenthesedSelect select, S context) {
        return visit((Select) select, context);
      }
    }, null);
  }

  /**
   * Validates operands without reparsing the rewritten scalar form. Predicate
   * subqueries such as EXISTS and ANY retain their original query syntax.
   *
   * @param source
   *          expression to validate
   * @param bind
   *          validates operands and identifies scope boundaries
   * @throws IllegalArgumentException
   *           if operand validation fails
   */
  static void validate(Expression source, Predicate<Expression> bind) {
    source.accept(bindingDeparser(new StringBuilder(), bind, new ArrayList<>()), null);
  }

  /**
   * Creates the shared scope-aware traversal for validation and scalar
   * compilation.
   *
   * @param sql
   *          rewritten scalar text
   * @param bind
   *          validates operands and identifies scope boundaries
   * @param bindings
   *          group-dependent operands in slot order
   * @return a deparser that stops at each bound operand
   */
  private static ExpressionDeParser bindingDeparser(StringBuilder sql, Predicate<Expression> bind,
      List<Expression> bindings) {
    return new ExpressionDeParser(null, sql) {
      /**
       * Emits a typed slot for a group-dependent operand.
       *
       * @param operand
       *          expression to inspect
       * @return whether a slot was emitted
       */
      private boolean binding(Expression operand) {
        if (!bind.test(operand)) {
          return false;
        }
        sql.append(new CorrelatedBindings.Reference(BINDING_KEY, bindings.size()).expression());
        bindings.add(operand);
        return true;
      }

      @Override
      public <S> StringBuilder visit(Column column, S context) {
        return binding(column) ? sql : super.visit(column, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.Function function, S context) {
        return binding(function) ? sql : super.visit(function, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.CastExpression expression, S context) {
        return binding(expression) ? sql : super.visit(expression, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.SignedExpression expression, S context) {
        return binding(expression) ? sql : super.visit(expression, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.CaseExpression expression, S context) {
        return binding(expression) ? sql : super.visit(expression, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.ExtractExpression expression, S context) {
        return binding(expression) ? sql : super.visit(expression, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.CollateExpression expression, S context) {
        return binding(expression) ? sql : super.visit(expression, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.TrimFunction expression, S context) {
        return binding(expression) ? sql : super.visit(expression, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.ArrayConstructor expression, S context) {
        return binding(expression) ? sql : super.visit(expression, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.JsonFunction expression, S context) {
        return binding(expression) ? sql : super.visit(expression, context);
      }

      @Override
      public <S> StringBuilder visit(net.sf.jsqlparser.expression.TranscodingFunction expression, S context) {
        return binding(expression) ? sql : super.visit(expression, context);
      }

      @Override
      public <S> StringBuilder visit(IsDistinctExpression expression, S context) {
        if (!binding(expression)) {
          expression.getLeftExpression().accept(this, context);
          sql.append(' ').append(expression.getStringExpression()).append(' ');
          expression.getRightExpression().accept(this, context);
        }
        return sql;
      }

      @Override
      public <S> StringBuilder visit(InExpression predicate, S context) {
        return binding(predicate) ? sql : super.visit(predicate, context);
      }

      @Override
      public <S> StringBuilder visit(ExistsExpression predicate, S context) {
        return binding(predicate) ? sql : super.visit(predicate, context);
      }

      @Override
      public <S> StringBuilder visit(Select select, S context) {
        binding(select);
        return sql;
      }

      @Override
      public <S> StringBuilder visit(ParenthesedSelect select, S context) {
        binding(select);
        return sql;
      }
      @Override
      public <S> StringBuilder deparse(OldOracleJoinBinaryExpression binary, String operator, S context) {
        // Ordinary SQL comparisons also use this JSqlParser overload.
        return binding(binary) ? sql : super.deparse(binary, operator, context);
      }

      @Override
      protected <S> void deparse(BinaryExpression binary, String operator, S context) {
        if (!binding(binary)) {
          super.deparse(binary, operator, context);
        }
      }

    };
  }

  /**
   * Evaluates the scalar using the current group's typed operand values.
   *
   * @param values
   *          resolves each bound operand
   * @return scalar value, possibly null
   * @throws IllegalArgumentException
   *           if a classified evaluation failure occurs, preserving its SQLState
   *           and original cause with the source expression as diagnostic context
   */
  Object evaluate(Function<Expression, Object> values) {
    List<Object> operands = new AbstractList<>() {
      @Override
      public Object get(int index) {
        return values.apply(bindings.get(index));
      }

      @Override
      public int size() {
        return bindings.size();
      }
    };
    var frame = new CorrelatedBindings(BINDING_KEY, operands);
    try (frame) {
      return evaluator.eval(expression, new GenericData.Record(schema));
    } catch (RuntimeException failure) {
      if (SqlExceptions.findSqlStateFailure(failure) == null) {
        throw failure;
      }
      throw new IllegalArgumentException("Failed to evaluate grouped expression: " + sourceDescription, failure);
    }
  }
}
