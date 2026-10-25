package se.alipsa.jparq.engine.function;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import se.alipsa.jparq.engine.CorrelatedBindings;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;

/**
 * Evaluates scalar expressions with typed bindings for completed group values.
 */
final class GroupedExpression {
  private static final String BINDING_KEY = "grouped-expression";
  private final List<Expression> bindings = new ArrayList<>();
  private final Expression expression;
  private final Schema schema = Schema.createRecord("grouped", null, null, false, List.of());

  /**
   * Compiles a scalar expression, replacing group-dependent operands with slots.
   *
   * @param source
   *          source expression
   * @param bind
   *          identifies operands supplied by the group evaluator
   * @throws IllegalArgumentException
   *           if compilation or operand validation fails
   */
  GroupedExpression(Expression source, Predicate<Expression> bind) {
    StringBuilder sql = new StringBuilder();
    ExpressionDeParser deparser = new ExpressionDeParser(null, sql) {
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
      protected <S> void deparse(BinaryExpression binary, String operator, S context) {
        if (!binding(binary)) {
          super.deparse(binary, operator, context);
        }
      }

    };
    source.accept(deparser, null);
    try {
      expression = CCJSqlParserUtil.parseExpression(sql.toString());
    } catch (net.sf.jsqlparser.JSQLParserException e) {
      throw new IllegalArgumentException("Cannot compile grouped expression: " + source, e);
    }
  }

  /**
   * Evaluates the scalar using the current group's typed operand values.
   *
   * @param values
   *          resolves each bound operand
   * @return scalar value, possibly null
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
      return new ValueExpressionEvaluator(schema).eval(expression, new GenericData.Record(schema));
    }
  }
}
