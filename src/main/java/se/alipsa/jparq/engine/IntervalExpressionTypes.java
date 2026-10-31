package se.alipsa.jparq.engine;

import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import org.apache.avro.Schema;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/**
 * Resolves interval declarations without evaluating rows or losing empty-result
 * metadata.
 */
public final class IntervalExpressionTypes {

  /** Prevents utility class instantiation. */
  private IntervalExpressionTypes() {
  }

  /**
   * Recognizes interval literals, arithmetic, and typed input columns.
   *
   * @param expression
   *          expression to inspect
   * @param columns
   *          resolver for input column schemas
   * @return whether the expression has interval type
   */
  public static boolean isInterval(Expression expression, Function<Column, Schema> columns) {
    return isInterval(expression, columns, select -> null);
  }

  /**
   * Resolves interval types with an independent schema scope for scalar
   * subqueries.
   *
   * @param expression
   *          projected expression
   * @param columns
   *          input column schema resolver
   * @param subqueries
   *          resolver for a scalar subquery's declared output schema
   * @return whether the result has interval type, independently of returned rows
   */
  public static boolean isInterval(Expression expression, Function<Column, Schema> columns,
      Function<Select, Schema> subqueries) {
    Expression value = ExpressionEvaluator.unwrapParenthesis(expression);
    if (value instanceof CaseExpression choice) {
      return commonBranches(ExpressionBranches.caseResults(choice), columns, subqueries);
    }
    if (value instanceof AnalyticExpression analytic && analytic.getName() != null) {
      return switch (analytic.getName().toUpperCase(Locale.ROOT)) {
        case "MIN", "MAX", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE" ->
          isInterval(analytic.getExpression(), columns, subqueries);
        default -> false;
      };
    }
    if (value instanceof Select select) {
      Schema declared = subqueries.apply(select);
      if (declared != null) {
        return IntervalType.matches(declared);
      }
      Select inner = select;
      while (inner instanceof ParenthesedSelect parentheses) {
        inner = parentheses.getSelect();
      }
      if (inner instanceof SetOperationList set && !set.getSelects().isEmpty()) {
        inner = set.getSelects().getFirst();
      }
      return inner instanceof PlainSelect plain && plain.getSelectItems().size() == 1
          && isInterval(plain.getSelectItems().getFirst().getExpression(),
              plain.getFromItem() == null ? columns : column -> null, subqueries);
    }
    if (value instanceof net.sf.jsqlparser.expression.Function function && function.getName() != null
        && function.getParameters() != null && !function.getParameters().isEmpty()) {
      String name = function.getName().toUpperCase(Locale.ROOT);
      if ("COALESCE".equals(name) || "NULLIF".equals(name)) {
        return commonBranches(function.getParameters(), columns, subqueries);
      }
      if (List.of("MIN", "MAX").contains(name)) {
        return isInterval(function.getParameters().getFirst(), columns, subqueries);
      }
    }
    if (value instanceof IntervalExpression) {
      return true;
    }
    if (value instanceof Column column) {
      return IntervalType.matches(columns.apply(column));
    }
    if (value instanceof net.sf.jsqlparser.expression.Function function && CorrelatedBindings.matches(function)) {
      return IntervalType.matches(CorrelatedBindings.schema(function));
    }
    if (value instanceof SignedExpression signed) {
      return isInterval(signed.getExpression(), columns, subqueries);
    }
    if (value instanceof Subtraction subtraction) {
      int left = temporalType(subtraction.getLeftExpression(), columns);
      int right = temporalType(subtraction.getRightExpression(), columns);
      if (left != Types.OTHER && right != Types.OTHER) {
        return !(left == Types.DATE && right == Types.DATE);
      }
      return isInterval(subtraction.getLeftExpression(), columns, subqueries)
          && isInterval(subtraction.getRightExpression(), columns, subqueries);
    }
    if (value instanceof Addition addition) {
      return isInterval(addition.getLeftExpression(), columns, subqueries)
          && isInterval(addition.getRightExpression(), columns, subqueries);
    }
    return false;
  }

  /**
   * Resolves CASE and COALESCE results without evaluating branches.
   *
   * @param branches
   *          possible result expressions
   * @param columns
   *          input column resolver
   * @param subqueries
   *          scalar subquery schema resolver
   * @return whether every typed branch is an interval and at least one is typed
   */
  private static boolean commonBranches(List<? extends Expression> branches, Function<Column, Schema> columns,
      Function<Select, Schema> subqueries) {
    boolean interval = false;
    for (Expression branch : branches) {
      if (ExpressionEvaluator.unwrapParenthesis(branch) instanceof NullValue) {
        continue;
      }
      if (!isInterval(branch, columns, subqueries)) {
        return false;
      }
      interval = true;
    }
    return interval;
  }

  /**
   * Resolves supported temporal operands by declaration.
   *
   * @param expression
   *          possible temporal operand
   * @param columns
   *          resolver for input column schemas
   * @return DATE, TIME, TIMESTAMP, or OTHER
   */
  private static int temporalType(Expression expression, Function<Column, Schema> columns) {
    Expression value = ExpressionEvaluator.unwrapParenthesis(expression);
    if (value instanceof Column column) {
      int type = JdbcTypeMapper.mapSchemaToJdbcType(columns.apply(column));
      return type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP ? type : Types.OTHER;
    }
    if (value instanceof DateValue) {
      return Types.DATE;
    }
    if (value instanceof TimeValue) {
      return Types.TIME;
    }
    if (value instanceof TimestampValue) {
      return Types.TIMESTAMP;
    }
    if (value instanceof net.sf.jsqlparser.expression.Function function && CorrelatedBindings.matches(function)) {
      return JdbcTypeMapper.mapSchemaToJdbcType(CorrelatedBindings.schema(function));
    }
    String type = null;
    if (value instanceof DateTimeLiteralExpression literal) {
      type = literal.getType().name();
    } else if (value instanceof TimeKeyExpression key) {
      type = key.getStringValue();
    } else if (value instanceof CastExpression cast && cast.getColDataType() != null) {
      type = cast.getColDataType().getDataType();
    }
    if (type == null) {
      return Types.OTHER;
    }
    return switch (type.toUpperCase(Locale.ROOT).replace("CURRENT_", "").replace("()", "")) {
      case "DATE" -> Types.DATE;
      case "TIME" -> Types.TIME;
      case "TIMESTAMP", "LOCALTIMESTAMP" -> Types.TIMESTAMP;
      default -> Types.OTHER;
    };
  }
}
