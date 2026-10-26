package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Locale;
import java.util.function.Function;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import se.alipsa.jparq.NumericType;

/** Resolves numeric expression types from declarations before reading rows. */
public final class NumericExpressionTypes {
  /** Prevents utility class instantiation. */
  private NumericExpressionTypes() {
  }

  /**
   * Resolves literals, numeric casts, aggregates, and arithmetic operators.
   *
   * @param expression
   *          the expression to resolve
   * @param columns
   *          declared numeric types of source columns
   * @return the numeric type, or null when the type is unknown or nonnumeric
   */
  public static NumericType resolve(Expression expression, Function<Column, NumericType> columns) {
    if (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return resolve(list.getFirst(), columns);
    }
    if (expression instanceof Column column) {
      return columns.apply(column);
    }
    if (expression instanceof LongValue literal) {
      var integer = literal.getBigIntegerValue();
      if (integer.bitLength() < 32) {
        return new NumericType(Types.INTEGER, integer.abs().toString().length(), 0);
      }
      return integer.bitLength() < 64
          ? new NumericType(Types.BIGINT, integer.abs().toString().length(), 0)
          : decimal(new BigDecimal(integer));
    }
    if (expression instanceof DoubleValue literal) {
      return decimal(new BigDecimal(literal.toString()));
    }
    if (expression instanceof CastExpression cast && cast.getColDataType() != null) {
      String name = cast.getColDataType().getDataType().replace(" ", "").toUpperCase(Locale.ROOT);
      if (DecimalCasts.isDecimalType(name)) {
        var declared = DecimalCasts.declaredType(cast);
        NumericType operand = resolve(cast.getLeftExpression(), columns);
        return declared != null
            ? NumericType.of(Types.DECIMAL, declared.getPrecision(), declared.getScale())
            : operand == null || operand.jdbcType() == Types.DOUBLE || operand.jdbcType() == Types.REAL
                ? new NumericType(Types.DECIMAL, 0, 0)
                : new NumericType(Types.DECIMAL, operand.precision(), operand.scale());
      }
      return switch (name) {
        case "TINYINT" -> new NumericType(Types.TINYINT, 3, 0);
        case "SMALLINT", "INT2" -> new NumericType(Types.SMALLINT, 5, 0);
        case "INT", "INTEGER", "SIGNED", "INT4" -> NumericType.of(Types.INTEGER, 0, 0);
        case "BIGINT", "INT8" -> NumericType.of(Types.BIGINT, 0, 0);
        case "FLOAT", "REAL", "FLOAT4" -> NumericType.of(Types.REAL, 0, 0);
        case "DOUBLE", "DOUBLEPRECISION", "FLOAT8" -> NumericType.of(Types.DOUBLE, 0, 0);
        default -> null;
      };
    }
    if (expression instanceof SignedExpression signed) {
      if (signed.getSign() == '-' && signed.getExpression() instanceof LongValue literal
          && literal.getBigIntegerValue().bitLength() >= 64 && literal.getBigIntegerValue().negate().bitLength() < 64) {
        return NumericType.of(Types.BIGINT, 0, 0);
      }
      NumericType operand = resolve(signed.getExpression(), columns);
      return operand == null
          ? null
          : operand.jdbcType() == Types.DECIMAL ? operand : operand.arithmetic(operand, Operation.ADD);
    }
    if (expression instanceof net.sf.jsqlparser.expression.Function function && function.getName() != null) {
      String name = function.getName().toUpperCase(Locale.ROOT);
      if ("COUNT".equals(name)) {
        return NumericType.of(Types.BIGINT, 0, 0);
      }
      if (function.getParameters() != null && !function.getParameters().isEmpty()) {
        NumericType argument = resolve(function.getParameters().getFirst(), columns);
        if (argument != null) {
          return switch (name) {
            case "SUM" -> switch (argument.jdbcType()) {
              case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> NumericType.of(Types.BIGINT, 0, 0);
              case Types.REAL -> NumericType.of(Types.DOUBLE, 0, 0);
              case Types.DECIMAL -> argument.precision() == 0
                  ? argument
                  : NumericType.of(Types.DECIMAL, argument.precision() + 19, argument.scale());
              default -> argument;
            };
            case "AVG" -> NumericType.of(Types.DOUBLE, 0, 0);
            case "MIN", "MAX" -> argument;
            default -> null;
          };
        }
      }
    }
    Operation operation = operation(expression);
    if (operation != null && expression instanceof BinaryExpression binary) {
      NumericType left = resolve(binary.getLeftExpression(), columns);
      NumericType right = resolve(binary.getRightExpression(), columns);
      if (left == null && isNull(binary.getLeftExpression())) {
        left = right;
      }
      if (right == null && isNull(binary.getRightExpression())) {
        right = left;
      }
      return left == null || right == null ? null : left.arithmetic(right, operation);
    }
    return null;
  }

  /**
   * Recognizes NULL operands through scalar parentheses.
   *
   * @param expression
   *          operand expression
   * @return whether the operand is an untyped NULL
   */
  private static boolean isNull(Expression expression) {
    if (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return isNull(list.getFirst());
    }
    return expression instanceof NullValue;
  }

  /**
   * Maps a parsed arithmetic expression to its operator.
   *
   * @param expression
   *          parsed expression
   * @return the operator, or null for other expressions
   */
  public static Operation operation(Expression expression) {
    if (expression instanceof Addition) {
      return Operation.ADD;
    }
    if (expression instanceof Subtraction) {
      return Operation.SUB;
    }
    if (expression instanceof Multiplication) {
      return Operation.MUL;
    }
    if (expression instanceof Division) {
      return Operation.DIV;
    }
    return expression instanceof Modulo ? Operation.MOD : null;
  }

  /**
   * Describes an exact decimal literal, including leading fractional zeros.
   *
   * @param value
   *          literal value
   * @return its decimal bounds
   */
  private static NumericType decimal(BigDecimal value) {
    int scale = Math.max(0, value.scale());
    return NumericType.of(Types.DECIMAL, Math.max(value.precision() - value.scale(), 0) + scale, scale);
  }
}
