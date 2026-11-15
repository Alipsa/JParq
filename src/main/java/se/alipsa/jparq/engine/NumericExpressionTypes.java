package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
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
import net.sf.jsqlparser.statement.select.Select;
import org.apache.avro.Schema;
import se.alipsa.jparq.NumericType;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/** Resolves numeric expression types from declarations before reading rows. */
public final class NumericExpressionTypes {
  private static final Set<String> AGGREGATES = Set.of("COUNT", "SUM", "AVG", "MIN", "MAX");

  /** Prevents utility class instantiation. */
  private NumericExpressionTypes() {
  }

  /**
   * Recognizes the aggregate functions whose numeric result rules are shared.
   *
   * @param name
   *          case-insensitive function name, possibly null
   * @return whether the function is COUNT, SUM, AVG, MIN, or MAX
   */
  public static boolean isAggregate(String name) {
    return name != null && AGGREGATES.contains(name.toUpperCase(Locale.ROOT));
  }

  /**
   * Resolves literals, numeric casts, regular and window aggregates, CASE, and
   * arithmetic operators.
   *
   * @param expression
   *          the expression to resolve
   * @param columns
   *          declared numeric types of source columns
   * @return the numeric type, or null when the type is unknown or nonnumeric
   */
  public static NumericType resolve(Expression expression, Function<Column, NumericType> columns) {
    return resolve(expression, columns, select -> null);
  }

  /**
   * Resolves numeric declarations without executing scalar subqueries or
   * functions.
   *
   * @param expression
   *          expression to resolve
   * @param columns
   *          declared source column types
   * @param scalarSchemas
   *          independently scoped scalar subquery declarations
   * @return numeric type, or null for unknown or nonnumeric declarations
   */
  public static NumericType resolve(Expression expression, Function<Column, NumericType> columns,
      Function<Select, Schema> scalarSchemas) {
    return resolve(expression, columns, scalarSchemas, select -> numeric(scalarSchemas.apply(select)));
  }

  /**
   * Resolves numeric types including scalar projections with unbounded decimals.
   *
   * @param expression
   *          expression to resolve
   * @param columns
   *          declared source column types
   * @param scalarSchemas
   *          scalar schemas used to recognize untyped NULL
   * @param scalarNumbers
   *          numeric scalar declarations, including decimals without fixed bounds
   * @return numeric type, or null when unresolved or nonnumeric
   */
  public static NumericType resolve(Expression expression, Function<Column, NumericType> columns,
      Function<Select, Schema> scalarSchemas, Function<Select, NumericType> scalarNumbers) {
    if (expression instanceof Select select) {
      NumericType declared = scalarNumbers.apply(select);
      return declared == null ? numeric(scalarSchemas.apply(select)) : declared;
    }
    if (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return resolve(list.getFirst(), columns, scalarSchemas, scalarNumbers);
    }
    if (expression instanceof Column column) {
      return columns.apply(column);
    }
    if (expression instanceof CaseExpression choice) {
      return commonBranches(ExpressionBranches.caseResults(choice), columns, scalarSchemas, scalarNumbers);
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
        NumericType operand = resolve(cast.getLeftExpression(), columns, scalarSchemas, scalarNumbers);
        return declared != null
            ? NumericType.of(Types.DECIMAL, declared.getPrecision(), declared.getScale())
            : operand == null || operand.jdbcType() == Types.DOUBLE || operand.jdbcType() == Types.REAL
                ? new NumericType(Types.DECIMAL, 0, 0)
                : new NumericType(Types.DECIMAL, operand.precision(), operand.scale());
      }
      NumericCastTarget target = NumericCastTarget.fromName(name);
      return target == null ? null : target.numericType();
    }
    if (expression instanceof SignedExpression signed) {
      if (signed.getSign() == '-' && signed.getExpression() instanceof LongValue literal
          && literal.getBigIntegerValue().bitLength() >= 64 && literal.getBigIntegerValue().negate().bitLength() < 64) {
        return NumericType.of(Types.BIGINT, 0, 0);
      }
      NumericType operand = resolve(signed.getExpression(), columns, scalarSchemas, scalarNumbers);
      return operand == null
          ? null
          : operand.jdbcType() == Types.DECIMAL ? operand : operand.arithmetic(operand, Operation.ADD);
    }
    if (expression instanceof net.sf.jsqlparser.expression.Function function && function.getName() != null) {
      if (CorrelatedBindings.matches(function)) {
        return numeric(CorrelatedBindings.schema(function));
      }
      String name = function.getName().toUpperCase(Locale.ROOT);
      NumericType declared = scalarFunctionType(name);
      if (declared != null) {
        return declared;
      }
      if (("COALESCE".equals(name) || "IFNULL".equals(name)) && function.getParameters() != null) {
        return commonBranches(function.getParameters(), columns, scalarSchemas, scalarNumbers);
      }
      Expression argument = function.getParameters() == null || function.getParameters().isEmpty()
          ? null
          : function.getParameters().getFirst();
      if ("NULLIF".equals(name)) {
        return resolve(argument, columns, scalarSchemas, scalarNumbers);
      }
      NumericType aggregate = aggregateType(name, argument, columns, scalarSchemas, scalarNumbers);
      if (aggregate != null) {
        return aggregate;
      }
      if ("ABS".equals(name) && argument != null) {
        NumericType type = resolve(argument, columns, scalarSchemas, scalarNumbers);
        return type == null
            ? null
            : type.jdbcType() == Types.REAL || type.jdbcType() == Types.DOUBLE
                ? new NumericType(Types.DECIMAL, 0, 0)
                : new NumericType(Types.DECIMAL, type.precision(), type.scale());
      }
    }
    if (expression instanceof AnalyticExpression analytic && analytic.getName() != null) {
      return aggregateType(analytic.getName().toUpperCase(Locale.ROOT), analytic.getExpression(), columns,
          scalarSchemas, scalarNumbers);
    }
    Operation operation = operation(expression);
    if (operation != null && expression instanceof BinaryExpression binary) {
      NumericType left = resolve(binary.getLeftExpression(), columns, scalarSchemas, scalarNumbers);
      NumericType right = resolve(binary.getRightExpression(), columns, scalarSchemas, scalarNumbers);
      if (left == null && isNull(binary.getLeftExpression(), scalarSchemas)) {
        left = right;
      }
      if (right == null && isNull(binary.getRightExpression(), scalarSchemas)) {
        right = left;
      }
      return left == null || right == null ? null : left.arithmetic(right, operation);
    }
    return null;
  }

  /**
   * Shares numeric result declarations between regular and window aggregates.
   *
   * @param name
   *          normalized function name
   * @param argument
   *          aggregate operand, or null for COUNT(*)
   * @param columns
   *          declared numeric source column types
   * @param scalarSchemas
   *          scalar subquery declarations
   * @param scalarNumbers
   *          numeric scalar declarations
   * @return aggregate result type, or null for unknown or nonnumeric operands
   */
  private static NumericType aggregateType(String name, Expression argument, Function<Column, NumericType> columns,
      Function<Select, Schema> scalarSchemas, Function<Select, NumericType> scalarNumbers) {
    if ("COUNT".equals(name)) {
      return NumericType.of(Types.BIGINT, 0, 0);
    }
    if (!isAggregate(name) || argument == null) {
      return null;
    }
    NumericType type = resolve(argument, columns, scalarSchemas, scalarNumbers);
    if (type == null) {
      return null;
    }
    return switch (name) {
      case "SUM" -> switch (type.jdbcType()) {
        case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> NumericType.of(Types.BIGINT, 0, 0);
        case Types.REAL -> NumericType.of(Types.DOUBLE, 0, 0);
        case Types.DECIMAL ->
          type.precision() == 0 ? type : NumericType.of(Types.DECIMAL, type.precision() + 19, type.scale());
        default -> type;
      };
      case "AVG" -> NumericType.of(Types.DOUBLE, 0, 0);
      default -> type;
    };
  }

  /**
   * Combines numeric result branches without evaluating conditions or values.
   * Untyped NULL branches do not constrain the result type.
   *
   * @param branches
   *          CASE or COALESCE result branches
   * @param columns
   *          declared numeric source column types
   * @param scalarSchemas
   *          scalar subquery declarations
   * @param scalarNumbers
   *          numeric scalar declarations
   * @return common numeric type, or null if any non-NULL branch is unresolved
   */
  private static NumericType commonBranches(List<? extends Expression> branches, Function<Column, NumericType> columns,
      Function<Select, Schema> scalarSchemas, Function<Select, NumericType> scalarNumbers) {
    NumericType common = null;
    boolean unknownDecimal = false;
    for (Expression branch : branches) {
      if (isNull(branch, scalarSchemas)) {
        continue;
      }
      NumericType type = resolve(branch, columns, scalarSchemas, scalarNumbers);
      if (type == null) {
        return null;
      }
      unknownDecimal |= type.jdbcType() == Types.DECIMAL && type.precision() == 0;
      common = common == null ? type : common.common(type);
    }
    return common != null && common.jdbcType() == Types.DECIMAL && unknownDecimal
        ? new NumericType(Types.DECIMAL, 0, 0)
        : common;
  }

  /**
   * Maps fixed scalar function declarations to their evaluated numeric families.
   *
   * @param name
   *          normalized function name
   * @return declared numeric type, or null for operand-dependent or nonnumeric
   *         functions
   */
  private static NumericType scalarFunctionType(String name) {
    return switch (name) {
      case "ROUND", "CEIL", "CEILING", "FLOOR", "TRUNC", "TRUNCATE", "MOD" -> new NumericType(Types.DECIMAL, 0, 0);
      case "SQRT", "POWER", "POW", "EXP", "LOG", "LOG10", "RAND", "RANDOM", "SIN", "COS", "TAN", "COT", "ASIN", "ACOS",
          "ATAN", "ATAN2", "DEGREES", "RADIANS", "PI" ->
        NumericType.of(Types.DOUBLE, 0, 0);
      case "SIGN", "ASCII", "UNICODE", "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "OCTET_LENGTH", "POSITION",
          "LOCATE", "DIFFERENCE", "DAYOFWEEK", "DAYOFMONTH", "DAYOFYEAR", "HOUR", "MINUTE", "SECOND", "MONTH", "YEAR",
          "QUARTER", "WEEK" ->
        NumericType.of(Types.INTEGER, 0, 0);
      case "TIMESTAMPDIFF" -> NumericType.of(Types.BIGINT, 0, 0);
      default -> null;
    };
  }

  /**
   * Reads a numeric declaration from a nullable value schema.
   *
   * @param schema
   *          declared schema, possibly null
   * @return numeric descriptor, or null for unknown or nonnumeric schemas
   */
  private static NumericType numeric(Schema schema) {
    Schema value = JdbcTypeMapper.nonNullSchema(schema);
    return value == null ? null : NumericType.of(JdbcTypeMapper.mapSchemaToJdbcType(value), value);
  }

  /**
   * Converts a numeric result while preserving scales of unbounded decimals.
   *
   * @param type
   *          the common result type
   * @param value
   *          selected or grouped result, possibly null
   * @return value converted to the common Java representation
   * @throws SQLException
   *           if conversion loses numeric data
   */
  public static Object convert(NumericType type, Object value) throws SQLException {
    if (value == null) {
      return null;
    }
    if (type.jdbcType() == Types.DECIMAL && type.precision() == 0) {
      return value instanceof BigDecimal ? value : new BigDecimal(value.toString());
    }
    return type.convert(value);
  }

  /**
   * Recognizes NULL operands through scalar parentheses.
   *
   * @param expression
   *          operand expression
   * @param scalarSchemas
   *          scalar subquery declarations
   * @return whether the operand is an untyped NULL
   */
  private static boolean isNull(Expression expression, Function<Select, Schema> scalarSchemas) {
    if (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return isNull(list.getFirst(), scalarSchemas);
    }
    if (expression instanceof Select select) {
      Schema schema = JdbcTypeMapper.nonNullSchema(scalarSchemas.apply(select));
      return schema != null && schema.getType() == Schema.Type.NULL;
    }
    if (expression instanceof net.sf.jsqlparser.expression.Function function && CorrelatedBindings.matches(function)) {
      Schema schema = JdbcTypeMapper.nonNullSchema(CorrelatedBindings.schema(function));
      return schema != null && schema.getType() == Schema.Type.NULL;
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
