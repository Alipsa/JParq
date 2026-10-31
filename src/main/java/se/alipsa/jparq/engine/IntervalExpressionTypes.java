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
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.helper.IntervalQualifier;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalInterval;

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
    return resolve(expression, columns, subqueries) != null;
  }

  /**
   * Resolves a common interval field range and precision without evaluating rows.
   *
   * @param expression
   *          expression to inspect
   * @param columns
   *          input column declarations
   * @param subqueries
   *          independently scoped scalar declarations
   * @return interval schema, or null for noninterval or unknown expressions
   */
  public static Schema resolve(Expression expression, Function<Column, Schema> columns,
      Function<Select, Schema> subqueries) {
    Expression value = ExpressionEvaluator.unwrapParenthesis(expression);
    if (value instanceof CaseExpression choice) {
      return commonBranches(ExpressionBranches.caseResults(choice), columns, subqueries);
    }
    if (value instanceof AnalyticExpression analytic && analytic.getName() != null) {
      return switch (analytic.getName().toUpperCase(Locale.ROOT)) {
        case "MIN", "MAX", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE" ->
          resolve(analytic.getExpression(), columns, subqueries);
        default -> null;
      };
    }
    if (value instanceof Select select) {
      Schema declared = subqueries.apply(select);
      if (declared != null) {
        return IntervalType.matches(declared) ? JdbcTypeMapper.nonNullSchema(declared) : null;
      }
      Select inner = select;
      while (inner instanceof ParenthesedSelect parentheses) {
        inner = parentheses.getSelect();
      }
      if (inner instanceof SetOperationList set) {
        Schema common = null;
        for (Select branch : set.getSelects()) {
          common = IntervalType.common(common, resolve(branch, columns, subqueries));
        }
        return common;
      }
      return inner instanceof PlainSelect plain && plain.getSelectItems().size() == 1
          ? resolve(plain.getSelectItems().getFirst().getExpression(),
              plain.getFromItem() == null ? columns : column -> null, subqueries)
          : null;
    }
    if (value instanceof net.sf.jsqlparser.expression.Function function && function.getName() != null
        && function.getParameters() != null && !function.getParameters().isEmpty()) {
      String name = function.getName().toUpperCase(Locale.ROOT);
      if ("COALESCE".equals(name) || "NULLIF".equals(name)) {
        return commonBranches(function.getParameters(), columns, subqueries);
      }
      if (List.of("MIN", "MAX").contains(name)) {
        return resolve(function.getParameters().getFirst(), columns, subqueries);
      }
    }
    if (value instanceof IntervalExpression literal) {
      TemporalInterval interval = DateTimeFunctions.toInterval(literal);
      return IntervalType.schema(interval.qualifier(), interval.fractionalPrecision());
    }
    if (value instanceof Column column) {
      Schema schema = columns.apply(column);
      return IntervalType.matches(schema) ? JdbcTypeMapper.nonNullSchema(schema) : null;
    }
    if (value instanceof net.sf.jsqlparser.expression.Function function && CorrelatedBindings.matches(function)) {
      Schema schema = CorrelatedBindings.schema(function);
      return IntervalType.matches(schema) ? JdbcTypeMapper.nonNullSchema(schema) : null;
    }
    if (value instanceof SignedExpression signed) {
      return resolve(signed.getExpression(), columns, subqueries);
    }
    if (value instanceof Subtraction subtraction) {
      int left = temporalType(subtraction.getLeftExpression(), columns, subqueries);
      int right = temporalType(subtraction.getRightExpression(), columns, subqueries);
      if (left != Types.OTHER && right != Types.OTHER) {
        return left == Types.DATE && right == Types.DATE
            ? null
            : IntervalType.schema(IntervalQualifier.DAY_TO_SECOND,
                Math.max(temporalPrecision(subtraction.getLeftExpression(), columns, subqueries),
                    temporalPrecision(subtraction.getRightExpression(), columns, subqueries)));
      }
      return arithmetic(subtraction.getLeftExpression(), subtraction.getRightExpression(), columns, subqueries);
    }
    if (value instanceof Addition addition) {
      return arithmetic(addition.getLeftExpression(), addition.getRightExpression(), columns, subqueries);
    }
    return null;
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
   * @return the common declaration, or null if a typed branch is noninterval
   */
  private static Schema commonBranches(List<? extends Expression> branches, Function<Column, Schema> columns,
      Function<Select, Schema> subqueries) {
    Schema common = null;
    for (Expression branch : branches) {
      if (ExpressionEvaluator.unwrapParenthesis(branch) instanceof NullValue) {
        continue;
      }
      Schema declaration = resolve(branch, columns, subqueries);
      if (declaration == null) {
        return null;
      }
      common = IntervalType.common(common, declaration);
    }
    return common;
  }

  /**
   * Combines both interval operands of an arithmetic expression.
   *
   * @param left
   *          first operand
   * @param right
   *          second operand
   * @param columns
   *          input declarations
   * @param subqueries
   *          scalar declarations
   * @return common interval declaration, or null for incompatible operands
   */
  private static Schema arithmetic(Expression left, Expression right, Function<Column, Schema> columns,
      Function<Select, Schema> subqueries) {
    Schema first = resolve(left, columns, subqueries);
    Schema second = resolve(right, columns, subqueries);
    return first == null || second == null ? null : IntervalType.common(first, second);
  }

  /**
   * Resolves a datetime declaration, including its fractional precision.
   *
   * @param expression
   *          expression to inspect
   * @param columns
   *          input column declarations
   * @param subqueries
   *          independently scoped scalar declarations
   * @return datetime schema, or null for an unknown or nondatetime expression
   */
  public static Schema resolveTemporal(Expression expression, Function<Column, Schema> columns,
      Function<Select, Schema> subqueries) {
    int type = temporalType(expression, columns, subqueries);
    if (type == Types.OTHER) {
      return null;
    }
    Schema schema = type == Types.DATE
        ? org.apache.avro.LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))
        : type == Types.TIME
            ? org.apache.avro.LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))
            : org.apache.avro.LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG));
    schema.addProp("jparq.temporal.precision", Integer.toString(temporalPrecision(expression, columns, subqueries)));
    return schema;
  }

  /**
   * Combines compatible datetime declarations, keeping the larger precision.
   *
   * @param first
   *          first declaration, possibly null
   * @param second
   *          second declaration, possibly null
   * @return common datetime schema, or null for incompatible types
   */
  public static Schema commonTemporal(Schema first, Schema second) {
    if (first == null || second == null) {
      return first == null ? second : first;
    }
    if (JdbcTypeMapper.mapSchemaToJdbcType(first) != JdbcTypeMapper.mapSchemaToJdbcType(second)) {
      return null;
    }
    int left = Integer.parseInt(first.getProp("jparq.temporal.precision"));
    int right = Integer.parseInt(second.getProp("jparq.temporal.precision"));
    return left >= right ? first : second;
  }

  /**
   * Resolves conditional, aggregate, window, and scalar datetime operands.
   *
   * @param value
   *          unwrapped expression
   * @param columns
   *          input declarations
   * @param subqueries
   *          scalar declarations
   * @return common datetime declaration, or null for other expressions
   */
  private static Schema compositeTemporal(Expression value, Function<Column, Schema> columns,
      Function<Select, Schema> subqueries) {
    if (value instanceof Select select) {
      return subqueries.apply(select);
    }
    if (value instanceof AnalyticExpression analytic && analytic.getName() != null
        && List.of("MIN", "MAX", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE")
            .contains(analytic.getName().toUpperCase(Locale.ROOT))) {
      return resolveTemporal(analytic.getExpression(), columns, subqueries);
    }
    List<? extends Expression> branches = null;
    if (value instanceof CaseExpression choice) {
      branches = ExpressionBranches.caseResults(choice);
    } else if (value instanceof net.sf.jsqlparser.expression.Function function && function.getName() != null
        && function.getParameters() != null && !function.getParameters().isEmpty()) {
      String name = function.getName().toUpperCase(Locale.ROOT);
      if (List.of("MIN", "MAX").contains(name)) {
        return resolveTemporal(function.getParameters().getFirst(), columns, subqueries);
      }
      if (List.of("COALESCE", "NULLIF").contains(name)) {
        branches = function.getParameters();
      }
    }
    if (branches == null) {
      return null;
    }
    Expression widest = null;
    int type = Types.OTHER;
    int precision = -1;
    for (Expression branch : branches) {
      if (ExpressionEvaluator.unwrapParenthesis(branch) instanceof NullValue) {
        continue;
      }
      int branchType = temporalType(branch, columns, subqueries);
      if (branchType == Types.OTHER || (type != Types.OTHER && type != branchType)) {
        return null;
      }
      type = branchType;
      int digits = temporalPrecision(branch, columns, subqueries);
      if (digits > precision) {
        widest = branch;
        precision = digits;
      }
    }
    return widest == null ? null : resolveTemporal(widest, columns, subqueries);
  }

  /**
   * Resolves fractional-second digits from literal text or declared logical
   * types.
   *
   * @param expression
   *          temporal operand
   * @param columns
   *          input declarations
   * @param subqueries
   *          scalar declarations
   * @return fractional-second precision
   */
  private static int temporalPrecision(Expression expression, Function<Column, Schema> columns,
      Function<Select, Schema> subqueries) {
    Expression value = ExpressionEvaluator.unwrapParenthesis(expression);
    if (temporalType(value, columns, subqueries) == Types.DATE) {
      return 0;
    }
    Schema schema = compositeTemporal(value, columns, subqueries);
    if (schema == null) {
      schema = value instanceof Column column
          ? columns.apply(column)
          : value instanceof net.sf.jsqlparser.expression.Function function && CorrelatedBindings.matches(function)
              ? CorrelatedBindings.schema(function)
              : null;
    }
    if (schema != null) {
      Schema declaration = JdbcTypeMapper.nonNullSchema(schema);
      String precision = declaration.getProp("jparq.temporal.precision");
      if (precision != null) {
        return Integer.parseInt(precision);
      }
      String logical = declaration.getProp("logicalType");
      return logical == null
          ? 0
          : logical.endsWith("nanos") ? 9 : logical.endsWith("micros") ? 6 : logical.endsWith("millis") ? 3 : 0;
    }
    if (value instanceof CastExpression cast) {
      if (cast.getColDataType() != null && cast.getColDataType().getArgumentsStringList() != null
          && !cast.getColDataType().getArgumentsStringList().isEmpty()) {
        return Integer.parseInt(cast.getColDataType().getArgumentsStringList().getFirst());
      }
      java.util.regex.Matcher declared = java.util.regex.Pattern.compile("\\(\\s*(\\d+)\\s*\\)")
          .matcher(cast.getColDataType().toString());
      return declared.find()
          ? Integer.parseInt(declared.group(1))
          : temporalType(value, columns, subqueries) == Types.TIMESTAMP ? 6 : 0;
    }
    if (value instanceof TimeKeyExpression || value instanceof net.sf.jsqlparser.expression.Function) {
      return 9;
    }
    String text = value == null ? "" : value.toString();
    java.util.regex.Matcher fraction = java.util.regex.Pattern.compile("\\.(\\d+)").matcher(text);
    return fraction.find() ? Math.min(9, fraction.group(1).length()) : 0;
  }

  /**
   * Resolves supported temporal operands by declaration.
   *
   * @param expression
   *          possible temporal operand
   * @param columns
   *          resolver for input column schemas
   * @param subqueries
   *          scalar declarations
   * @return DATE, TIME, TIMESTAMP, or OTHER
   */
  private static int temporalType(Expression expression, Function<Column, Schema> columns,
      Function<Select, Schema> subqueries) {
    Expression value = ExpressionEvaluator.unwrapParenthesis(expression);
    Schema composite = compositeTemporal(value, columns, subqueries);
    if (composite != null) {
      return JdbcTypeMapper.mapSchemaToJdbcType(composite);
    }
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
    if (value instanceof net.sf.jsqlparser.expression.Function function) {
      type = function.getName();
    } else if (value instanceof DateTimeLiteralExpression literal) {
      type = literal.getType().name();
    } else if (value instanceof TimeKeyExpression key) {
      type = key.getStringValue();
    } else if (value instanceof CastExpression cast && cast.getColDataType() != null) {
      if (cast.isTimeStamp()) {
        return Types.TIMESTAMP;
      }
      if (cast.isTime()) {
        return Types.TIME;
      }
      if (cast.isDate()) {
        return Types.DATE;
      }
      type = cast.getColDataType().getDataType();
    }
    if (type == null) {
      return Types.OTHER;
    }
    return switch (type.toUpperCase(Locale.ROOT).replace("CURRENT_", "").replaceAll("\\([^)]*\\)", "").replace(" ",
        "")) {
      case "DATE" -> Types.DATE;
      case "TIME", "LOCALTIME" -> Types.TIME;
      case "TIMESTAMP", "LOCALTIMESTAMP" -> Types.TIMESTAMP;
      default -> Types.OTHER;
    };
  }
}
