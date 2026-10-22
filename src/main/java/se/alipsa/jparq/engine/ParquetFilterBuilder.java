package se.alipsa.jparq.engine;

import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static se.alipsa.jparq.engine.ExpressionEvaluator.unwrapParenthesis;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import se.alipsa.jparq.helper.LiteralConverter;

/**
 * Best-effort translator from JSQLParser Expression -> Parquet FilterPredicate.
 */
public final class ParquetFilterBuilder {
  private static final ThreadLocal<Boolean> BOUND_VALUES = new ThreadLocal<>();

  private ParquetFilterBuilder() {
  }

  /**
   * Translates predicates using the current correlated binding frame. This scope
   * is used at execution time for reusable plans and during construction of
   * per-row join statements. Predicates built here capture current values and
   * must not be retained across binding frames.
   *
   * @param translation
   *          predicate translation to perform
   * @param <T>
   *          translation result type
   * @return translation result
   */
  public static <T> T withCorrelatedBindings(Supplier<T> translation) {
    Boolean previous = BOUND_VALUES.get();
    BOUND_VALUES.set(true);
    try {
      return translation.get();
    } finally {
      if (previous == null) {
        BOUND_VALUES.remove();
      } else {
        BOUND_VALUES.set(previous);
      }
    }
  }

  /**
   * Try to build a Parquet FilterPredicate; return empty if expression isn’t
   * fully pushdownable.
   *
   * @param avroSchema
   *          the Avro schema of the Parquet data
   * @param where
   *          the WHERE expression
   * @return optional FilterPredicate
   */
  public static Optional<FilterPredicate> build(Schema avroSchema, Expression where) {
    if (where == null) {
      return Optional.empty();
    }
    return tryBuild(avroSchema, normalize(where, false));
  }

  /**
   * Internal recursive builder for Parquet FilterPredicate.
   *
   * @param schema
   *          the Avro schema of the Parquet data
   * @param expression
   *          the expression subtree to translate
   * @return an Optional with the predicate if the subtree is fully supported;
   *         empty otherwise
   */
  private static Optional<FilterPredicate> tryBuild(Schema schema, Expression expression) {
    Expression exp = unwrap(expression);

    if (exp instanceof AndExpression and) {
      var l = tryBuild(schema, and.getLeftExpression());
      var r = tryBuild(schema, and.getRightExpression());
      if (l.isPresent() && r.isPresent()) {
        return Optional.of(and(l.get(), r.get()));
      }
      return Optional.empty();
    }
    if (exp instanceof OrExpression or) {
      var l = tryBuild(schema, or.getLeftExpression());
      var r = tryBuild(schema, or.getRightExpression());
      if (l.isPresent() && r.isPresent()) {
        return Optional.of(or(l.get(), r.get()));
      }
      return Optional.empty();
    }
    if (exp instanceof IsNullExpression isNull && isNull.getLeftExpression() instanceof Column col) {
      Schema.Field field = findFieldCaseInsensitive(schema, col.getColumnName());
      if (field == null) {
        return Optional.empty();
      }
      Schema base = AvroCoercions.effectiveSchema(field.schema());
      String name = field.name();
      FilterPredicate predicate = switch (base.getType()) {
        case INT -> isNull.isNot() ? notEq(intColumn(name), null) : eq(intColumn(name), null);
        case LONG -> isNull.isNot() ? notEq(longColumn(name), null) : eq(longColumn(name), null);
        case FLOAT -> isNull.isNot() ? notEq(floatColumn(name), null) : eq(floatColumn(name), null);
        case DOUBLE -> isNull.isNot() ? notEq(doubleColumn(name), null) : eq(doubleColumn(name), null);
        case BOOLEAN -> isNull.isNot() ? notEq(booleanColumn(name), null) : eq(booleanColumn(name), null);
        case STRING -> isNull.isNot() ? notEq(binaryColumn(name), null) : eq(binaryColumn(name), null);
        default -> null;
      };
      return Optional.ofNullable(predicate);
    }
    if (exp instanceof Between b) {
      // Only a column on the left guarantees that the upper bound rejects NaN.
      boolean standalone = !(b.getLeftExpression() instanceof Column);
      var ge = comparison(schema, new GreaterThanEquals(b.getLeftExpression(), b.getBetweenExpressionStart()),
          standalone);
      var le = comparison(schema, new MinorThanEquals(b.getLeftExpression(), b.getBetweenExpressionEnd()), standalone);
      if (ge.isPresent() && le.isPresent()) {
        FilterPredicate p = and(ge.get(), le.get());
        return Optional.of(p);
      }
      return Optional.empty();
    }

    if (exp instanceof InExpression in) {
      if (!(in.getRightExpression() instanceof ExpressionList<?> list)) {
        return Optional.empty();
      }
      if (list.isEmpty() || list.size() > 20) {
        return Optional.empty();
      }
      List<FilterPredicate> eqs = new ArrayList<>();
      for (Expression val : list) {
        var eq = comparison(schema, new EqualsTo(in.getLeftExpression(), val), true);
        if (eq.isEmpty()) {
          return Optional.empty();
        }
        eqs.add(eq.get());
      }
      FilterPredicate disj = eqs.getFirst();
      for (int i = 1; i < eqs.size(); i++) {
        disj = or(disj, eqs.get(i));
      }
      return Optional.of(disj);
    }

    return comparison(schema, exp, true);
  }

  private static Schema.Field findFieldCaseInsensitive(Schema schema, String name) {
    Schema.Field f = schema.getField(name);
    if (f != null) {
      return f;
    }
    String target = Identifier.lookupKey(name);
    for (Schema.Field sf : schema.getFields()) {
      if (target != null && target.equals(Identifier.lookupKey(sf.name()))) {
        return sf;
      }
    }
    return null;
  }

  /**
   * Build a simple column-operator-literal comparison predicate; returns empty if
   * the expression is not of supported form.
   *
   * @param schema
   *          the Avro schema of the Parquet data
   * @param e
   *          the expression to translate
   * @param standalone
   *          {@code true} when the comparison is pushed on its own, so it must
   *          not match NaN on FLOAT and DOUBLE columns (which PyArrow leaves out
   *          of the statistics); {@code false} when another conjunct rejects NaN
   * @return optional FilterPredicate if pushdownable; empty otherwise
   */
  private static Optional<FilterPredicate> comparison(Schema schema, Expression e, boolean standalone) {
    if (!(e instanceof BinaryExpression be)) {
      return Optional.empty();
    }

    Expression l = unwrap(be.getLeftExpression());
    Expression r = unwrap(be.getRightExpression());

    boolean leftIsCol = l instanceof Column;
    boolean rightIsCol = r instanceof Column;

    if (leftIsCol && !rightIsCol) {
      return buildColOpLit(schema, (Column) l, be, r, standalone);
    } else if (!leftIsCol && rightIsCol) {
      // swap: lit <op> col => col <op-swapped> lit
      return buildColOpLit(schema, (Column) r, swap(be), l, standalone);
    }
    return Optional.empty();
  }

  /**
   * Convert \"LIT OP COL\" into \"COL OP' LIT\" by swapping operator direction.
   *
   * @param be
   *          the original binary operator
   * @return a BinaryExpression representing the swapped operator
   */
  private static BinaryExpression swap(BinaryExpression be) {
    String op = be.getStringExpression();
    return switch (op) {
      case "=" -> new EqualsTo();
      case "<" -> new GreaterThan();
      case ">" -> new MinorThan();
      case "<=" -> new GreaterThanEquals();
      case ">=" -> new MinorThanEquals();
      case "<>", "!=" -> new NotEqualsTo();
      default -> be;
    };
  }

  /**
   * Build a predicate for column vs literal using the provided operator.
   *
   * @param avroSchema
   *          the Avro schema of the Parquet data
   * @param colExpr
   *          the column expression
   * @param op
   *          the operator (already adjusted for operand order)
   * @param operand
   *          the literal expression, possibly wrapped in parentheses
   * @param standalone
   *          whether the comparison is pushed on its own (see
   *          {@link #comparison(Schema, Expression, boolean)})
   * @return optional FilterPredicate; empty if schema/type or operator is
   *         unsupported
   */
  private static Optional<FilterPredicate> buildColOpLit(Schema avroSchema, Column colExpr, BinaryExpression op,
      Expression operand, boolean standalone) {
    String col = colExpr.getColumnName();
    Expression litExpr = unwrap(operand);
    Schema.Field f = findFieldCaseInsensitive(avroSchema, col);
    if (f == null) {
      return Optional.empty();
    }

    if (!isLiteral(litExpr)) {
      return Optional.empty();
    }

    Schema effective = AvroCoercions.effectiveSchema(f.schema());
    col = f.name();
    Object lit = LiteralConverter.toLiteral(litExpr);
    if (!(litExpr instanceof StringValue)) {
      try {
        lit = new BigDecimal(litExpr.toString());
      } catch (NumberFormatException ignored) {
        /* Keep nonnumeric literals. */ }
    }
    Object coerced = AvroCoercions.coerceLiteral(lit, effective);
    if (coerced == null) {
      return Optional.empty();
    }
    try {
      if (effective.getLogicalType() instanceof LogicalTypes.Decimal decimal) {
        return decimalPredicate(effective, decimal, col, op, lit);
      }
      if (LogicalTypes.date().equals(effective.getLogicalType())) {
        if (!(coerced instanceof Date date)) {
          return Optional.empty();
        }
        coerced = date.toLocalDate().toEpochDay();
      } else if (AvroCoercions.isTimestamp(effective)) {
        if (!(coerced instanceof Timestamp timestamp)) {
          return Optional.empty();
        }
        long seconds = AvroCoercions.isLocalTimestamp(effective)
            ? timestamp.toLocalDateTime().toEpochSecond(ZoneOffset.UTC)
            : timestamp.toInstant().getEpochSecond();
        long units = AvroCoercions.timestampUnits(effective);
        coerced = BigDecimal.valueOf(seconds).multiply(BigDecimal.valueOf(units))
            .add(BigDecimal.valueOf(timestamp.getNanos()).multiply(BigDecimal.valueOf(units)).movePointLeft(9));
      }
      return switch (effective.getType()) {
        case INT -> {
          BigDecimal bound = integralBound(coerced, op, Integer.MIN_VALUE, Integer.MAX_VALUE);
          yield bound == null
              ? Optional.empty()
              : Optional.ofNullable(buildInt(intColumn(col), op, bound.intValueExact()));
        }
        case LONG -> {
          BigDecimal bound = integralBound(coerced, op, Long.MIN_VALUE, Long.MAX_VALUE);
          yield bound == null
              ? Optional.empty()
              : Optional.ofNullable(buildLong(longColumn(col), op, bound.longValueExact()));
        }
        case FLOAT -> {
          Float value = floatLiteral(coerced);
          yield value == null || !floatingOperatorAllowed(op, standalone)
              ? Optional.empty()
              : Optional.ofNullable(buildFloat(floatColumn(col), op, value));
        }
        case DOUBLE -> {
          Double value = doubleLiteral(coerced);
          yield value == null || !floatingOperatorAllowed(op, standalone)
              ? Optional.empty()
              : Optional.ofNullable(buildDouble(doubleColumn(col), op, value));
        }
        case BOOLEAN -> coerced instanceof Boolean bool
            ? Optional.ofNullable(buildBoolean(booleanColumn(col), op, bool))
            : Optional.empty();
        case STRING -> Optional.ofNullable(buildBinary(binaryColumn(col), op, Binary.fromString(coerced.toString())));
        default -> Optional.empty();
      };
    } catch (ArithmeticException | NumberFormatException exception) {
      return Optional.empty();
    }
  }

  /**
   * Convert a literal to the exact float the column compares against.
   *
   * @param coerced
   *          the coerced literal
   * @return the float value, or {@code null} when pushing it down could disagree
   *         with the Java engine: a non-numeric, non-finite or zero value (the
   *         record-level filter orders {@code -0.0} below {@code 0.0}), or a
   *         decimal that is not the shortest representation of the float
   */
  private static Float floatLiteral(Object coerced) {
    if (!(coerced instanceof BigDecimal exact)) {
      return null;
    }
    float value = exact.floatValue();
    if (!Float.isFinite(value) || value == 0.0f || new BigDecimal(Float.toString(value)).compareTo(exact) != 0) {
      return null;
    }
    return value;
  }

  /**
   * Convert a literal to the exact double the column compares against.
   *
   * @param coerced
   *          the coerced literal
   * @return the double value, or {@code null} when pushing it down could disagree
   *         with the Java engine: a non-numeric, non-finite or zero value (the
   *         record-level filter orders {@code -0.0} below {@code 0.0}), or a
   *         decimal that is not the shortest representation of the double
   */
  private static Double doubleLiteral(Object coerced) {
    if (!(coerced instanceof BigDecimal exact)) {
      return null;
    }
    double value = exact.doubleValue();
    if (!Double.isFinite(value) || value == 0.0 || new BigDecimal(Double.toString(value)).compareTo(exact) != 0) {
      return null;
    }
    return value;
  }

  /**
   * Check that a floating-point comparison can be pushed down. NaN is the largest
   * value in the SQL order but PyArrow leaves it out of row-group and page
   * statistics, so a standalone {@code >}, {@code >=} or {@code <>} could skip
   * pages whose only matching rows are NaN.
   *
   * @param op
   *          the comparison operator
   * @param standalone
   *          whether no other conjunct rejects NaN
   * @return {@code true} if the operator may be pushed down
   */
  private static boolean floatingOperatorAllowed(BinaryExpression op, boolean standalone) {
    return !standalone || switch (op.getStringExpression()) {
      case "=", "<", "<=" -> true;
      default -> false;
    };
  }

  /**
   * Restricts translation to literals and execution-scoped binding slots;
   * ordinary SQL functions and computed operands remain in Java.
   */
  private static boolean isLiteral(Expression expression) {
    if (Boolean.TRUE.equals(BOUND_VALUES.get()) && expression instanceof Function function
        && CorrelatedBindings.matches(function)) {
      return true;
    }
    if (expression instanceof net.sf.jsqlparser.expression.CastExpression cast) {
      return isLiteral(cast.getLeftExpression());
    }
    if (expression instanceof net.sf.jsqlparser.expression.SignedExpression signed) {
      if (!isLiteral(signed.getExpression())) {
        return false;
      }
      try {
        LiteralConverter.toLiteral(signed);
        return true;
      } catch (IllegalArgumentException failure) {
        return false;
      }
    }
    return LiteralConverter.isLiteral(expression);
  }

  /** Converts exact decimal values to their Parquet physical representation. */
  private static Optional<FilterPredicate> decimalPredicate(Schema schema, LogicalTypes.Decimal decimal, String column,
      BinaryExpression op, Object literal) {
    BigDecimal value = new BigDecimal(literal.toString()).setScale(decimal.getScale(), RoundingMode.UNNECESSARY);
    if (value.precision() > decimal.getPrecision()) {
      return Optional.empty();
    }
    var unscaled = value.unscaledValue();
    String physical = schema.getProp("jparq.physicalType");
    if ("INT32".equals(physical)) {
      return Optional.ofNullable(buildInt(intColumn(column), op, unscaled.intValueExact()));
    }
    if ("INT64".equals(physical)) {
      return Optional.ofNullable(buildLong(longColumn(column), op, unscaled.longValueExact()));
    }
    if (schema.getType() != Schema.Type.BYTES && schema.getType() != Schema.Type.FIXED
        || schema.getType() == Schema.Type.FIXED && unscaled.toByteArray().length > schema.getFixedSize()) {
      return Optional.empty();
    }
    return Optional
        .of(userDefined(binaryColumn(column), new DecimalBinaryPredicate(unscaled, op.getStringExpression())));
  }

  /** Rounds an integral bound without truncation or overflow. */
  private static BigDecimal integralBound(Object value, BinaryExpression op, long min, long max) {
    if (!(value instanceof Number)) {
      return null;
    }
    BigDecimal number = new BigDecimal(value.toString());
    if (number.compareTo(BigDecimal.valueOf(min)) < 0 || number.compareTo(BigDecimal.valueOf(max)) > 0) {
      return null;
    }
    String operator = op.getStringExpression();
    if (("=".equals(operator) || "<>".equals(operator) || "!=".equals(operator))
        && number.stripTrailingZeros().scale() > 0) {
      return null;
    }
    return number.setScale(0,
        "<".equals(operator) || ">=".equals(operator) ? RoundingMode.CEILING : RoundingMode.FLOOR);
  }

  /**
   * Build an int predicate for the given operator and value.
   *
   * @param c
   *          the Parquet int column
   * @param op
   *          the binary operator
   * @param v
   *          the literal value
   * @return FilterPredicate, or null if the operator is unsupported
   */
  private static FilterPredicate buildInt(org.apache.parquet.filter2.predicate.Operators.IntColumn c,
      BinaryExpression op, int v) {
    return switch (op.getStringExpression()) {
      case "=" -> eq(c, v);
      case "<" -> lt(c, v);
      case ">" -> gt(c, v);
      case "<=" -> ltEq(c, v);
      case ">=" -> gtEq(c, v);
      case "<>", "!=" -> and(notEq(c, v), notEq(c, null));
      default -> null;
    };
  }

  /**
   * Build a long predicate for the given operator and value.
   *
   * @param c
   *          the Parquet long column
   * @param op
   *          the binary operator
   * @param v
   *          the literal value
   * @return FilterPredicate, or null if the operator is unsupported
   */
  private static FilterPredicate buildLong(org.apache.parquet.filter2.predicate.Operators.LongColumn c,
      BinaryExpression op, long v) {
    return switch (op.getStringExpression()) {
      case "=" -> eq(c, v);
      case "<" -> lt(c, v);
      case ">" -> gt(c, v);
      case "<=" -> ltEq(c, v);
      case ">=" -> gtEq(c, v);
      case "<>", "!=" -> and(notEq(c, v), notEq(c, null));
      default -> null;
    };
  }

  /**
   * Build a float predicate for the given operator and value.
   *
   * @param c
   *          the Parquet float column
   * @param op
   *          the binary operator
   * @param v
   *          the literal value
   * @return FilterPredicate, or null if the operator is unsupported
   */
  private static FilterPredicate buildFloat(org.apache.parquet.filter2.predicate.Operators.FloatColumn c,
      BinaryExpression op, float v) {
    return switch (op.getStringExpression()) {
      case "=" -> eq(c, v);
      case "<" -> lt(c, v);
      case ">" -> gt(c, v);
      case "<=" -> ltEq(c, v);
      case ">=" -> gtEq(c, v);
      case "<>", "!=" -> and(notEq(c, v), notEq(c, null));
      default -> null;
    };
  }

  /**
   * Build a double predicate for the given operator and value.
   *
   * @param c
   *          the Parquet double column
   * @param op
   *          the binary operator
   * @param v
   *          the literal value
   * @return FilterPredicate, or null if the operator is unsupported
   */
  private static FilterPredicate buildDouble(org.apache.parquet.filter2.predicate.Operators.DoubleColumn c,
      BinaryExpression op, double v) {
    return switch (op.getStringExpression()) {
      case "=" -> eq(c, v);
      case "<" -> lt(c, v);
      case ">" -> gt(c, v);
      case "<=" -> ltEq(c, v);
      case ">=" -> gtEq(c, v);
      case "<>", "!=" -> and(notEq(c, v), notEq(c, null));
      default -> null;
    };
  }

  /**
   * Build a boolean predicate for the given operator and value.
   *
   * @param c
   *          the Parquet boolean column
   * @param op
   *          the binary operator
   * @param v
   *          the literal value
   * @return FilterPredicate, or null if the operator is unsupported for boolean
   */
  private static FilterPredicate buildBoolean(org.apache.parquet.filter2.predicate.Operators.BooleanColumn c,
      BinaryExpression op, boolean v) {
    return switch (op.getStringExpression()) {
      case "=" -> eq(c, v);
      case "<>", "!=" -> and(notEq(c, v), notEq(c, null));
      default -> null; // no ordering for booleans
    };
  }

  /**
   * Build a binary (string-backed) predicate for the given operator and value.
   *
   * @param c
   *          the Parquet binary column
   * @param op
   *          the binary operator
   * @param v
   *          the literal value as Binary
   * @return FilterPredicate, or null if the operator is unsupported
   */
  private static FilterPredicate buildBinary(org.apache.parquet.filter2.predicate.Operators.BinaryColumn c,
      BinaryExpression op, Binary v) {
    return switch (op.getStringExpression()) {
      case "=" -> eq(c, v);
      case "<>", "!=" -> and(notEq(c, v), notEq(c, null));
      case "<" -> lt(c, v);
      case ">" -> gt(c, v);
      case "<=" -> ltEq(c, v);
      case ">=" -> gtEq(c, v);
      default -> null;
    };
  }

  /**
   * Predicate and residual planned atomically so no conjunct is lost.
   * 
   * @param predicate
   *          the optional physical predicate
   * @param residual
   *          the remaining SQL predicate, or null
   */
  public record PushdownSplit(Optional<FilterPredicate> predicate, Expression residual) {
  }

  /**
   * Splits supported AND terms from residual expressions. OR remains atomic.
   * 
   * @param schema
   *          the Avro file schema
   * @param where
   *          the SQL predicate, or null
   * @return a predicate and its residual
   */
  public static PushdownSplit split(Schema schema, Expression where) {
    return splitNormalized(schema, where == null ? null : normalize(where, false));
  }

  /** Splits an expression already in negation normal form. */
  private static PushdownSplit splitNormalized(Schema schema, Expression expression) {
    if (expression == null) {
      return new PushdownSplit(Optional.empty(), null);
    }
    Expression exp = unwrap(expression);
    if (exp instanceof AndExpression and) {
      PushdownSplit left = splitNormalized(schema, and.getLeftExpression());
      PushdownSplit right = splitNormalized(schema, and.getRightExpression());
      Optional<FilterPredicate> predicate = left.predicate().isEmpty()
          ? right.predicate()
          : right.predicate().isEmpty()
              ? left.predicate()
              : Optional.of(and(left.predicate().get(), right.predicate().get()));
      Expression residual = left.residual() == null
          ? right.residual()
          : right.residual() == null ? left.residual() : new AndExpression(left.residual(), right.residual());
      return new PushdownSplit(predicate, residual);
    }
    Optional<FilterPredicate> exact = tryBuild(schema, exp);
    if (exact.isPresent()) {
      return new PushdownSplit(exact, null);
    }
    return new PushdownSplit(prefixPredicate(schema, exp), exp);
  }

  /**
   * Builds a conservative unsigned UTF-8 prefix range while retaining LIKE in
   * Java.
   */
  private static Optional<FilterPredicate> prefixPredicate(Schema schema, Expression expression) {
    if (!(expression instanceof LikeExpression like) || like.isNot() || like.getEscape() != null
        || like.getLikeKeyWord() != null && like.getLikeKeyWord() != LikeExpression.KeyWord.LIKE
        || !(like.getLeftExpression() instanceof Column column)
        || !(like.getRightExpression() instanceof StringValue pattern)) {
      return Optional.empty();
    }
    Schema.Field field = findFieldCaseInsensitive(schema, column.getColumnName());
    if (field == null || AvroCoercions.effectiveSchema(field.schema()).getType() != Schema.Type.STRING) {
      return Optional.empty();
    }
    String decodedPattern = LiteralConverter.decodeStringLiteral(pattern);
    int wildcard = decodedPattern.indexOf('%');
    if (wildcard <= 0) {
      return Optional.empty();
    }
    String prefix = decodedPattern.substring(0, wildcard);
    if (prefix.indexOf('_') >= 0) {
      return Optional.empty();
    }
    byte[] bytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    FilterPredicate lower = gtEq(binaryColumn(field.name()), Binary.fromConstantByteArray(bytes));
    int last = bytes.length - 1;
    while (last >= 0 && (bytes[last] & 255) == 255) {
      last--;
    }
    if (last < 0) {
      return Optional.of(lower);
    }
    byte[] upper = Arrays.copyOf(bytes, last + 1);
    upper[last]++;
    return Optional.of(and(lower, lt(binaryColumn(field.name()), Binary.fromConstantByteArray(upper))));
  }

  /** Unwraps both modern and legacy parenthesized scalar expressions. */
  private static Expression unwrap(Expression expression) {
    Expression result = unwrapParenthesis(expression);
    while (result instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      result = unwrapParenthesis(list.getFirst());
    }
    return result;
  }

  /**
   * Produces SQL negation normal form without mutating the query's expression
   * tree.
   */
  private static Expression normalize(Expression expression, boolean negative) {
    Expression exp = unwrap(expression);
    if (exp instanceof NotExpression not) {
      return normalize(not.getExpression(), !negative);
    }
    if (exp instanceof AndExpression and) {
      Expression left = normalize(and.getLeftExpression(), negative);
      Expression right = normalize(and.getRightExpression(), negative);
      return negative ? new OrExpression(left, right) : new AndExpression(left, right);
    }
    if (exp instanceof OrExpression or) {
      Expression left = normalize(or.getLeftExpression(), negative);
      Expression right = normalize(or.getRightExpression(), negative);
      return negative ? new AndExpression(left, right) : new OrExpression(left, right);
    }
    if (exp instanceof Between between && (negative || between.isNot())) {
      boolean inverse = negative != between.isNot();
      Expression lower = inverse
          ? new MinorThan(between.getLeftExpression(), between.getBetweenExpressionStart())
          : new GreaterThanEquals(between.getLeftExpression(), between.getBetweenExpressionStart());
      Expression upper = inverse
          ? new GreaterThan(between.getLeftExpression(), between.getBetweenExpressionEnd())
          : new MinorThanEquals(between.getLeftExpression(), between.getBetweenExpressionEnd());
      return inverse ? new OrExpression(lower, upper) : new AndExpression(lower, upper);
    }
    if (exp instanceof InExpression in && (negative || in.isNot())
        && in.getRightExpression() instanceof ExpressionList<?> list && !list.isEmpty() && list.size() <= 20) {
      // A NULL list member must keep the entire IN in Java.
      if (list.stream().anyMatch(value -> isLiteral(value) && LiteralConverter.toLiteral(value) == null)) {
        return negative ? new NotExpression(exp) : exp;
      }
      boolean inverse = negative != in.isNot();
      Expression result = null;
      for (Expression value : list) {
        Expression term = inverse
            ? new NotEqualsTo(in.getLeftExpression(), value)
            : new EqualsTo(in.getLeftExpression(), value);
        result = result == null ? term : inverse ? new AndExpression(result, term) : new OrExpression(result, term);
      }
      return result;
    }
    if (!negative) {
      return exp;
    }
    if (exp instanceof BinaryExpression binary) {
      if (JSqlParserSupport.isQuantifiedOperand(binary.getLeftExpression())
          || JSqlParserSupport.isQuantifiedOperand(binary.getRightExpression())) {
        return new NotExpression(exp);
      }
      BinaryExpression inverse = switch (binary.getStringExpression()) {
        case "=" -> new NotEqualsTo();
        case "<>", "!=" -> new EqualsTo();
        case "<" -> new GreaterThanEquals();
        case ">" -> new MinorThanEquals();
        case "<=" -> new GreaterThan();
        case ">=" -> new MinorThan();
        default -> null;
      };
      if (inverse != null) {
        inverse.setLeftExpression(binary.getLeftExpression());
        inverse.setRightExpression(binary.getRightExpression());
        return inverse;
      }
    }
    if (exp instanceof IsNullExpression || exp instanceof LikeExpression) {
      try {
        Expression copy = JSqlParserSupport.parseCondition(exp.toString());
        if (copy instanceof IsNullExpression isNull) {
          isNull.setNot(!isNull.isNot());
        }
        if (copy instanceof LikeExpression like) {
          like.setNot(!like.isNot());
        }
        return copy;
      } catch (JSQLParserException exception) {
        return new NotExpression(exp);
      }
    }
    return new NotExpression(exp);
  }
}
