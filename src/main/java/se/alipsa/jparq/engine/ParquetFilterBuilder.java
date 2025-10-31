package se.alipsa.jparq.engine;

import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static se.alipsa.jparq.engine.ExpressionEvaluator.unwrapParenthesis;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import se.alipsa.jparq.helper.LiteralConverter;

/**
 * Best-effort translator from JSQLParser Expression -> Parquet FilterPredicate.
 */
public final class ParquetFilterBuilder {

  private ParquetFilterBuilder() {
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
    return tryBuild(avroSchema, unwrapParenthesis(where));
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
    Expression exp = unwrapParenthesis(expression);

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
    if (exp instanceof NotExpression not) {
      var inner = tryBuild(schema, not.getExpression());
      return inner.map(FilterApi::not);
    }

    if (exp instanceof Between b) {
      var ge = comparison(schema, new GreaterThanEquals(b.getLeftExpression(), b.getBetweenExpressionStart()));
      var le = comparison(schema, new MinorThanEquals(b.getLeftExpression(), b.getBetweenExpressionEnd()));
      if (ge.isPresent() && le.isPresent()) {
        FilterPredicate p = and(ge.get(), le.get());
        return Optional.of(b.isNot() ? not(p) : p);
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
        var eq = comparison(schema, new EqualsTo(in.getLeftExpression(), val));
        if (eq.isEmpty()) {
          return Optional.empty();
        }
        eqs.add(eq.get());
      }
      FilterPredicate disj = eqs.getFirst();
      for (int i = 1; i < eqs.size(); i++) {
        disj = or(disj, eqs.get(i));
      }
      return Optional.of(in.isNot() ? not(disj) : disj);
    }

    return comparison(schema, exp);
  }

  private static Schema.Field findFieldCaseInsensitive(Schema schema, String name) {
    Schema.Field f = schema.getField(name);
    if (f != null) {
      return f;
    }
    String target = name.toLowerCase(Locale.ROOT);
    for (Schema.Field sf : schema.getFields()) {
      if (sf.name().toLowerCase(Locale.ROOT).equals(target)) {
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
   * @return optional FilterPredicate if pushdownable; empty otherwise
   */
  private static Optional<FilterPredicate> comparison(Schema schema, Expression e) {
    if (!(e instanceof BinaryExpression be)) {
      return Optional.empty();
    }

    Expression l = be.getLeftExpression();
    Expression r = be.getRightExpression();

    boolean leftIsCol = l instanceof Column;
    boolean rightIsCol = r instanceof Column;

    if (leftIsCol && !rightIsCol) {
      return buildColOpLit(schema, (Column) l, be, r);
    } else if (!leftIsCol && rightIsCol) {
      // swap: lit <op> col => col <op-swapped> lit
      return buildColOpLit(schema, (Column) r, swap(be), l);
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
   * @param litExpr
   *          the literal expression
   * @return optional FilterPredicate; empty if schema/type or operator is
   *         unsupported
   */
  private static Optional<FilterPredicate> buildColOpLit(Schema avroSchema, Column colExpr, BinaryExpression op,
      Expression litExpr) {
    String col = colExpr.getColumnName();
    Schema.Field f = findFieldCaseInsensitive(avroSchema, col);
    if (f == null) {
      return Optional.empty();
    }

    if (litExpr instanceof AnyComparisonExpression) {
      return Optional.empty();
    }

    Schema effective = AvroCoercions.effectiveSchema(f.schema());
    Object lit = LiteralConverter.toLiteral(litExpr);
    Object coerced = AvroCoercions.coerceLiteral(lit, effective);
    if (coerced == null) {
      return Optional.empty(); // don’t push null semantics
    }

    return switch (effective.getType()) {
      case INT -> Optional.ofNullable(buildInt(intColumn(col), op, ((Number) coerced).intValue()));
      case LONG -> Optional.ofNullable(buildLong(longColumn(col), op, ((Number) coerced).longValue()));
      case FLOAT -> Optional.ofNullable(buildFloat(floatColumn(col), op, ((Number) coerced).floatValue()));
      case DOUBLE -> Optional.ofNullable(buildDouble(doubleColumn(col), op, ((Number) coerced).doubleValue()));
      case BOOLEAN -> Optional.ofNullable(buildBoolean(booleanColumn(col), op, (Boolean) coerced));
      case STRING -> {
        Binary b = Binary.fromString(coerced.toString());
        yield Optional.ofNullable(buildBinary(binaryColumn(col), op, b));
      }
      default -> Optional.empty();
    };
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
      case "<>", "!=" -> notEq(c, v);
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
      case "<>", "!=" -> notEq(c, v);
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
      case "<>", "!=" -> notEq(c, v);
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
      case "<>", "!=" -> notEq(c, v);
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
      case "<>", "!=" -> notEq(c, v);
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
      case "<>", "!=" -> notEq(c, v);
      case "<" -> lt(c, v);
      case ">" -> gt(c, v);
      case "<=" -> ltEq(c, v);
      case ">=" -> gtEq(c, v);
      default -> null;
    };
  }

  /**
   * True if we could build a predicate for the whole WHERE expression tree.
   *
   * @param avroSchema
   *          the Avro schema of the Parquet data
   * @param where
   *          the WHERE expression
   * @return true if the expression can be fully pushed down
   */
  public static boolean covers(Schema avroSchema, Expression where) {
    return build(avroSchema, where).isPresent();
  }

  /**
   * Residual expression to evaluate in Java after Parquet filtering. Simple
   * version: if fully covered, return null (no residual), otherwise return
   * original WHERE.
   *
   * @param avroSchema
   *          the Avro schema of the Parquet data
   * @param where
   *          the WHERE expression
   * @return residual expression, or null if none
   */
  public static Expression residual(Schema avroSchema, Expression where) {
    return covers(avroSchema, where) ? null : where;
  }
}
