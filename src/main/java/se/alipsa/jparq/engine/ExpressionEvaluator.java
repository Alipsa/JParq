package se.alipsa.jparq.engine;

import static se.alipsa.jparq.engine.AvroCoercions.coerceLiteral;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
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
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.helper.StringExpressions;

/**
 * Evaluates SQL expressions (via JSqlParser) against Avro
 * {@link GenericRecord}s.
 *
 * <p>
 * This class performs a <em>no-semantics-change</em> evaluation compared to the
 * original implementation: it preserves the existing coercion rules, comparison
 * behavior (including the treatment of {@code null}), and special parsing of
 * fully parenthesized expressions, while improving readability and cohesion.
 * </p>
 */
public final class ExpressionEvaluator {

  /** exact field name -> schema */
  private final Map<String, Schema> fieldSchemas;
  /** lower(field name) -> canonical field name */
  private final Map<String, String> caseInsensitiveIndex;
  private final ValueExpressionEvaluator literalEvaluator;

  /**
   * Creates a new evaluator for the provided Avro {@link Schema}.
   *
   * @param schema
   *          the Avro schema of the records to evaluate against; must not be
   *          {@code null}
   */
  public ExpressionEvaluator(Schema schema) {
    Map<String, Schema> fs = new HashMap<>();
    Map<String, String> ci = new HashMap<>();
    for (Schema.Field f : schema.getFields()) {
      fs.put(f.name(), f.schema());
      ci.put(f.name().toLowerCase(Locale.ROOT), f.name());
    }
    this.fieldSchemas = Collections.unmodifiableMap(fs);
    this.caseInsensitiveIndex = Collections.unmodifiableMap(ci);
    this.literalEvaluator = new ValueExpressionEvaluator(schema);
  }

  /**
   * Evaluate the given SQL expression against the provided {@link GenericRecord}.
   *
   * <p>
   * Notes on behavior (unchanged from the original):
   * <ul>
   * <li>All layers of parentheses are unwrapped before evaluation.</li>
   * <li>If the expression text is fully parenthesized (e.g., {@code "(...)"}) it
   * is re-parsed with {@code CCJSqlParserUtil.parseCondExpression} as a
   * best-effort and then evaluated.</li>
   * <li>When comparing a column to a literal, the literal is coerced to the
   * column's Avro type.</li>
   * <li>{@code null} participates as "non-match" in comparisons (returns
   * {@code -1} internally).</li>
   * <li>LIKE supports {@code %} (any sequence) and {@code _} (single char); ILIKE
   * is case-insensitive.</li>
   * </ul>
   *
   * @param expression
   *          the expression to evaluate; must not be {@code null}
   * @param rec
   *          the Avro record to evaluate against; must not be {@code null}
   * @return {@code true} if the expression evaluates to {@code true}; otherwise
   *         {@code false}
   * @throws IllegalArgumentException
   *           if the expression type/operator is unsupported
   */
  @SuppressWarnings({
      "PMD.LooseCoupling", "PMD.EmptyCatchBlock"
  })
  public boolean eval(Expression expression, GenericRecord rec) {
    // Always strip all layers of (...) first
    final Expression expr = unwrapParenthesis(expression);

    // Special-case: best-effort to re-parse "( ... )" to a condition expression
    // (kept for exact behavioral parity with the original implementation)
    if (isFullyParenthesized(expr)) {
      try {
        Expression inner = CCJSqlParserUtil.parseCondExpression(stripOuterParens(expr.toString()));
        return eval(inner, rec);
      } catch (JSQLParserException ignore) {
        // fall back to standard handlers below
      }
    }

    // Boolean connectives
    if (expr instanceof AndExpression and) {
      return evalAnd(and, rec);
    }
    if (expr instanceof OrExpression or) {
      return evalOr(or, rec);
    }
    if (expr instanceof NotExpression not) {
      return evalNot(not, rec);
    }

    // Null checks, LIKE, BETWEEN, IN
    if (expr instanceof IsNullExpression isNull) {
      return evalIsNull(isNull, rec);
    }
    if (expr instanceof LikeExpression like) {
      return evalLike(like, rec);
    }
    if (expr instanceof SimilarToExpression similar) {
      return evalSimilar(similar, rec);
    }
    if (expr instanceof Between between) {
      return evalBetween(between, rec);
    }
    if (expr instanceof InExpression in) {
      return evalIn(in, rec);
    }

    // Comparison operators
    if (expr instanceof EqualsTo e) {
      return compare(e.getLeftExpression(), e.getRightExpression(), rec) == 0;
    }
    if (expr instanceof GreaterThan gt) {
      return compare(gt.getLeftExpression(), gt.getRightExpression(), rec) > 0;
    }
    if (expr instanceof MinorThan lt) {
      return compare(lt.getLeftExpression(), lt.getRightExpression(), rec) < 0;
    }
    if (expr instanceof GreaterThanEquals ge) {
      return compare(ge.getLeftExpression(), ge.getRightExpression(), rec) >= 0;
    }
    if (expr instanceof MinorThanEquals le) {
      return compare(le.getLeftExpression(), le.getRightExpression(), rec) <= 0;
    }

    // Generic fallback for binary comparisons (covers parenthesized variants too)
    if (expr instanceof BinaryExpression be) {
      return evalBinary(be, rec);
    }

    throw new IllegalArgumentException("Unsupported WHERE expression: " + expr);
  }

  // -------------------------
  // Operator-specific handlers
  // -------------------------

  private boolean evalAnd(AndExpression and, GenericRecord rec) {
    return eval(and.getLeftExpression(), rec) && eval(and.getRightExpression(), rec);
  }

  private boolean evalOr(OrExpression or, GenericRecord rec) {
    return eval(or.getLeftExpression(), rec) || eval(or.getRightExpression(), rec);
  }

  private boolean evalNot(NotExpression not, GenericRecord rec) {
    // unwrap happens in the next eval call
    return !eval(not.getExpression(), rec);
  }

  private boolean evalIsNull(IsNullExpression isNull, GenericRecord rec) {
    Operand op = operand(isNull.getLeftExpression(), rec);
    boolean isNullVal = (op.value == null);
    return isNull.isNot() != isNullVal;
  }

  private boolean evalLike(LikeExpression like, GenericRecord rec) {
    Operand leftOperand = operand(like.getLeftExpression(), rec);
    Operand rightOperand = operand(like.getRightExpression(), rec);

    String left = (leftOperand.value == null) ? null : leftOperand.value.toString();
    String pat = (rightOperand.value == null) ? null : rightOperand.value.toString();

    if (left == null || pat == null) {
      return false;
    }

    LikeExpression.KeyWord keyWord = like.getLikeKeyWord();
    String keyword = keyWord == null ? like.getStringExpression() : keyWord.name();
    Character escapeChar = null;
    if (like.getEscape() != null) {
      Operand escapeOperand = operand(like.getEscape(), rec);
      String escape = escapeOperand.value == null ? null : escapeOperand.value.toString();
      if (escape != null && !escape.isEmpty()) {
        if (escape.length() != 1) {
          throw new IllegalArgumentException("LIKE escape clause must be a single character");
        }
        escapeChar = escape.charAt(0);
      }
    }
    boolean matches;
    if ("SIMILAR_TO".equalsIgnoreCase(keyword)) {
      matches = StringExpressions.similarTo(left, pat, escapeChar);
    } else {
      boolean caseInsensitive = keyWord == LikeExpression.KeyWord.ILIKE
          || (keyWord == null && "ILIKE".equalsIgnoreCase(keyword));
      matches = StringExpressions.like(left, pat, caseInsensitive, escapeChar);
    }
    return like.isNot() != matches;
  }

  private boolean evalSimilar(SimilarToExpression similar, GenericRecord rec) {
    Operand leftOperand = operand(similar.getLeftExpression(), rec);
    Operand rightOperand = operand(similar.getRightExpression(), rec);

    String left = (leftOperand.value == null) ? null : leftOperand.value.toString();
    String pattern = (rightOperand.value == null) ? null : rightOperand.value.toString();

    if (left == null || pattern == null) {
      return false;
    }

    String escape = similar.getEscape();
    Character escapeChar = null;
    if (escape != null && !escape.isEmpty()) {
      if (escape.length() != 1) {
        throw new IllegalArgumentException("SIMILAR TO escape clause must be a single character");
      }
      escapeChar = escape.charAt(0);
    }

    boolean matches = StringExpressions.similarTo(left, pattern, escapeChar);
    return similar.isNot() != matches;
  }

  private boolean evalBetween(Between between, GenericRecord rec) {
    Operand val = operand(between.getLeftExpression(), rec);
    Operand lo = operand(between.getBetweenExpressionStart(), rec);
    Operand hi = operand(between.getBetweenExpressionEnd(), rec);

    Object curVal = val.value;
    Object lowVal = coerceIfColumnType(lo.value, val.schemaOrNull);
    Object highVal = coerceIfColumnType(hi.value, val.schemaOrNull);

    if (curVal == null || lowVal == null || highVal == null) {
      return false;
    }

    int cmpLo = typedCompare(curVal, lowVal);
    int cmpHi = typedCompare(curVal, highVal);
    boolean in = (cmpLo >= 0 && cmpHi <= 0);
    return between.isNot() != in;
  }

  private boolean evalIn(InExpression in, GenericRecord rec) {
    Operand left = operand(in.getLeftExpression(), rec);
    Expression right = in.getRightExpression();

    if (right instanceof ExpressionList<?> list) {
      boolean found = false;
      for (Expression e : list) {
        Operand rightOperand = operand(e, rec);

        Object leftVal = left.value;
        Object rightVal = coerceIfColumnType(rightOperand.value, left.schemaOrNull);

        if (leftVal != null && rightVal != null && typedCompare(leftVal, rightVal) == 0) {
          found = true;
          break;
        }
      }
      return in.isNot() != found;
    }
    return false;
  }

  private boolean evalBinary(BinaryExpression be, GenericRecord rec) {
    int cmp = compare(be.getLeftExpression(), be.getRightExpression(), rec);
    String op = be.getStringExpression(); // "=", "<", ">", "<=", ">=", "<>", "!="
    return switch (op) {
      case "=" -> cmp == 0;
      case "<" -> cmp < 0;
      case ">" -> cmp > 0;
      case "<=" -> cmp <= 0;
      case ">=" -> cmp >= 0;
      case "<>", "!=" -> cmp != 0;
      default -> throw new IllegalArgumentException("Unsupported operator: " + op);
    };
  }

  // -------------------------
  // Utilities
  // -------------------------

  /**
   * Recursively unwrap all {@link Parenthesis} layers from an expression.
   *
   * @param expr
   *          the expression to unwrap; must not be {@code null}
   * @return the unwrapped expression
   */
  @SuppressWarnings("removal")
  public static Expression unwrapParenthesis(Expression expr) {
    Expression unwrapped = expr;
    while (unwrapped instanceof Parenthesis p) {
      unwrapped = p.getExpression();
    }
    return unwrapped;
  }

  private static boolean isFullyParenthesized(Expression expr) {
    String txt = expr.toString().trim();
    return txt.length() >= 2 && txt.charAt(0) == '(' && txt.charAt(txt.length() - 1) == ')';
  }

  private static String stripOuterParens(String s) {
    return s.substring(1, s.length() - 1);
  }

  private record Operand(Object value, Schema schemaOrNull) {
  }

  private Operand operand(Expression e, GenericRecord rec) {
    Expression expr = unwrapParenthesis(e);
    if (expr instanceof ParenthesedExpressionList<?> pel) {
      if (!pel.isEmpty()) {
        return operand(pel.getFirst(), rec);
      }
      return new Operand(null, null);
    }
    if (expr instanceof Column c) {
      String name = c.getColumnName();
      String lookupName = name; // default to the parsed token
      Schema colSchema = fieldSchemas.get(name);
      if (colSchema == null) {
        String canon = caseInsensitiveIndex.get(name.toLowerCase(Locale.ROOT));
        if (canon != null) {
          colSchema = fieldSchemas.get(canon);
          lookupName = canon; // USE canonical name when reading from the record
        }
      }
      if (colSchema == null) {
        return new Operand(null, null);
      }
      Object v = AvroCoercions.unwrap(rec.get(lookupName), colSchema);
      return new Operand(v, colSchema);
    }
    Object literal = literalEvaluator.eval(expr, rec);
    return new Operand(literal, null);
  }

  private static Object coerceIfColumnType(Object value, Schema columnSchemaOrNull) {
    return (columnSchemaOrNull == null) ? value : coerceLiteral(value, columnSchemaOrNull);
  }

  static int typedCompare(Object l, Object r) {
    if (l instanceof Number && r instanceof Number) {
      return new BigDecimal(l.toString()).compareTo(new BigDecimal(r.toString()));
    }
    if (l instanceof Boolean && r instanceof Boolean) {
      return Boolean.compare((Boolean) l, (Boolean) r);
    }
    if (l instanceof Timestamp && r instanceof Timestamp) {
      return Long.compare(((Timestamp) l).getTime(), ((Timestamp) r).getTime());
    }
    if (l instanceof Date && r instanceof Date) {
      return Long.compare(((Date) l).getTime(), ((Date) r).getTime());
    }
    if (l instanceof se.alipsa.jparq.helper.TemporalInterval li
        && r instanceof se.alipsa.jparq.helper.TemporalInterval ri) {
      return li.compareTo(ri);
    }
    return l.toString().compareTo(r.toString());
  }

  private int compare(Expression leftExpr, Expression rightExpr, GenericRecord rec) {
    Operand leftOperand = operand(leftExpr, rec);
    Operand rightOperand = operand(rightExpr, rec);

    Object leftVal = leftOperand.value;
    Object rightVal = rightOperand.value;

    // If one side is a column (has schema) and the other is a literal,
    // then coerce literal to column type
    if (leftOperand.schemaOrNull != null && rightOperand.schemaOrNull == null) {
      rightVal = coerceLiteral(rightVal, leftOperand.schemaOrNull);
    } else if (rightOperand.schemaOrNull != null && leftOperand.schemaOrNull == null) {
      leftVal = coerceLiteral(leftVal, rightOperand.schemaOrNull);
    }

    if (leftVal == null || rightVal == null) {
      // original behavior: nulls don't match
      return -1;
    }

    try {
      return typedCompare(leftVal, rightVal);
    } catch (Exception e) {
      // original behavior: swallow and treat as non-match
      return -1;
    }
  }
}
