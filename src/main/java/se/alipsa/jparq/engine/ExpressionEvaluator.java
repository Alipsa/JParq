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
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** Evaluates SQL expressions against Avro GenericRecords. */
public final class ExpressionEvaluator {

  private final Map<String, Schema> fieldSchemas; // exact match
  private final Map<String, String> caseInsensitiveIndex; // lower(name) -> canonical

  /**
   * Constructor for ExpressionEvaluator.
   *
   * @param schema
   *          the Avro schema of the records to evaluate against
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
  }

  /**
   * Evaluate the given expression against the provided GenericRecord.
   *
   * @param expression
   *          the expression to evaluate
   * @param rec
   *          the GenericRecord to evaluate against
   * @return true if the expression evaluates to true, false otherwise
   */
  @SuppressWarnings({
      "PMD.LooseCoupling", "PMD.EmptyCatchBlock"
  })
  public boolean eval(Expression expression, GenericRecord rec) {
    // Always strip all layers of (...) first
    Expression expr = unwrapParenthesis(expression);

    String txt = expr.toString().trim();
    if (txt.length() >= 2 && txt.charAt(0) == '(' && txt.charAt(txt.length() - 1) == ')') {
      try {
        Expression inner = CCJSqlParserUtil.parseCondExpression(txt.substring(1, txt.length() - 1));
        return eval(inner, rec);
      } catch (JSQLParserException ignore) {
        // If reparse fails, we’ll continue to the standard handlers below
      }
    }

    if (expr instanceof AndExpression and) {
      return eval(and.getLeftExpression(), rec) && eval(and.getRightExpression(), rec);
    }
    if (expr instanceof OrExpression or) {
      return eval(or.getLeftExpression(), rec) || eval(or.getRightExpression(), rec);
    }
    if (expr instanceof NotExpression not) {
      return !eval(not.getExpression(), rec); // this will also be unwrapped on next call
    }
    if (expr instanceof IsNullExpression isNull) {
      Operand op = operand(isNull.getLeftExpression(), rec);
      boolean isNullVal = (op.value == null);
      return isNull.isNot() != isNullVal;
    }
    if (expr instanceof LikeExpression like) {
      Operand leftOperand = operand(like.getLeftExpression(), rec);
      Operand rightOperand = operand(like.getRightExpression(), rec); // should be a string literal
      String left = (leftOperand.value == null) ? null : leftOperand.value.toString();
      String pat = (rightOperand.value == null) ? null : rightOperand.value.toString();
      if (left == null || pat == null) {
        return false;
      }

      // FIX: Use the non-deprecated getLikeKeyWord().toString() to determine
      // case-insensitivity
      // for JSQLParser 5.3, replacing the deprecated getStringExpression() and
      // isCaseInsensitive().
      boolean caseInsensitive = "ILIKE".equalsIgnoreCase(like.getLikeKeyWord().toString());

      boolean matches = likeMatch(left, pat, caseInsensitive);
      return like.isNot() != matches;
    }
    if (expr instanceof Between between) {
      Operand val = operand(between.getLeftExpression(), rec);
      Operand lo = operand(between.getBetweenExpressionStart(), rec);
      Operand hi = operand(between.getBetweenExpressionEnd(), rec);

      Object curVal = val.value;
      Object lowVal = lo.value;
      Object highVal = hi.value;
      // coerce to column type if available
      if (val.schemaOrNull != null) {
        lowVal = coerceLiteral(lowVal, val.schemaOrNull);
        highVal = coerceLiteral(highVal, val.schemaOrNull);
      }
      if (curVal == null || lowVal == null || highVal == null) {
        return false;
      }
      int cmpLo = typedCompare(curVal, lowVal);
      int cmpHi = typedCompare(curVal, highVal);
      boolean in = (cmpLo >= 0 && cmpHi <= 0);
      return between.isNot() != in;
    }
    if (expr instanceof InExpression in) {
      Operand left = operand(in.getLeftExpression(), rec);
      Expression right = in.getRightExpression(); // right side is an Expression
      if (right instanceof ExpressionList<?> list) {
        boolean found = false;
        for (Expression e : list) {
          Operand rightOperand = operand(e, rec); // your existing helper
          Object leftVal = left.value;
          Object rightVal = rightOperand.value; // left is the evaluated left operand
          if (left.schemaOrNull != null) {
            rightVal = coerceLiteral(rightVal, left.schemaOrNull);
          }
          if (leftVal != null && rightVal != null && typedCompare(leftVal, rightVal) == 0) {
            found = true;
            break;
          }
        }
        return in.isNot() != found;
      }
    }

    // existing comparisons
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
      int cmp = compare(be.getLeftExpression(), be.getRightExpression(), rec);
      String op = be.getStringExpression(); // e.g. "=", "<", ">", "<=", ">=", "<>", "!="
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
    throw new IllegalArgumentException("Unsupported WHERE expression: " + expr);
  }

  @SuppressWarnings("removal")
  public static Expression unwrapParenthesis(Expression expr) {
    Expression unwrapped = expr;
    while (unwrapped instanceof Parenthesis p) {
      unwrapped = p.getExpression();
    }
    return unwrapped;
  }

  private record Operand(Object value, Schema schemaOrNull) {
  }

  private Operand operand(Expression e, GenericRecord rec) {
    if (e instanceof Column c) {
      String name = c.getColumnName();
      Schema colSchema = fieldSchemas.get(name);
      if (colSchema == null) {
        String canon = caseInsensitiveIndex.get(name.toLowerCase(Locale.ROOT));
        if (canon != null) {
          colSchema = fieldSchemas.get(canon);
        }
      }
      if (colSchema == null) {
        return new Operand(null, null);
      }
      Object v = AvroCoercions.unwrap(rec.get(name), colSchema);
      return new Operand(v, colSchema);
    }
    return new Operand(SqlParser.toLiteral(e), null);
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
    return l.toString().compareTo(r.toString());
  }

  // LIKE with % and _ (naive regex conversion)
  private static boolean likeMatch(String s, String pattern, boolean caseInsensitive) {
    StringBuilder re = new StringBuilder();
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      switch (c) {
        case '%':
          re.append(".*");
          break;
        case '_':
          re.append('.');
          break;
        default:
          // escape regex metacharacters
          if ("\\.[]{}()*+-?^$|".indexOf(c) >= 0) {
            re.append('\\');
          }
          re.append(c);
      }
    }
    String regex = re.toString();
    return caseInsensitive ? s.toLowerCase().matches(regex.toLowerCase()) : s.matches(regex);
  }

  private int compare(Expression leftExpr, Expression rightExpr, GenericRecord rec) {
    Operand leftlOperand = operand(leftExpr, rec);
    Operand rightOperand = operand(rightExpr, rec);

    Object leftVal = leftlOperand.value;
    Object rightVal = rightOperand.value;

    // If one side is a column (has schema) and the other is a literal, coerce
    // literal to column
    // type
    if (leftlOperand.schemaOrNull != null && rightOperand.schemaOrNull == null) {
      rightVal = coerceLiteral(rightVal, leftlOperand.schemaOrNull);
    } else if (rightOperand.schemaOrNull != null && leftlOperand.schemaOrNull == null) {
      leftVal = coerceLiteral(leftVal, rightOperand.schemaOrNull);
    }

    if (leftVal == null || rightVal == null) {
      return -1; // nulls don't match in this minimal impl
    }

    try {
      return typedCompare(leftVal, rightVal);
    } catch (Exception e) {
      return -1;
    }
  }
}
