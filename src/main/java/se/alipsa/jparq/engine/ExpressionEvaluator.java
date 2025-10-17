package se.alipsa.jparq.engine;

import static se.alipsa.jparq.engine.AvroCoercions.coerceLiteral;

import java.math.BigDecimal;
import java.sql.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public final class ExpressionEvaluator {

  private final Schema schema;

  public ExpressionEvaluator(Schema schema) {
    this.schema = schema;
  }

  @SuppressWarnings("PMD.LooseCoupling")
  public boolean eval(Expression expr, GenericRecord rec) {
    // Always strip all layers of (...) first
    while (expr instanceof Parenthesis) {
      expr = ((Parenthesis) expr).getExpression();
    }

    String txt = expr.toString().trim();
    if (txt.length() >= 2 && txt.charAt(0) == '(' && txt.charAt(txt.length() - 1) == ')') {
      try {
        Expression inner = CCJSqlParserUtil.parseCondExpression(txt.substring(1, txt.length() - 1));
        return eval(inner, rec);
      } catch (Exception ignore) {
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
      return isNull.isNot() ? !isNullVal : isNullVal;
    }
    if (expr instanceof LikeExpression like) {
      Operand L = operand(like.getLeftExpression(), rec);
      Operand R = operand(like.getRightExpression(), rec); // should be a string literal
      String left = (L.value == null) ? null : L.value.toString();
      String pat = (R.value == null) ? null : R.value.toString();
      if (left == null || pat == null) return false;
      boolean matches = likeMatch(left, pat, like.isCaseInsensitive());
      return like.isNot() ? !matches : matches;
    }
    if (expr instanceof Between between) {
      Operand val = operand(between.getLeftExpression(), rec);
      Operand lo = operand(between.getBetweenExpressionStart(), rec);
      Operand hi = operand(between.getBetweenExpressionEnd(), rec);

      Object v = val.value, l = lo.value, h = hi.value;
      // coerce to column type if available
      if (val.schemaOrNull != null) {
        l = coerceLiteral(l, val.schemaOrNull);
        h = coerceLiteral(h, val.schemaOrNull);
      }
      if (v == null || l == null || h == null) return false;
      int cmpLo = typedCompare(v, l);
      int cmpHi = typedCompare(v, h);
      boolean in = (cmpLo >= 0 && cmpHi <= 0);
      return between.isNot() ? !in : in;
    }
    if (expr instanceof InExpression in) {
      Operand left = operand(in.getLeftExpression(), rec);
      Expression right = in.getRightExpression(); // right side is an Expression
      if (right
          instanceof net.sf.jsqlparser.expression.operators.relational.ExpressionList<?> list) {
        // ExpressionList in 5.x implements List<Expression>; don't call getExpressions()
        boolean found = false;
        for (Expression e : list) {
          Operand r = operand(e, rec); // your existing helper
          Object L = left.value, R = r.value; // left is the evaluated left operand
          if (left.schemaOrNull != null) {
            R = coerceLiteral(R, left.schemaOrNull);
          }
          if (L != null && R != null && typedCompare(L, R) == 0) {
            found = true;
            break;
          }
        }
        return in.isNot() ? !found : found;
      }
    }

    // existing comparisons
    if (expr instanceof EqualsTo e)
      return compare(e.getLeftExpression(), e.getRightExpression(), rec) == 0;
    if (expr instanceof GreaterThan gt)
      return compare(gt.getLeftExpression(), gt.getRightExpression(), rec) > 0;
    if (expr instanceof MinorThan lt)
      return compare(lt.getLeftExpression(), lt.getRightExpression(), rec) < 0;
    if (expr instanceof GreaterThanEquals ge)
      return compare(ge.getLeftExpression(), ge.getRightExpression(), rec) >= 0;
    if (expr instanceof MinorThanEquals le)
      return compare(le.getLeftExpression(), le.getRightExpression(), rec) <= 0;

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

  private record Operand(Object value, Schema schemaOrNull) {}

  private Operand operand(Expression e, GenericRecord rec) {
    if (e instanceof Column c) {
      String name = c.getColumnName();
      Schema.Field f = rec.getSchema().getField(name);
      if (f == null) return new Operand(null, null);
      Object v = AvroCoercions.unwrap(rec.get(name), f.schema());
      return new Operand(v, f.schema());
    }
    // literal
    return new Operand(JParqSqlParser.toLiteral(e), null);
  }

  private static int typedCompare(Object l, Object r) {
    if (l instanceof Number && r instanceof Number)
      return new BigDecimal(l.toString()).compareTo(new BigDecimal(r.toString()));
    if (l instanceof Boolean && r instanceof Boolean)
      return Boolean.compare((Boolean) l, (Boolean) r);
    if (l instanceof java.sql.Timestamp && r instanceof java.sql.Timestamp)
      return Long.compare(((Timestamp) l).getTime(), ((Timestamp) r).getTime());
    if (l instanceof java.sql.Date && r instanceof java.sql.Date)
      return Long.compare(((Date) l).getTime(), ((Date) r).getTime());
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
          if ("\\.[]{}()*+-?^$|".indexOf(c) >= 0) re.append('\\');
          re.append(c);
      }
    }
    String regex = re.toString();
    return caseInsensitive ? s.toLowerCase().matches(regex.toLowerCase()) : s.matches(regex);
  }

  private int compare(Expression lExpr, Expression rExpr, GenericRecord rec) {
    Operand L = operand(lExpr, rec);
    Operand R = operand(rExpr, rec);

    Object l = L.value;
    Object r = R.value;

    // If one side is a column (has schema) and the other is a literal, coerce literal to column
    // type
    if (L.schemaOrNull != null && R.schemaOrNull == null) {
      r = coerceLiteral(r, L.schemaOrNull);
    } else if (R.schemaOrNull != null && L.schemaOrNull == null) {
      l = coerceLiteral(l, R.schemaOrNull);
    }

    if (l == null || r == null) return -1; // nulls don't match in this minimal impl

    try {
      if (l instanceof Number && r instanceof Number) {
        return new BigDecimal(l.toString()).compareTo(new BigDecimal(r.toString()));
      }
      if (l instanceof Boolean && r instanceof Boolean) {
        return Boolean.compare((Boolean) l, (Boolean) r);
      }
      if (l instanceof java.sql.Timestamp && r instanceof java.sql.Timestamp) {
        return Long.compare(((Timestamp) l).getTime(), ((Timestamp) r).getTime());
      }
      if (l instanceof java.sql.Date && r instanceof java.sql.Date) {
        return Long.compare(((Date) l).getTime(), ((Date) r).getTime());
      }
      // fallback: string compare
      return l.toString().compareTo(r.toString());
    } catch (Exception e) {
      return -1;
    }
  }
}
