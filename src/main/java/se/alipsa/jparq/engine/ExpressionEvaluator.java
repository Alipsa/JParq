// java
package se.alipsa.jparq.engine;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
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
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class ExpressionEvaluator {

  private final Schema schema;

  public ExpressionEvaluator(Schema schema) {
    this.schema = schema;
  }

  // Unwrap Parenthesis / EnclosedExpression reflectively (JSqlParser 5.x
  // compatible).
  public static Expression unwrapParenthesis(Expression expr) {
    Expression e = expr;
    while (true) {
      Expression before = e;

      e = reflectUnwrap(e, "Parenthesis", "getExpression");
      if (e != before) {
        continue;
      }

      e = reflectUnwrap(e, "EnclosedExpression", "getExpression");
      if (e != before) {
        continue;
      }

      break;
    }
    return e;
  }

  private static Expression reflectUnwrap(Expression e, String simpleClassName, String getter) {
    if (e == null) {
      return null;
    }
    try {
      if (e.getClass().getName().endsWith("." + simpleClassName)) {
        Method m = e.getClass().getMethod(getter);
        Object inner = m.invoke(e);
        if (inner instanceof Expression) {
          return (Expression) inner;
        }
      }
    } catch (ReflectiveOperationException ignore) {
      // ignore
    }
    return e;
  }

  private static Object reflectInvoke(Object obj, String simpleClassName, String getter) {
    if (obj == null) {
      return null;
    }
    try {
      if (obj.getClass().getName().endsWith("." + simpleClassName)) {
        Method m = obj.getClass().getMethod(getter);
        return m.invoke(obj);
      }
    } catch (ReflectiveOperationException ignore) {
      // ignore
    }
    return null;
  }

  // WHERE evaluation: NULL in any operand -> predicate is false (simplified 3VL).
  public boolean eval(Expression expr, GenericRecord rec) {
    Expression e = unwrapParenthesis(expr);

    if (e instanceof AndExpression) {
      AndExpression ae = (AndExpression) e;
      return eval(ae.getLeftExpression(), rec) && eval(ae.getRightExpression(), rec);
    }
    if (e instanceof OrExpression) {
      OrExpression oe = (OrExpression) e;
      return eval(oe.getLeftExpression(), rec) || eval(oe.getRightExpression(), rec);
    }
    if (e instanceof NotExpression) {
      NotExpression ne = (NotExpression) e;
      return !eval(ne.getExpression(), rec);
    }
    if (e instanceof IsNullExpression) {
      IsNullExpression isn = (IsNullExpression) e;
      Object v = value(isn.getLeftExpression(), rec);
      boolean isNull = (v == null);
      if (isn.isNot()) {
        return !isNull;
      } else {
        return isNull;
      }
    }
    if (e instanceof Between) {
      Between bt = (Between) e;
      Object v = value(bt.getLeftExpression(), rec);
      Object begin = value(bt.getBetweenExpressionStart(), rec);
      Object end = value(bt.getBetweenExpressionEnd(), rec);
      if (v == null || begin == null || end == null) {
        return false;
      }
      boolean in = typedCompare(v, begin) >= 0 && typedCompare(v, end) <= 0;
      if (bt.isNot()) {
        return !in;
      } else {
        return in;
      }
    }
    if (e instanceof InExpression) {
      InExpression in = (InExpression) e;
      Object left = value(in.getLeftExpression(), rec);
      if (left == null) {
        return false;
      }
      List<Expression> rightExprs = getInRightExpressions(in);
      boolean any = false;
      for (Expression re : rightExprs) {
        Object rv = value(re, rec);
        if (rv != null && typedCompare(left, rv) == 0) {
          any = true;
          break;
        }
      }
      if (in.isNot()) {
        return !any;
      } else {
        return any;
      }
    }
    if (e instanceof EqualsTo) {
      EqualsTo eq = (EqualsTo) e;
      Object l = value(eq.getLeftExpression(), rec);
      Object r = value(eq.getRightExpression(), rec);
      return l != null && r != null && typedCompare(l, r) == 0;
    }
    if (e instanceof NotEqualsTo) {
      NotEqualsTo ne = (NotEqualsTo) e;
      Object l = value(ne.getLeftExpression(), rec);
      Object r = value(ne.getRightExpression(), rec);
      return l != null && r != null && typedCompare(l, r) != 0;
    }
    if (e instanceof GreaterThan) {
      GreaterThan gt = (GreaterThan) e;
      Object l = value(gt.getLeftExpression(), rec);
      Object r = value(gt.getRightExpression(), rec);
      return l != null && r != null && typedCompare(l, r) > 0;
    }
    if (e instanceof GreaterThanEquals) {
      GreaterThanEquals ge = (GreaterThanEquals) e;
      Object l = value(ge.getLeftExpression(), rec);
      Object r = value(ge.getRightExpression(), rec);
      return l != null && r != null && typedCompare(l, r) >= 0;
    }
    if (e instanceof MinorThan) {
      MinorThan lt = (MinorThan) e;
      Object l = value(lt.getLeftExpression(), rec);
      Object r = value(lt.getRightExpression(), rec);
      return l != null && r != null && typedCompare(l, r) < 0;
    }
    if (e instanceof MinorThanEquals) {
      MinorThanEquals le = (MinorThanEquals) e;
      Object l = value(le.getLeftExpression(), rec);
      Object r = value(le.getRightExpression(), rec);
      return l != null && r != null && typedCompare(l, r) <= 0;
    }
    if (e instanceof LikeExpression) {
      return evalLike((LikeExpression) e, rec);
    }

    throw new UnsupportedOperationException("Unhandled expression in WHERE: " + e.getClass().getName());
  }

  private List<Expression> getInRightExpressions(InExpression in) {
    Object right = null;

    // Prefer getRightItemsList() (newer JSqlParser), fallback to
    // getRightExpression()
    try {
      Method m = in.getClass().getMethod("getRightItemsList");
      right = m.invoke(in);
    } catch (ReflectiveOperationException ignore) {
      try {
        Method m2 = in.getClass().getMethod("getRightExpression");
        right = m2.invoke(in);
      } catch (ReflectiveOperationException ignore2) {
        right = null;
      }
    }

    if (right instanceof ExpressionList) {
      ExpressionList el = (ExpressionList) right;
      List<Expression> exprs = el.getExpressions();
      if (exprs == null) {
        return Collections.emptyList();
      } else {
        return exprs;
      }
    }

    // Handle ParenthesedExpressionList reflectively without compiling against it
    if (right != null && right.getClass().getName().endsWith(".ParenthesedExpressionList")) {
      Object maybeEl = reflectInvoke(right, "ParenthesedExpressionList", "getExpressionList");
      if (maybeEl instanceof ExpressionList) {
        ExpressionList el = (ExpressionList) maybeEl;
        List<Expression> exprs = el.getExpressions();
        if (exprs == null) {
          return Collections.emptyList();
        } else {
          return exprs;
        }
      }
      Object single = reflectInvoke(right, "ParenthesedExpressionList", "getExpression");
      if (single instanceof Expression) {
        List<Expression> list = new ArrayList<>();
        list.add((Expression) single);
        return list;
      }
    }

    return Collections.emptyList();
  }

  private boolean evalLike(LikeExpression like, GenericRecord rec) {
    // Unwrap possible parentheses around the right expression (e.g., LIKE('Merc%'))
    Expression right = unwrapParenthesis(like.getRightExpression());
    Object leftVal = value(like.getLeftExpression(), rec);
    Object rightVal = value(right, rec);
    if (leftVal == null || rightVal == null) {
      return false;
    }

    String text = leftVal.toString();
    String pattern = rightVal.toString();

    Character escape = null;
    if (like.getEscape() != null) {
      Object escVal = value(like.getEscape(), rec);
      if (escVal != null) {
        String s = escVal.toString();
        if (!s.isEmpty()) {
          escape = s.charAt(0);
        }
      }
    }

    boolean matched = likeMatch(text, pattern, escape);
    if (like.isNot()) {
      return !matched;
    } else {
      return matched;
    }
  }

  private static boolean likeMatch(String text, String sqlPattern, Character escape) {
    String regex = toRegexFromLike(sqlPattern, escape);
    try {
      return Pattern.compile(regex, Pattern.DOTALL).matcher(text).matches();
    } catch (PatternSyntaxException e) {
      return false;
    }
  }

  private static String toRegexFromLike(String like, Character escape) {
    StringBuilder out = new StringBuilder();
    boolean escaping = false;

    for (int i = 0; i < like.length(); i++) {
      char c = like.charAt(i);

      if (escape != null) {
        if (escaping) {
          out.append(escapeRegexChar(c));
          escaping = false;
          continue;
        }
        if (c == escape.charValue()) {
          escaping = true;
          continue;
        }
      }

      if (c == '%') {
        out.append(".*");
      } else if (c == '_') {
        out.append('.');
      } else {
        out.append(escapeRegexChar(c));
      }
    }

    if (escaping) {
      out.append(escapeRegexChar(escape == null ? '\\' : escape.charValue()));
    }

    return "^" + out + "$";
  }

  private static String escapeRegexChar(char c) {
    switch (c) {
      case '\\':
      case '.':
      case '^':
      case '$':
      case '|':
      case '?':
      case '*':
      case '+':
      case '(':
      case ')':
      case '[':
      case ']':
      case '{':
      case '}':
        return "\\" + c;
      default:
        return Character.toString(c);
    }
  }

  // Resolve an expression to a Java value from the current record.
  public Object value(Expression expr, GenericRecord rec) {
    Expression e = unwrapParenthesis(expr);

    if (e instanceof NullValue) {
      return null;
    }
    if (e instanceof StringValue) {
      StringValue sv = (StringValue) e;
      return sv.getValue();
    }
    if (e instanceof LongValue) {
      LongValue lv = (LongValue) e;
      return lv.getValue();
    }
    if (e instanceof DoubleValue) {
      DoubleValue dv = (DoubleValue) e;
      return dv.getValue();
    }
    if (e instanceof SignedExpression) {
      SignedExpression se = (SignedExpression) e;
      Object inner = value(se.getExpression(), rec);
      if (inner == null) {
        return null;
      }
      if (se.getSign() == '-') {
        if (inner instanceof Number) {
          return negateNumber((Number) inner);
        }
        try {
          return -Double.parseDouble(inner.toString());
        } catch (NumberFormatException nfe) {
          return null;
        }
      }
      return inner;
    }
    if (e instanceof Column) {
      Column col = (Column) e;
      String name = col.getColumnName();
      try {
        return rec.get(name);
      } catch (Throwable t) {
        return null;
      }
    }
    if (e instanceof Function) {
      Function fn = (Function) e;
      String fname = fn.getName() == null ? "" : fn.getName().toUpperCase();
      List<Expression> args = new ArrayList<>();
      if (fn.getParameters() != null && fn.getParameters().getExpressions() != null) {
        for (Expression ex : fn.getParameters().getExpressions()) {
          args.add(ex);
        }
      }
      if ("LOWER".equals(fname) && args.size() == 1) {
        Object v = value(args.get(0), rec);
        if (v == null) {
          return null;
        }
        return v.toString().toLowerCase();
      }
      if ("UPPER".equals(fname) && args.size() == 1) {
        Object v = value(args.get(0), rec);
        if (v == null) {
          return null;
        }
        return v.toString().toUpperCase();
      }
      return null;
    }

    return null;
  }

  private static Number negateNumber(Number n) {
    if (n instanceof Integer) {
      Integer i = (Integer) n;
      return -i;
    }
    if (n instanceof Long) {
      Long l = (Long) n;
      return -l;
    }
    if (n instanceof Short) {
      Short s = (Short) n;
      return (short) -s;
    }
    if (n instanceof Byte) {
      Byte b = (Byte) n;
      return (byte) -b;
    }
    if (n instanceof Float) {
      Float f = (Float) n;
      return -f;
    }
    if (n instanceof Double) {
      Double d = (Double) n;
      return -d;
    }
    if (n instanceof BigDecimal) {
      BigDecimal bd = (BigDecimal) n;
      return bd.negate();
    }
    return -n.doubleValue();
  }

  public static int typedCompare(Object a, Object b) {
    if (a == b) {
      return 0;
    }
    if (a == null) {
      return -1;
    }
    if (b == null) {
      return 1;
    }
    if (a instanceof Number && b instanceof Number) {
      double da = ((Number) a).doubleValue();
      double db = ((Number) b).doubleValue();
      return Double.compare(da, db);
    }
    if (a instanceof CharSequence || b instanceof CharSequence) {
      String sa = a.toString();
      String sb = b.toString();
      return sa.compareTo(sb);
    }
    if (a instanceof Comparable && a.getClass().isInstance(b)) {
      @SuppressWarnings("unchecked")
      Comparable<Object> c = (Comparable<Object>) a;
      return c.compareTo(b);
    }
    return a.toString().compareTo(b.toString());
  }
}
