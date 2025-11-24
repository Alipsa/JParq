package jparq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.AggregateFunctions;

/**
 * Tests for the expression equivalence logic used when matching aggregate
 * expressions.
 */
public class AggregateFunctionsEquivalenceTest {

  @Test
  void likeExpressionsRespectNotFlag() throws Exception {
    Expression positiveLike = CCJSqlParserUtil.parseExpression("col LIKE 'a%'");
    Expression negativeLike = CCJSqlParserUtil.parseExpression("col NOT LIKE 'a%'");

    assertTrue(equivalent(positiveLike, positiveLike));
    assertTrue(equivalent(negativeLike, negativeLike));
    assertFalse(equivalent(positiveLike, negativeLike));
  }

  @Test
  void similarToExpressionsRespectNotFlag() throws Exception {
    Expression positiveSimilarTo = CCJSqlParserUtil.parseExpression("col SIMILAR TO 'a%'");
    Expression negativeSimilarTo = CCJSqlParserUtil.parseExpression("col NOT SIMILAR TO 'a%'");

    assertTrue(equivalent(positiveSimilarTo, positiveSimilarTo));
    assertTrue(equivalent(negativeSimilarTo, negativeSimilarTo));
    assertFalse(equivalent(positiveSimilarTo, negativeSimilarTo));
  }

  @Test
  void likeExpressionsRespectEscapeClause() throws Exception {
    Expression withEscape = CCJSqlParserUtil.parseExpression("col LIKE 'a!%' ESCAPE '!'");
    Expression withoutEscape = CCJSqlParserUtil.parseExpression("col LIKE 'a!%'");
    Expression withDifferentEscape = CCJSqlParserUtil.parseExpression("col LIKE 'a!%' ESCAPE '\\'");

    assertTrue(equivalent(withEscape, withEscape));
    assertTrue(equivalent(withoutEscape, withoutEscape));
    assertFalse(equivalent(withEscape, withoutEscape),
        "LIKE with ESCAPE should not be equivalent to LIKE without ESCAPE");
    assertFalse(equivalent(withEscape, withDifferentEscape),
        "LIKE with different ESCAPE characters should not be equivalent");
  }

  @Test
  void similarToExpressionsRespectEscapeClause() throws Exception {
    Expression withEscape = CCJSqlParserUtil.parseExpression("col SIMILAR TO 'a!%' ESCAPE '!'");
    Expression withoutEscape = CCJSqlParserUtil.parseExpression("col SIMILAR TO 'a!%'");
    Expression withDifferentEscape = CCJSqlParserUtil.parseExpression("col SIMILAR TO 'a!%' ESCAPE '\\'");

    assertTrue(equivalent(withEscape, withEscape));
    assertTrue(equivalent(withoutEscape, withoutEscape));
    assertFalse(equivalent(withEscape, withoutEscape),
        "SIMILAR TO with ESCAPE should not be equivalent to SIMILAR TO without ESCAPE");
    assertFalse(equivalent(withEscape, withDifferentEscape),
        "SIMILAR TO with different ESCAPE strings should not be equivalent");
  }

  private boolean equivalent(Expression first, Expression second)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method method = AggregateFunctions.class.getDeclaredMethod("expressionsEquivalent", Expression.class,
        Expression.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, first, second);
  }
}
