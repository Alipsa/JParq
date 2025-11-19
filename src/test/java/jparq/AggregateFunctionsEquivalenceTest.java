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
 * Tests for the expression equivalence logic used when matching aggregate expressions.
 */
public class AggregateFunctionsEquivalenceTest {

  @Test
  void likeExpressionsRespectNotFlag() throws Exception {
    Expression positiveLike = CCJSqlParserUtil.parseExpression("col LIKE 'a%'");
    Expression negativeLike = CCJSqlParserUtil.parseExpression("col NOT LIKE 'a%'");

    assertTrue(equivalent(positiveLike, positiveLike));
    assertFalse(equivalent(positiveLike, negativeLike));
  }

  @Test
  void similarToExpressionsRespectNotFlag() throws Exception {
    Expression positiveSimilarTo = CCJSqlParserUtil.parseExpression("col SIMILAR TO 'a%'");
    Expression negativeSimilarTo = CCJSqlParserUtil.parseExpression("col NOT SIMILAR TO 'a%'");

    assertTrue(equivalent(positiveSimilarTo, positiveSimilarTo));
    assertFalse(equivalent(positiveSimilarTo, negativeSimilarTo));
  }

  private boolean equivalent(Expression first, Expression second)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method method =
        AggregateFunctions.class.getDeclaredMethod(
            "expressionsEquivalent", Expression.class, Expression.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, first, second);
  }
}
