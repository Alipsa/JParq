package jparq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static se.alipsa.jparq.engine.AggregateFunctions.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;

/**
 * Tests for the expression equivalence logic used when matching aggregate
 * expressions.
 */
public class AggregateFunctionsEquivalenceTest {

  @Test
  void likeExpressionsRespectNotFlag() throws Exception {
    Expression positiveLike = CCJSqlParserUtil.parseExpression("col LIKE 'a%'");
    Expression negativeLike = CCJSqlParserUtil.parseExpression("col NOT LIKE 'a%'");

    assertTrue(expressionsEquivalent(positiveLike, positiveLike));
    assertTrue(expressionsEquivalent(negativeLike, negativeLike));
    assertFalse(expressionsEquivalent(positiveLike, negativeLike));
  }

  @Test
  void similarToExpressionsRespectNotFlag() throws Exception {
    Expression positiveSimilarTo = CCJSqlParserUtil.parseExpression("col SIMILAR TO 'a%'");
    Expression negativeSimilarTo = CCJSqlParserUtil.parseExpression("col NOT SIMILAR TO 'a%'");

    assertTrue(expressionsEquivalent(positiveSimilarTo, positiveSimilarTo));
    assertTrue(expressionsEquivalent(negativeSimilarTo, negativeSimilarTo));
    assertFalse(expressionsEquivalent(positiveSimilarTo, negativeSimilarTo));
  }

  @Test
  void likeExpressionsRespectEscapeClause() throws Exception {
    Expression withEscape = CCJSqlParserUtil.parseExpression("col LIKE 'a!%' ESCAPE '!'");
    assertTrue(expressionsEquivalent(withEscape, withEscape));

    Expression withoutEscape = CCJSqlParserUtil.parseExpression("col LIKE 'a!%'");
    assertTrue(expressionsEquivalent(withoutEscape, withoutEscape));
    assertFalse(expressionsEquivalent(withEscape, withoutEscape),
        "LIKE with ESCAPE should not be equivalent to LIKE without ESCAPE");

    Expression withDifferentEscape = CCJSqlParserUtil.parseExpression("col LIKE 'a!%' ESCAPE '\\'");
    assertFalse(expressionsEquivalent(withEscape, withDifferentEscape),
        "LIKE with different ESCAPE characters should not be equivalent");
  }

  @Test
  void similarToExpressionsRespectEscapeClause() throws Exception {
    Expression withEscape = CCJSqlParserUtil.parseExpression("col SIMILAR TO 'a!%' ESCAPE '!'");
    assertTrue(expressionsEquivalent(withEscape, withEscape));

    Expression withoutEscape = CCJSqlParserUtil.parseExpression("col SIMILAR TO 'a!%'");
    assertTrue(expressionsEquivalent(withoutEscape, withoutEscape));
    assertFalse(expressionsEquivalent(withEscape, withoutEscape),
        "SIMILAR TO with ESCAPE should not be equivalent to SIMILAR TO without ESCAPE");

    Expression withDifferentEscape = CCJSqlParserUtil.parseExpression("col SIMILAR TO 'a!%' ESCAPE '\\'");
    assertFalse(expressionsEquivalent(withEscape, withDifferentEscape),
        "SIMILAR TO with different ESCAPE strings should not be equivalent");
  }
}
