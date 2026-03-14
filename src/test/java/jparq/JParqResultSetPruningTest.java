package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Method;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.engine.Identifier;

/**
 * Tests for predicate pruning helpers in {@link JParqResultSet}.
 */
class JParqResultSetPruningTest {

  /**
   * Verify grouped legacy parenthesis expressions still recurse for join-side
   * pruning.
   *
   * @throws Exception
   *           if reflective access to the helper fails
   */
  @Test
  void pruneExpressionRecursesIntoLegacyParenthesis() throws Exception {
    Expression expression = CCJSqlParserUtil.parseExpression("(cars.model = 'Mazda' AND garage.id = 1)");

    Expression pruned = invokePruneExpression(expression, Set.of(Identifier.lookupKey("cars")));

    assertEquals("(cars.model = 'Mazda')", pruned.toString());
  }

  /**
   * Verify single wrapped parenthesized lists recurse while multi-expression
   * lists are not partially pruned.
   *
   * @throws Exception
   *           if reflective access to the helper fails
   */
  @Test
  void pruneExpressionOnlyTreatsSingleItemParenthesizedListsAsWrappers() throws Exception {
    ParenthesedExpressionList<Expression> singleItemList = new ParenthesedExpressionList<>(
        CCJSqlParserUtil.parseExpression("cars.model = 'Mazda' AND garage.id = 1"));

    Expression singlePruned = invokePruneExpression(singleItemList, Set.of(Identifier.lookupKey("cars")));

    ParenthesedExpressionList<?> rewritten = assertInstanceOf(ParenthesedExpressionList.class, singlePruned);
    assertEquals("(cars.model = 'Mazda')", rewritten.toString());

    ParenthesedExpressionList<Expression> multiItemList = new ParenthesedExpressionList<>(
        CCJSqlParserUtil.parseExpression("cars.model"), CCJSqlParserUtil.parseExpression("garage.id"));

    Expression multiPruned = invokePruneExpression(multiItemList, Set.of(Identifier.lookupKey("cars")));

    assertNull(multiPruned);
  }

  /**
   * Invoke the private pruning helper reflectively.
   *
   * @param expression
   *          expression to prune
   * @param availableQualifiers
   *          qualifiers that remain available
   * @return pruned expression or {@code null}
   * @throws Exception
   *           if reflective access fails
   */
  @SuppressWarnings("unchecked")
  private Expression invokePruneExpression(Expression expression, Set<String> availableQualifiers) throws Exception {
    Method method = JParqResultSet.class.getDeclaredMethod("pruneExpression", Expression.class, Set.class);
    method.setAccessible(true);
    return (Expression) method.invoke(null, expression, availableQualifiers);
  }
}
