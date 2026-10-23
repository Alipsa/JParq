package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.QuantifiedComparisons;

/** Direct contract tests for shared quantified comparison evaluation. */
class QuantifiedComparisonsTest {

  /** Rejects invalid public operands before invoking any context callback. */
  @Test
  void rejectsNonQuantifiedOperands() {
    for (Expression operand : Arrays.asList(null, new LongValue(1), new Function().withName("FOO"))) {
      var exception = assertThrows(IllegalArgumentException.class,
          () -> QuantifiedComparisons.evaluate(operand, 1, "=", false, expression -> {
            throw new AssertionError("Unexpected array evaluation");
          }, select -> {
            throw new AssertionError("Unexpected subquery evaluation");
          }, UnaryOperator.identity()));
      assertEquals("Not a quantified operand: " + operand, exception.getMessage());
    }
  }

  /**
   * Validates every candidate before a decisive comparison can short circuit.
   *
   * @param quantifier
   *          the quantified keyword
   * @throws Exception
   *           if parsing fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "ANY", "SOME", "ALL"
  })
  void rejectsStructuredSubqueryElements(String quantifier) throws Exception {
    var record = new GenericData.Record(SchemaBuilder.record("candidate").fields().endRecord());
    var comparison = (BinaryExpression) CCJSqlParserUtil.parseCondExpression("32 = " + quantifier + "(SELECT 32)");
    Object decisive = "ALL".equals(quantifier) ? 1 : 32;
    for (Object structured : List.of(List.of(32), Map.of("n", 32), new Object[]{
        32
    }, record)) {
      var exception = assertThrows(IllegalArgumentException.class, () -> QuantifiedComparisons.evaluate(comparison,
          expression -> new QuantifiedComparisons.ScalarOperand(32, UnaryOperator.identity()), expression -> {
            throw new AssertionError("Unexpected array evaluation");
          }, select -> List.of(decisive, structured)));
      assertEquals("ANY/SOME/ALL requires scalar array elements", exception.getMessage());
    }
  }

  /**
   * Resolves and coerces the scalar exactly once in either operand order.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void dispatchesBothSidesAndPreservesCoercion() throws Exception {
    for (String predicate : List.of("1 = ANY(ARRAY[2])", "ANY(ARRAY[2]) = 1")) {
      var comparison = (BinaryExpression) CCJSqlParserUtil.parseCondExpression(predicate);
      var resolutions = new AtomicInteger();
      assertEquals(true, QuantifiedComparisons.evaluate(comparison, expression -> {
        resolutions.incrementAndGet();
        return new QuantifiedComparisons.ScalarOperand(1, candidate -> 1);
      }, expression -> List.of(2), select -> {
        throw new AssertionError("Unexpected subquery evaluation");
      }), predicate);
      assertEquals(1, resolutions.get(), predicate);
    }
    for (String predicate : List.of("1 = 1", "ANY(ARRAY[1]) = ALL(ARRAY[1])")) {
      var comparison = (BinaryExpression) CCJSqlParserUtil.parseCondExpression(predicate);
      assertThrows(IllegalArgumentException.class, () -> QuantifiedComparisons.evaluate(comparison, expression -> {
        throw new AssertionError("Unexpected scalar evaluation");
      }, expression -> {
        throw new AssertionError("Unexpected array evaluation");
      }, select -> {
        throw new AssertionError("Unexpected subquery evaluation");
      }), predicate);
    }
  }
}
