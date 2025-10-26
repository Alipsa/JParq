package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.ExpressionEvaluator;

/**
 * Characterization tests for {@link ExpressionEvaluator}.
 *
 * <p>
 * Goal: lock down current external behavior so future refactors can be verified
 * as "no semantics change" by keeping these tests green.
 * </p>
 */
class ExpressionEvaluatorCharacterizationTest {

  private static Schema schema;
  private static GenericRecord rec;
  private static ExpressionEvaluator evaluator;

  @BeforeAll
  static void setUp() {
    // Avro schema with typical types (note: "note" is nullable for IS NULL tests)
    schema = SchemaBuilder.record("X").fields().name("age").type().intType().noDefault().name("name").type()
        .stringType().noDefault().name("active").type().booleanType().noDefault().name("note").type().unionOf()
        .nullType().and().stringType().endUnion().nullDefault().endRecord();

    rec = new GenericData.Record(schema);
    rec.put("age", 42);
    rec.put("name", "Per");
    rec.put("active", true);
    rec.put("note", null);

    evaluator = new ExpressionEvaluator(schema);
  }

  private boolean eval(String sqlWhere) throws Exception {
    Expression e = CCJSqlParserUtil.parseCondExpression(sqlWhere);
    return evaluator.eval(e, rec);
  }

  @Nested
  @DisplayName("Basic comparisons")
  class Comparisons {

    @Test
    void equalsNumber() throws Exception {
      assertTrue(eval("age = 42"));
      assertFalse(eval("age = 7"));
    }

    @Test
    void greaterAndLess() throws Exception {
      assertTrue(eval("age > 40"));
      assertFalse(eval("age > 100"));

      assertTrue(eval("age < 100"));
      assertFalse(eval("age < 10"));
    }

    @Test
    void greaterEqualsAndLessEquals() throws Exception {
      assertTrue(eval("age >= 42"));
      assertTrue(eval("age <= 42"));
      assertFalse(eval("age <= 10"));
    }

    @Test
    void equalsString() throws Exception {
      assertTrue(eval("name = 'Per'"));
      assertFalse(eval("name = 'Other'"));
    }

    @Test
    void literalCoercionToColumnType() throws Exception {
      // Column age is INT; literal is STRING -> should coerce and compare equal
      assertTrue(eval("age = '42'"));
      assertFalse(eval("age = '0043'"));
    }
  }

  @Nested
  @DisplayName("Boolean logic and parentheses")
  class BooleanLogic {

    @Test
    void andOrNot() throws Exception {
      assertTrue(eval("age > 40 AND active = true"));
      assertTrue(eval("(age > 40 AND active = true) OR name = 'X'"));
      assertTrue(eval("NOT(name = 'X')"));
      assertFalse(eval("NOT(active = true)"));
    }

    @Test
    void nestedParenthesesReparseBehavior() throws Exception {
      // Evaluator special-cases fully parenthesized text and re-parses; this should
      // still be true
      assertTrue(eval("((age = 42))"));
    }
  }

  @Nested
  @DisplayName("NULL / LIKE / IN / BETWEEN")
  class Operators {

    @Test
    void isNullAndIsNotNull() throws Exception {
      assertTrue(eval("note IS NULL"));
      assertFalse(eval("note IS NOT NULL"));
    }

    @Test
    void likeAndIlikeAndNotLike() throws Exception {
      assertTrue(eval("name LIKE 'P%'"));
      assertTrue(eval("name ILIKE 'p%'"));
      assertTrue(eval("name NOT LIKE 'X%'"));
      assertFalse(eval("name LIKE 'X%'"));
    }

    @Test
    void inList() throws Exception {
      assertTrue(eval("age IN (41, 42, 43)"));
      assertFalse(eval("age IN (1,2,3)"));
    }

    @Test
    void betweenInclusive() throws Exception {
      assertTrue(eval("age BETWEEN 40 AND 50"));
      assertTrue(eval("age BETWEEN 42 AND 42"));
      assertFalse(eval("age BETWEEN 0 AND 10"));
    }
  }

  @Nested
  @DisplayName("Case-insensitive columns")
  class CaseInsensitiveColumns {

    @Test
    void mixedCaseColumnNameResolves() throws Exception {
      // The evaluator builds a lower(name)->canonical index; this should resolve
      assertTrue(eval("NaMe = 'Per'"));
      assertTrue(eval("AGE = 42"));
      assertTrue(eval("aCtIvE = true"));
    }
  }
}
