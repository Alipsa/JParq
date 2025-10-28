package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;

/** Tests for CAST expression evaluation in {@link ValueExpressionEvaluator}. */
class CastTest {

  private Schema schema;
  private GenericRecord record;
  private ValueExpressionEvaluator evaluator;

  @BeforeEach
  void setUp() {
    schema = SchemaBuilder.record("Sample").fields().name("price").type().doubleType().noDefault().name("name").type()
        .stringType().noDefault().endRecord();
    record = new GenericData.Record(schema);
    record.put("price", 25.65d);
    record.put("name", "LongerThanFive");
    evaluator = new ValueExpressionEvaluator(schema);
  }

  @Test
  void castNumericLiteralToInt() throws Exception {
    Object value = eval("CAST(25.65 AS INT)");
    Integer intValue = assertInstanceOf(Integer.class, value);
    assertEquals(25, intValue.intValue());
  }

  @Test
  void castColumnToInt() throws Exception {
    Object value = eval("CAST(price AS INT)");
    Integer intValue = assertInstanceOf(Integer.class, value);
    assertEquals(25, intValue.intValue());
  }

  @Test
  void castToVarcharWithLengthLimit() throws Exception {
    Object value = eval("CAST(name AS VARCHAR(5))");
    String text = assertInstanceOf(String.class, value);
    assertEquals("Longe", text);
  }

  @Test
  void castNumberToVarchar() throws Exception {
    Object value = eval("CAST(price AS VARCHAR(6))");
    String text = assertInstanceOf(String.class, value);
    assertEquals("25.65", text);
  }

  @Test
  void castToDecimalAppliesScale() throws Exception {
    Object value = eval("CAST(price AS DECIMAL(10, 1))");
    BigDecimal decimal = assertInstanceOf(BigDecimal.class, value);
    assertEquals(new BigDecimal("25.7"), decimal);
  }

  @Test
  void castNumberToBoolean() throws Exception {
    Object value = eval("CAST(price AS BOOLEAN)");
    Boolean bool = assertInstanceOf(Boolean.class, value);
    assertTrue(bool.booleanValue());
  }

  @Test
  void castZeroToBoolean() throws Exception {
    Object value = eval("CAST(0 AS BOOLEAN)");
    Boolean bool = assertInstanceOf(Boolean.class, value);
    assertFalse(bool.booleanValue());
  }

  private Object eval(String expression) throws Exception {
    Expression parsed = CCJSqlParserUtil.parseExpression(expression);
    return evaluator.eval(parsed, record);
  }
}
