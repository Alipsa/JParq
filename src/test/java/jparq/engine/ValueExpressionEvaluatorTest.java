package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.helper.TemporalInterval;

class ValueExpressionEvaluatorTest {

  private static Schema schema;
  private static GenericRecord nullNoteRecord;
  private static ValueExpressionEvaluator evaluator;

  @BeforeAll
  static void setUp() {
    schema = SchemaBuilder.record("R").fields().name("note").type().unionOf().nullType().and().stringType().endUnion()
        .nullDefault().endRecord();

    nullNoteRecord = new GenericData.Record(schema);
    nullNoteRecord.put("note", null);

    evaluator = new ValueExpressionEvaluator(schema);
  }

  private Object eval(String sqlExpression, GenericRecord record) throws Exception {
    Expression expression = CCJSqlParserUtil.parseExpression(sqlExpression);
    return evaluator.eval(expression, record);
  }

  /**
   * Rejects parsed SQL/JSON options until their behavior is implemented.
   *
   * @param expression
   *          JSON function containing an unsupported clause
   * @throws Exception
   *           if parsing fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "JSON_VALUE('{\"a\":1}', '$.a' RETURNING INTEGER)", "JSON_VALUE('{\"a\":1}', '$.x' DEFAULT 0 ON EMPTY)",
      "JSON_VALUE('{\"a\":1}', '$.a' NULL ON ERROR)", "JSON_VALUE('{\"a\":1}', '$.a' PASSING 1)",
      "JSON_QUERY('{\"a\":1}', '$.a' RETURNING VARCHAR)", "JSON_QUERY('{\"a\":1}', '$.x' EMPTY ARRAY ON EMPTY)",
      "JSON_QUERY('{\"a\":1}', '$.a' ERROR ON ERROR)", "JSON_QUERY('{\"a\":1}', '$.a' PASSING 1)",
      "JSON_QUERY('{\"a\":1}', '$.a' WITH ARRAY WRAPPER)", "JSON_QUERY('{\"a\":1}', '$.a' WITHOUT ARRAY WRAPPER)",
      "JSON_QUERY('{\"a\":1}', '$.a', '$.b')", "JSON_QUERY('{\"a\":1}', '$.a' KEEP QUOTES)",
      "JSON_QUERY('{\"a\":1}', '$.a' OMIT QUOTES)"
  })
  void unsupportedJsonClausesFailClearly(String expression) throws Exception {
    Expression parsed = CCJSqlParserUtil.parseExpression(expression);
    var failure = assertThrows(IllegalArgumentException.class, () -> evaluator.eval(parsed, nullNoteRecord));
    assertTrue(failure.getMessage().startsWith("Unsupported JSON_"), failure::getMessage);
  }

  /**
   * Preserves numeric, NULL, and interval values under unary signs.
   *
   * @throws Exception
   *           if expression parsing fails
   */
  @Test
  void evaluatesSignedOperands() throws Exception {
    assertEquals(new BigDecimal("-2"), eval("-2", nullNoteRecord));
    assertEquals(new BigDecimal("2"), eval("+2", nullNoteRecord));
    assertNull(eval("-NULL", nullNoteRecord));
    assertEquals(TemporalInterval.parse("-2", "DAY"), eval("-INTERVAL '2 days'", nullNoteRecord));
    assertEquals(TemporalInterval.parse("2", "DAY"), eval("+INTERVAL '2 days'", nullNoteRecord));
  }

  @Test
  void simpleCaseDoesNotMatchNullBranch() throws Exception {
    Object result = eval("CASE note WHEN NULL THEN 'x' ELSE 'y' END", nullNoteRecord);
    assertEquals("y", result, "NULL should not match the WHEN NULL branch in a simple CASE expression");
  }

  @Test
  void concatenationOperatorSupportsBinaryAndNullPropagation() throws Exception {
    Schema binaryType = SchemaBuilder.builder().bytesType();
    binaryType.addProp("jparq.binary", true);
    Schema binarySchema = SchemaBuilder.record("B").fields().name("payload").type().unionOf().nullType().and()
        .type(binaryType).endUnion().nullDefault().name("suffix").type().unionOf().nullType().and().type(binaryType)
        .endUnion().nullDefault().endRecord();
    GenericRecord record = new GenericData.Record(binarySchema);
    record.put("payload", ByteBuffer.wrap(new byte[]{
        0x41, 0x42
    }));
    record.put("suffix", ByteBuffer.wrap(new byte[]{
        0x43
    }));
    ValueExpressionEvaluator binaryEvaluator = new ValueExpressionEvaluator(binarySchema);
    Expression expression = CCJSqlParserUtil.parseExpression("payload || suffix");

    Object concatenated = binaryEvaluator.eval(expression, record);
    assertArrayEquals(new byte[]{
        0x41, 0x42, 0x43
    }, (byte[]) concatenated, "Binary concatenation should preserve all bytes in order");

    record.put("suffix", null);
    assertNull(binaryEvaluator.eval(expression, record), "Concatenation should yield null when any operand is null");
  }
}
