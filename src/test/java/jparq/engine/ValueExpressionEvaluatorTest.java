package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;

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
