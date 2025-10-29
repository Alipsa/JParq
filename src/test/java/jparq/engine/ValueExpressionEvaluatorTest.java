package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
