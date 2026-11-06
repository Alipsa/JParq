package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TranscodingFunction;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.ScalarExpressionSchemas;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.helper.LiteralConverter;
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

  /**
   * Direct evaluation cannot silently bypass an ambiguous conversion target.
   */
  @Test
  void rejectsAmbiguousConvertInsteadOfReturningOperand() {
    var type = new ColDataType();
    type.setDataType("VARCHAR");
    var convert = new TranscodingFunction();
    convert.setTranscodeStyle(false);
    convert.setColDataType(type);
    convert.setExpression(new Column("INTEGER"));
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> evaluator.eval(convert, nullNoteRecord));
    assertEquals("42000", assertInstanceOf(java.sql.SQLException.class, error.getCause()).getSQLState());
    assertTrue(error.getMessage().contains("Ambiguous CONVERT arguments"));
  }

  /**
   * Reuses compiled binary bounds while reading fresh values and returning
   * independent arrays.
   *
   * @throws Exception
   *           if the expression cannot be parsed
   */
  @Test
  void compilesBinaryCastLengthOnceAcrossRows() throws Exception {
    CastExpression cast = (CastExpression) CCJSqlParserUtil.parseExpression("CAST(note AS BINARY(4))");
    var row = new GenericData.Record(schema);
    var values = new ValueExpressionEvaluator(schema);
    try (var declarations = mockStatic(ScalarExpressionSchemas.class, CALLS_REAL_METHODS)) {
      row.put("note", "xy");
      byte[] first = assertInstanceOf(byte[].class, values.eval(cast, row));
      assertArrayEquals(new byte[]{
          120, 121, 0, 0
      }, first);
      first[0] = 0;
      row.put("note", "abcdz");
      assertArrayEquals(new byte[]{
          97, 98, 99, 100
      }, assertInstanceOf(byte[].class, values.eval(cast, row)));
      row.put("note", null);
      assertNull(values.eval(cast, row));
      declarations.verify(() -> ScalarExpressionSchemas.binaryLength(cast), times(1));
    }
  }

  /**
   * Cast-style CONVERT keeps one compiled binary declaration across current row
   * values.
   *
   * @param sql
   *          CONVERT in either supported argument order
   * @throws Exception
   *           if expression parsing fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CONVERT(note, BINARY(4))", "CONVERT(BINARY(4), note)"
  })
  void compilesConvertBinaryLengthOnceAcrossRows(String sql) throws Exception {
    Expression convert = CCJSqlParserUtil.parseExpression(sql);
    var values = new ValueExpressionEvaluator(schema);
    var row = new GenericData.Record(schema);
    try (var declarations = mockStatic(ScalarExpressionSchemas.class, CALLS_REAL_METHODS)) {
      row.put("note", "xy");
      assertArrayEquals(new byte[]{
          120, 121, 0, 0
      }, assertInstanceOf(byte[].class, values.eval(convert, row)));
      row.put("note", "abcde");
      assertArrayEquals(new byte[]{
          97, 98, 99, 100
      }, assertInstanceOf(byte[].class, values.eval(convert, row)));
      row.put("note", null);
      assertNull(values.eval(convert, row));
      declarations.verify(() -> ScalarExpressionSchemas.binaryLength(any(CastExpression.class)), times(1));
    }
  }

  /**
   * One-shot literal folding uses current values and declarations without global
   * caching.
   *
   * @throws Exception
   *           if expression parsing fails
   */
  @Test
  void foldsBinaryLiteralsUsingCurrentValuesAndDeclarations() throws Exception {
    CastExpression cast = (CastExpression) CCJSqlParserUtil.parseExpression("CAST('abcd' AS VARBINARY(3))");
    byte[] first = assertInstanceOf(byte[].class, LiteralConverter.toLiteral(cast));
    assertArrayEquals(new byte[]{
        97, 98, 99
    }, first);
    first[0] = 0;
    cast.setLeftExpression(new StringValue("wxyz"));
    assertArrayEquals(new byte[]{
        119, 120, 121
    }, assertInstanceOf(byte[].class, LiteralConverter.toLiteral(cast)));
    var shorter = new ColDataType();
    shorter.setDataType("VARBINARY");
    shorter.setArgumentsStringList(List.of("2"));
    cast.setColDataType(shorter);
    assertArrayEquals(new byte[]{
        119, 120
    }, assertInstanceOf(byte[].class, LiteralConverter.toLiteral(cast)));
  }

  private Object eval(String sqlExpression, GenericRecord record) throws Exception {
    Expression expression = CCJSqlParserUtil.parseExpression(sqlExpression);
    return evaluator.eval(expression, record);
  }

  /**
   * Supports direct evaluation of raw parser expressions without letting physical
   * fields override bare datetime keywords.
   *
   * @param keyword
   *          datetime keyword parsed inside a scalar function
   * @throws Exception
   *           if parsing or evaluation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LOCALTIME", "LOCALTIMESTAMP"
  })
  void evaluatesRawDatetimeKeywords(String keyword) throws Exception {
    Schema keywordSchema = SchemaBuilder.record("KeywordFields").fields().requiredString("localtime")
        .requiredString("localtimestamp").endRecord();
    GenericRecord record = new GenericData.Record(keywordSchema);
    record.put("localtime", "stored time");
    record.put("localtimestamp", "stored timestamp");
    Expression expression = CCJSqlParserUtil.parseExpression("COALESCE(" + keyword + ", NULL)");
    Object value = new ValueExpressionEvaluator(keywordSchema).eval(expression, record);
    if ("LOCALTIME".equals(keyword)) {
      assertInstanceOf(LocalTime.class, value);
    } else {
      assertInstanceOf(LocalDateTime.class, value);
    }
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
    assertEquals(-2L, eval("-2", nullNoteRecord));
    assertEquals(2L, eval("+2", nullNoteRecord));
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
