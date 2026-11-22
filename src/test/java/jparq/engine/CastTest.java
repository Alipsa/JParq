package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.SqlExceptions;
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

  /**
   * Converts the shared CAST AS BOOLEAN text forms, including blank text.
   *
   * @param text
   *          the quoted SQL string literal to cast
   * @param expected
   *          the expected boolean
   * @throws Exception
   *           if parsing the expression fails
   */
  @ParameterizedTest
  @CsvSource(value = {
      "\"'true'\" | true", "\"'TRUE'\" | true", "\"'t'\" | true", "\"'yes'\" | true", "\"'y'\" | true",
      "\"'1'\" | true", "\"'false'\" | false", "\"'FALSE'\" | false", "\"'f'\" | false", "\"'no'\" | false",
      "\"'n'\" | false", "\"'0'\" | false", "\"''\" | false", "\"'   '\" | false"
  }, delimiter = '|', quoteCharacter = '"')
  void castTextFormsToBoolean(String text, boolean expected) throws Exception {
    assertEquals(expected, eval("CAST(" + text + " AS BOOLEAN)"));
  }

  /**
   * Classifies invalid CAST AS BOOLEAN text with SQLState 22018.
   *
   * @param text
   *          the quoted SQL string literal to cast
   * @throws Exception
   *           if parsing the expression fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'2'", "'1.5'", "'abc'", "'NaN'"
  })
  void castInvalidTextToBooleanIsClassified(String text) throws Exception {
    var failure = assertThrows(IllegalArgumentException.class, () -> eval("CAST(" + text + " AS BOOLEAN)"));
    String value = text.substring(1, text.length() - 1);
    assertEquals("Cannot cast value '" + value + "' to BOOLEAN", failure.getMessage());
    assertEquals("22018",
        assertInstanceOf(SQLDataException.class, SqlExceptions.findSqlStateFailure(failure)).getSQLState());
  }

  private Object eval(String expression) throws Exception {
    Expression parsed = CCJSqlParserUtil.parseExpression(expression);
    return evaluator.eval(parsed, record);
  }
}
