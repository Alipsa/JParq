package jparq.window;

import java.util.IdentityHashMap;
import java.util.Map;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.window.WindowState;

/**
 * Tests for verifying the {@link WindowState.Builder} behavior.
 */
class WindowStateBuilderTest {

  /**
   * Ensure that the builder produces an empty state when no values are provided.
   */
  @Test
  void builderProducesEmptyStateByDefault() {
    WindowState state = WindowState.builder().build();

    Assertions.assertTrue(state.isEmpty(), "Expected state to be empty when no values are supplied");
  }

  /**
   * Verify that configured analytic results are captured by the builder.
   *
   * @throws JSQLParserException
   *           if analytic expressions cannot be parsed
   */
  @Test
  void builderCapturesRowNumberValues() throws JSQLParserException {
    AnalyticExpression expression = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)");

    Schema schema = SchemaBuilder.record("Employee").fields().requiredString("dept").endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("dept", "Engineering");

    IdentityHashMap<GenericRecord, Long> rowNumberMap = new IdentityHashMap<>();
    rowNumberMap.put(record, 1L);

    Map<AnalyticExpression, IdentityHashMap<GenericRecord, Long>> rowNumberState = new IdentityHashMap<>();
    rowNumberState.put(expression, rowNumberMap);

    WindowState state = WindowState.builder().rowNumberValues(rowNumberState).build();

    rowNumberState.clear();

    Assertions.assertFalse(state.isEmpty(), "Expected state to contain configured values");
    Assertions.assertEquals(1L, state.rowNumber(expression, record), "Unexpected row number value");
    Assertions.assertThrows(IllegalArgumentException.class, () -> state.rank(expression, record),
        "Rank lookup should fail when no values are present");
  }

  /**
   * Verify that configured LAG results are captured by the builder.
   *
   * @throws JSQLParserException
   *           if analytic expressions cannot be parsed
   */
  @Test
  void builderCapturesLagValues() throws JSQLParserException {
    AnalyticExpression expression = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("LAG(salary, 1, 0) OVER (ORDER BY salary)");

    Schema schema = SchemaBuilder.record("Employee").fields().requiredDouble("salary").endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("salary", 1000.0);

    IdentityHashMap<GenericRecord, Object> lagMap = new IdentityHashMap<>();
    lagMap.put(record, 900.0);

    Map<AnalyticExpression, IdentityHashMap<GenericRecord, Object>> lagState = new IdentityHashMap<>();
    lagState.put(expression, lagMap);

    WindowState state = WindowState.builder().lagValues(lagState).build();

    lagState.clear();

    Assertions.assertFalse(state.isEmpty(), "Expected state to contain configured values");
    Assertions.assertEquals(900.0, state.lag(expression, record));
    Assertions.assertThrows(IllegalArgumentException.class, () -> state.max(expression, record),
        "MAX lookup should fail when no values are present");
  }
}
