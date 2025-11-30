package jparq.window;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.window.WindowFunctions;
import se.alipsa.jparq.engine.window.WindowPlan;
import se.alipsa.jparq.engine.window.WindowState;

/**
 * Tests covering RANGE frame handling across aggregate window functions.
 */
class WindowFunctionsRangeFrameTest {

  private static final Schema SCHEMA = SchemaBuilder.record("RangeRow").fields().requiredInt("value").endRecord();

  /**
   * Verify RANGE window frames across multiple aggregate functions.
   *
   * @throws JSQLParserException
   *           if expressions cannot be parsed
   */
  @Test
  void rangeFramesAreEvaluated() throws JSQLParserException {
    AnalyticExpression sumTotal = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("SUM(value) OVER (ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)");
    AnalyticExpression sumFollowing = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("SUM(value) OVER (ORDER BY value RANGE UNBOUNDED FOLLOWING)");
    AnalyticExpression avgCurrent = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("AVG(value) OVER (ORDER BY value RANGE BETWEEN CURRENT ROW AND CURRENT ROW)");
    AnalyticExpression minFollowing = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("MIN(value) OVER (ORDER BY value RANGE UNBOUNDED FOLLOWING)");
    AnalyticExpression maxCurrent = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("MAX(value) OVER (ORDER BY value RANGE CURRENT ROW)");
    AnalyticExpression maxFollowing = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("MAX(value) OVER (ORDER BY value RANGE UNBOUNDED FOLLOWING)");
    AnalyticExpression countFollowing = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("COUNT(value) OVER (ORDER BY value RANGE UNBOUNDED FOLLOWING)");
    AnalyticExpression countCurrent = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("COUNT(value) OVER (ORDER BY value RANGE BETWEEN CURRENT ROW AND CURRENT ROW)");

    List<AnalyticExpression> expressions = List.of(sumTotal, sumFollowing, avgCurrent, minFollowing, maxCurrent,
        maxFollowing, countFollowing, countCurrent);
    WindowPlan plan = WindowFunctions.plan(List.copyOf(expressions));
    assertNotNull(plan, "Expected a plan to be produced for RANGE frame analytics");

    List<GenericRecord> records = sampleRecords();
    WindowState state = WindowFunctions.compute(plan, records, SCHEMA, null, List.of(), Map.of(), Map.of());

    long total = 10L;
    int minValue = 1;
    long totalCount = records.size();
    Map<Integer, Integer> countsPerValue = new HashMap<>();
    for (GenericRecord record : records) {
      int value = (Integer) record.get("value");
      countsPerValue.merge(value, 1, Integer::sum);

      assertEquals(total, ((Number) state.sum(sumTotal, record)).longValue(),
          "UNBOUNDED PRECEDING/FOLLOWING should yield the total sum");
      assertEquals(total, ((Number) state.sum(sumFollowing, record)).longValue(),
          "UNBOUNDED FOLLOWING offset should produce the same total sum");
      assertEquals(value, ((Number) state.avg(avgCurrent, record)).intValue(),
          "CURRENT ROW frame should isolate the row (or peers) for AVG");
      assertEquals(minValue, ((Number) state.min(minFollowing, record)).intValue(),
          "UNBOUNDED FOLLOWING should propagate the minimum value");
      assertEquals(value, ((Number) state.max(maxCurrent, record)).intValue(),
          "CURRENT ROW offset should cap the MAX at the peer group value");
      assertEquals(5, ((Number) state.max(maxFollowing, record)).intValue(),
          "UNBOUNDED FOLLOWING offset should expose the partition maximum");
      assertEquals(totalCount, state.count(countFollowing, record),
          "UNBOUNDED FOLLOWING offset should return the full partition count");
    }

    for (GenericRecord record : records) {
      int value = (Integer) record.get("value");
      assertEquals(countsPerValue.get(value).longValue(), state.count(countCurrent, record),
          "CURRENT ROW frame should count peers sharing the ORDER BY value");
    }
  }

  /**
   * Ensure RANGE navigation functions are evaluated for different boundary
   * shapes.
   *
   * @throws JSQLParserException
   *           if expressions cannot be parsed
   */
  @Test
  void navigationRangeFramesAreEvaluated() throws JSQLParserException {
    AnalyticExpression firstFollowing = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("FIRST_VALUE(value) OVER (ORDER BY value RANGE UNBOUNDED FOLLOWING)");
    AnalyticExpression firstCurrent = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("FIRST_VALUE(value) OVER (ORDER BY value RANGE BETWEEN CURRENT ROW AND CURRENT ROW)");
    AnalyticExpression lastFollowing = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("LAST_VALUE(value) OVER (ORDER BY value RANGE UNBOUNDED FOLLOWING)");
    AnalyticExpression lastCurrent = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("LAST_VALUE(value) OVER (ORDER BY value RANGE BETWEEN CURRENT ROW AND CURRENT ROW)");
    AnalyticExpression nthPartitionWide = (AnalyticExpression) CCJSqlParserUtil.parseExpression(
        "NTH_VALUE(value, 2) OVER (ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING " + "AND UNBOUNDED FOLLOWING)");
    AnalyticExpression nthCurrentRow = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("NTH_VALUE(value, 2) OVER (ORDER BY value RANGE BETWEEN CURRENT ROW AND CURRENT ROW)");
    AnalyticExpression nthFollowing = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("NTH_VALUE(value, 2) OVER (ORDER BY value RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)");

    List<AnalyticExpression> expressions = List.of(firstFollowing, firstCurrent, lastFollowing, lastCurrent,
        nthPartitionWide, nthCurrentRow, nthFollowing);
    WindowPlan plan = WindowFunctions.plan(List.copyOf(expressions));
    assertNotNull(plan, "Navigation RANGE analytics should be collected");

    List<GenericRecord> records = sampleRecords();
    WindowState state = WindowFunctions.compute(plan, records, SCHEMA, null, List.of(), Map.of(), Map.of());

    for (GenericRecord record : records) {
      int value = (Integer) record.get("value");
      assertEquals(1, ((Number) state.firstValue(firstFollowing, record)).intValue(),
          "UNBOUNDED FOLLOWING should return the first value of the partition");
      assertEquals(value, ((Number) state.firstValue(firstCurrent, record)).intValue(),
          "CURRENT ROW frame should align FIRST_VALUE with the peer group start");
      assertEquals(5, ((Number) state.lastValue(lastFollowing, record)).intValue(),
          "UNBOUNDED FOLLOWING should return the partition tail");
      assertEquals(value, ((Number) state.lastValue(lastCurrent, record)).intValue(),
          "CURRENT ROW frame should align LAST_VALUE with the peer group end");
      assertEquals(2, ((Number) state.nthValue(nthPartitionWide, record)).intValue(),
          "Partition wide NTH_VALUE should be stable across rows");
    }

    Map<Integer, Integer> indexByValue = Map.of(1, 0, 2, 1, 5, 3);
    for (GenericRecord record : records) {
      int value = (Integer) record.get("value");
      Object currentNth = state.nthValue(nthCurrentRow, record);
      if (value == 2) {
        assertEquals(2, ((Number) currentNth).intValue(), "Peer group of size two should expose the second value");
      } else {
        assertNull(currentNth, "Frames smaller than N should yield null");
      }

      int position = indexByValue.get(value);
      Object followingNth = state.nthValue(nthFollowing, record);
      if (position <= 1) {
        assertEquals(2, ((Number) followingNth).intValue(),
            "Frame extending to UNBOUNDED FOLLOWING should expose the second item when available");
      } else {
        assertNull(followingNth, "Trailing frames shorter than N should yield null");
      }
    }
  }

  /**
   * Validate ROWS frame evaluation for COUNT windows.
   *
   * @throws JSQLParserException
   *           if the expression cannot be parsed
   */
  @Test
  void rowsFrameCountUsesBounds() throws JSQLParserException {
    AnalyticExpression countRows = (AnalyticExpression) CCJSqlParserUtil
        .parseExpression("COUNT(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)");
    WindowPlan plan = WindowFunctions.plan(List.of(countRows));
    assertNotNull(plan, "ROWS COUNT analytic should be planned");

    List<GenericRecord> records = sampleRecords();
    WindowState state = WindowFunctions.compute(plan, records, SCHEMA, null, List.of(), Map.of(), Map.of());

    List<GenericRecord> sorted = records.stream()
        .sorted((left, right) -> Integer.compare((Integer) left.get("value"), (Integer) right.get("value"))).toList();

    long[] expected = new long[]{
        2L, 3L, 3L, 2L
    };
    for (int i = 0; i < sorted.size(); i++) {
      GenericRecord record = sorted.get(i);
      assertEquals(expected[i], state.count(countRows, record),
          "ROWS frame should count the bounded neighborhood for record index " + i);
    }
  }

  private static GenericRecord record(int value) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("value", value);
    return record;
  }

  private static List<GenericRecord> sampleRecords() {
    return List.of(record(1), record(2), record(2), record(5));
  }
}
