package jparq.window;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static se.alipsa.jparq.engine.window.WindowFunctions.compareValues;
import static se.alipsa.jparq.engine.window.WindowFunctions.nthValueFromRelativeFrame;
import static se.alipsa.jparq.engine.window.WindowFunctions.resolveNthPosition;
import static se.alipsa.jparq.engine.window.WindowFunctions.resolvePositiveBucketCount;
import static se.alipsa.jparq.engine.window.WindowFunctions.resolvePositiveOffset;
import static se.alipsa.jparq.engine.window.WindowFunctions.safeLongToInt;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.window.WindowFunctions;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Unit tests covering validation helpers inside {@link WindowFunctions}.
 */
public class WindowFunctionsValidationTest {

  private static AnalyticExpression analyticExpression;

  @BeforeAll
  static void setUpReflection() throws NoSuchMethodException, JSQLParserException {
    analyticExpression = (AnalyticExpression) CCJSqlParserUtil.parseExpression("SUM(value) OVER ()");
  }

  /**
   * Ensure resolvePositiveOffset returns defaults and rejects invalid values.
   */
  @Test
  void resolvePositiveOffsetValidation() {
    assertEquals(1L, callResolvePositiveOffset(null), "Null offsets should default to one");
    assertEquals(3L, callResolvePositiveOffset(new BigDecimal("3")), "Positive numeric offsets should pass validation");

    IllegalArgumentException nonNumeric = assertThrows(IllegalArgumentException.class,
        () -> callResolvePositiveOffset("not-a-number"));
    assertTrue(nonNumeric.getMessage().contains("non-numeric value"), "Non-numeric offsets should be rejected");

    IllegalArgumentException fractional = assertThrows(IllegalArgumentException.class,
        () -> callResolvePositiveOffset(new BigDecimal("2.5")));
    assertTrue(fractional.getMessage().contains("integer value"), "Fractional offsets should be rejected");

    IllegalArgumentException negative = assertThrows(IllegalArgumentException.class,
        () -> callResolvePositiveOffset(-1));
    assertTrue(negative.getMessage().contains("strictly positive"), "Offsets must be strictly positive");
  }

  /**
   * Verify resolvePositiveBucketCount enforces positive integers.
   */
  @Test
  void resolvePositiveBucketCountValidation() {
    assertEquals(4L, callResolvePositiveBucketCount(4), "Positive integer buckets should be accepted");

    IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
        () -> callResolvePositiveBucketCount(null));
    assertTrue(missing.getMessage().contains("must evaluate to a positive integer"), "Null buckets should be rejected");

    IllegalArgumentException nonNumeric = assertThrows(IllegalArgumentException.class,
        () -> callResolvePositiveBucketCount("bucket"));
    assertTrue(nonNumeric.getMessage().contains("non-numeric"), "Non-numeric buckets should be rejected");

    IllegalArgumentException zero = assertThrows(IllegalArgumentException.class,
        () -> callResolvePositiveBucketCount(0));
    assertTrue(zero.getMessage().contains("strictly positive"), "Zero buckets should be rejected");
  }

  /**
   * Validate nthValueFromRelativeFrame behavior for a variety of frame bounds.
   */
  @Test
  void nthValueFromRelativeFrameBounds() {
    List<Object> values = List.of("first", "second", "third");

    assertEquals("second", nthValueFromRelativeFrame(values, 2L, 0, 0, 3),
        "Nth within frame should return the matching value");
    assertEquals("third", nthValueFromRelativeFrame(values, 1L, 1, 1, 3), "Frame offsets must be honored");
    assertNull(nthValueFromRelativeFrame(values, 0L, 0, 0, 3), "Non-positive positions should yield null");
    assertNull(nthValueFromRelativeFrame(values, 5L, 0, 0, 3), "Positions beyond the frame length should yield null");
    assertNull(nthValueFromRelativeFrame(values, (long) Integer.MAX_VALUE + 1L, 0, 0, 3),
        "Positions exceeding integer bounds should be ignored");
    assertNull(nthValueFromRelativeFrame(values, 1L, 0, 2, 2), "Empty frames must result in null values");
  }

  /**
   * Ensure safeLongToInt enforces bounds.
   */
  @Test
  void safeLongToIntValidation() {
    assertEquals(10, safeLongToInt(10L), "Within range values should be returned unchanged");

    IllegalArgumentException overflow = assertThrows(IllegalArgumentException.class,
        () -> safeLongToInt((long) Integer.MAX_VALUE + 10L));
    assertTrue(overflow.getMessage().contains("exceeds supported range"), "Overflow should be reported");
  }

  /**
   * Ensure resolveNthPosition enforces numeric and positive values.
   */
  @Test
  void resolveNthPositionValidation() {
    assertEquals(2L, callResolveNthPosition(2), "Valid integer positions should be accepted");

    IllegalArgumentException nonNumeric = assertThrows(IllegalArgumentException.class,
        () -> callResolveNthPosition("nth"));
    assertTrue(nonNumeric.getMessage().contains("non-numeric"), "Non-numeric positions should be rejected");

    IllegalArgumentException fractional = assertThrows(IllegalArgumentException.class,
        () -> callResolveNthPosition(new BigDecimal("3.5")));
    assertTrue(fractional.getMessage().contains("integer value"), "Fractional positions should be rejected");

    IllegalArgumentException nonPositive = assertThrows(IllegalArgumentException.class,
        () -> callResolveNthPosition(0));
    assertTrue(nonPositive.getMessage().contains("strictly positive"), "Positions must be strictly positive");
  }

  /**
   * Cover compareValues branches for binary, temporal and interval values.
   */
  @Test
  void compareValuesCoversSpecializedTypes() {
    ByteBuffer leftBuffer = ByteBuffer.wrap(new byte[]{
        1, 2
    });
    ByteBuffer rightBuffer = ByteBuffer.wrap(new byte[]{
        1, 3
    });
    assertTrue(compareValues(leftBuffer, rightBuffer) < 0, "ByteBuffer comparison should be lexicographic");

    Date earlierDate = new Date(0L);
    Date laterDate = new Date(1_000L);
    assertTrue(compareValues(earlierDate, laterDate) < 0, "Earlier dates should sort before later ones");

    Timestamp earlierTimestamp = new Timestamp(0L);
    Timestamp laterTimestamp = new Timestamp(5_000L);
    assertTrue(compareValues(earlierTimestamp, laterTimestamp) < 0,
        "Timestamps should be compared using their epoch millis");

    TemporalInterval shortInterval = TemporalInterval.of(Period.ofDays(1), Duration.ZERO);
    TemporalInterval longInterval = TemporalInterval.of(Period.ofDays(2), Duration.ZERO);
    assertTrue(compareValues(shortInterval, longInterval) < 0,
        "TemporalInterval comparison should defer to compareTo implementations");
  }

  private long callResolvePositiveOffset(Object value) {
    return resolvePositiveOffset(value, analyticExpression, "LAG");
  }

  private long callResolvePositiveBucketCount(Object value) {
    return resolvePositiveBucketCount(value, analyticExpression);
  }

  private long callResolveNthPosition(Object value) {
    return resolveNthPosition(value, analyticExpression);
  }
}
