package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.OrderingUtil;

class OrderingUtilTest {

  @Test
  void parsesOrdinalBoundaries() {
    assertEquals(1, OrderingUtil.position("1", 1));
    assertEquals(5, OrderingUtil.position("5", -1));
    assertEquals(Integer.MAX_VALUE, OrderingUtil.position(Integer.MAX_VALUE, Integer.MAX_VALUE));
    assertEquals(5, OrderingUtil.position(5, 5));
    assertEquals(1, OrderingUtil.position(1, 1));
  }

  @Test
  void rejectsPositionsOutsideTheSelectList() {
    assertOrdinalFailure("2", 1, "2");
    assertOrdinalFailure("0", 1, "0");
    assertOrdinalFailure("99", 5, "99");
    assertOrdinalFailure("0", -1, "0");
    IllegalArgumentException zero = assertThrows(IllegalArgumentException.class, () -> OrderingUtil.position(0, 5));
    assertEquals("ORDER BY position is outside the select list: 0", zero.getMessage());
    assertEquals("42000", stateOf(zero));
  }

  @Test
  void rejectsLiteralsBeyondTheRepresentableRange() {
    // A negative maximum defers the upper bound but never the representability.
    assertOrdinalFailure("2147483648", -1, "2147483648");
    assertOrdinalFailure("99999999999999999999", 5, "99999999999999999999");
  }

  @Test
  void reportsLiteralsExactlyAsWritten() {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> OrderingUtil.position("099", 5));
    assertEquals("ORDER BY position is outside the select list: 099", failure.getMessage());
    assertEquals("42000", stateOf(failure));
  }

  /**
   * Asserts that a string-based positional reference fails with the syntax
   * classification naming the literal as written.
   *
   * @param literal
   *          the positional reference to parse
   * @param maximum
   *          the largest acceptable position
   * @param expected
   *          literal expected in the message
   */
  private static void assertOrdinalFailure(String literal, int maximum, String expected) {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> OrderingUtil.position(literal, maximum));
    assertEquals("ORDER BY position is outside the select list: " + expected, failure.getMessage());
    assertEquals("42000", stateOf(failure));
  }

  /**
   * Reads the SQLState carried by the cause of an engine classification failure.
   *
   * @param failure
   *          the engine failure to inspect
   * @return the SQLState of its {@link SQLException} cause
   */
  private static String stateOf(IllegalArgumentException failure) {
    Throwable cause = failure.getCause();
    assertTrue(cause instanceof SQLException);
    return ((SQLException) cause).getSQLState();
  }

  @Test
  void honorsExplicitNullsFirstAndLast() {
    int nullsFirst = OrderingUtil.compareNulls(null, "value", true, OrderByElement.NullOrdering.NULLS_FIRST);
    assertTrue(nullsFirst < 0);
    int nullsLast = OrderingUtil.compareNulls(null, "value", true, OrderByElement.NullOrdering.NULLS_LAST);
    assertTrue(nullsLast > 0);
  }

  @Test
  void appliesDefaultNullPlacementForAscAndDesc() {
    int ascNullLast = OrderingUtil.compareNulls(null, "value", true, null);
    assertTrue(ascNullLast > 0);
    int descNullFirst = OrderingUtil.compareNulls(null, "value", false, null);
    assertTrue(descNullFirst < 0);
  }

  @Test
  void ignoresNonNullValues() {
    int result = OrderingUtil.compareNulls("left", "right", true, null);
    assertEquals(0, result);
  }
}
