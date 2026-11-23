package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.OrderingUtil;

class OrderingUtilTest {

  /**
   * Accepts every representable position at or above 1, with and without an upper
   * bound, including the boundary values 1 and {@link Integer#MAX_VALUE}.
   */
  @Test
  void parsesOrdinalBoundaries() {
    assertEquals(1, OrderingUtil.position("1", 1));
    assertEquals(5, OrderingUtil.position("5"));
    assertEquals(Integer.MAX_VALUE, OrderingUtil.position("2147483647"));
    assertEquals(5, OrderingUtil.position("5", 5));
  }

  /**
   * Rejects zero and positions above the select list with the syntax
   * classification naming the invalid position.
   */
  @Test
  void rejectsPositionsOutsideTheSelectList() {
    assertOrdinalFailure("2", 1, "2");
    assertOrdinalFailure("0", 1, "0");
    assertOrdinalFailure("99", 5, "99");
    assertOrdinalFailure("0", "0");
  }

  /**
   * Rejects values beyond the representable range, with and without an upper
   * bound; a deferred bound never defers representability.
   */
  @Test
  void rejectsLiteralsBeyondTheRepresentableRange() {
    assertOrdinalFailure("2147483648", "2147483648");
    assertOrdinalFailure("99999999999999999999", 5, "99999999999999999999");
  }

  /**
   * Names non-canonical integer literals exactly as written in the query, with
   * and without an upper bound.
   */
  @Test
  void reportsLiteralsExactlyAsWritten() {
    assertOrdinalFailure("099", 5, "099");
    assertOrdinalFailure("00", "00");
  }

  /**
   * Recognizes unquoted digit runs as positional references and treats null,
   * empty, mixed, signed, and quoted values as column names.
   */
  @Test
  void detectsPositionalReferences() {
    assertTrue(OrderingUtil.isPosition("1"));
    assertTrue(OrderingUtil.isPosition("99"));
    assertTrue(OrderingUtil.isPosition("0"));
    assertTrue(OrderingUtil.isPosition("099"));
    assertFalse(OrderingUtil.isPosition(null));
    assertFalse(OrderingUtil.isPosition(""));
    assertFalse(OrderingUtil.isPosition("12a"));
    assertFalse(OrderingUtil.isPosition("a1"));
    assertFalse(OrderingUtil.isPosition("-1"));
    assertFalse(OrderingUtil.isPosition("\"1\""));
  }

  /**
   * Asserts that an unbounded positional reference fails with the syntax
   * classification naming the literal as written.
   *
   * @param literal
   *          the positional reference to parse
   * @param expected
   *          literal expected in the message
   */
  private static void assertOrdinalFailure(String literal, String expected) {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> OrderingUtil.position(literal));
    assertEquals("ORDER BY position is outside the select list: " + expected, failure.getMessage());
    assertEquals("42000", stateOf(failure));
  }

  /**
   * Asserts that a bounded positional reference fails with the syntax
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
