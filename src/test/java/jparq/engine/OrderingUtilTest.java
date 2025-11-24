package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.sf.jsqlparser.statement.select.OrderByElement;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.OrderingUtil;

class OrderingUtilTest {

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
