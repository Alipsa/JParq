package jparq.window;

import static se.alipsa.jparq.engine.window.WindowFunctions.compareBinary;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for the private binary comparison helper used by window ordering.
 */
class WindowFunctionsBinaryTest {

  @Test
  void compareBinaryNullHandling() {
    Assertions.assertEquals(0, compareBinary(null, null), "Both null should compare as equal");
    Assertions.assertTrue(compareBinary(null, new byte[]{
        1
    }) < 0, "Null should be treated as less than non-null");
    Assertions.assertTrue(compareBinary(new byte[]{
        1
    }, null) > 0, "Non-null should be greater than null");
  }

  @Test
  void compareBinaryLexicographicOrdering() {
    Assertions.assertTrue(compareBinary(new byte[]{
        1, 2, 3
    }, new byte[]{
        1, 2, 4
    }) < 0, "Comparison should defer to the first differing byte");
    Assertions.assertTrue(compareBinary(new byte[]{
        1, 2, 4
    }, new byte[]{
        1, 2, 3
    }) > 0, "Comparison should defer to the first differing byte");
  }

  @Test
  void compareBinaryLengthWhenPrefixesMatch() {
    Assertions.assertTrue(compareBinary(new byte[]{
        1, 2
    }, new byte[]{
        1, 2, 0
    }) < 0, "Shorter prefix should be ordered before longer value when content matches");
    Assertions.assertTrue(compareBinary(new byte[]{
        1, 2, 0
    }, new byte[]{
        1, 2
    }) > 0, "Longer value should be ordered after matching shorter prefix");
  }
}
