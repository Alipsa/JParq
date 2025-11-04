package jparq.window;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.window.WindowFunctions;

/**
 * Tests for the private binary comparison helper used by window ordering.
 */
class WindowFunctionsBinaryTest {

  private static Method compareBinaryMethod;

  @BeforeAll
  static void locateMethod() throws NoSuchMethodException {
    compareBinaryMethod = WindowFunctions.class.getDeclaredMethod("compareBinary", byte[].class, byte[].class);
    compareBinaryMethod.setAccessible(true);
  }

  @Test
  void compareBinaryNullHandling() {
    Assertions.assertEquals(0, invokeCompare(null, null), "Both null should compare as equal");
    Assertions.assertTrue(invokeCompare(null, new byte[]{
        1
    }) < 0, "Null should be treated as less than non-null");
    Assertions.assertTrue(invokeCompare(new byte[]{
        1
    }, null) > 0, "Non-null should be greater than null");
  }

  @Test
  void compareBinaryLexicographicOrdering() {
    Assertions.assertTrue(invokeCompare(new byte[]{
        1, 2, 3
    }, new byte[]{
        1, 2, 4
    }) < 0, "Comparison should defer to the first differing byte");
    Assertions.assertTrue(invokeCompare(new byte[]{
        1, 2, 4
    }, new byte[]{
        1, 2, 3
    }) > 0, "Comparison should defer to the first differing byte");
  }

  @Test
  void compareBinaryLengthWhenPrefixesMatch() {
    Assertions.assertTrue(invokeCompare(new byte[]{
        1, 2
    }, new byte[]{
        1, 2, 0
    }) < 0, "Shorter prefix should be ordered before longer value when content matches");
    Assertions.assertTrue(invokeCompare(new byte[]{
        1, 2, 0
    }, new byte[]{
        1, 2
    }) > 0, "Longer value should be ordered after matching shorter prefix");
  }

  private static int invokeCompare(byte[] left, byte[] right) {
    try {
      return (int) compareBinaryMethod.invoke(null, left, right);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Failed to invoke compareBinary", e);
    }
  }
}
