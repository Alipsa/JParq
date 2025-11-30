package jparq.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.function.ArrayFunctions;
import se.alipsa.jparq.engine.function.SystemFunctions;

/** Unit tests for {@link SystemFunctions} and related helpers. */
class SystemFunctionsTest {

  @Test
  void returnsSystemInformation() {
    assertEquals("JParq", SystemFunctions.database());
    assertEquals(System.getProperty("user.name"), SystemFunctions.user());
  }

  @Test
  void ifNullReturnsFirstNonNull() {
    assertEquals("first", SystemFunctions.ifNull("first", "second"));
    assertEquals("fallback", SystemFunctions.ifNull(null, "fallback"));
    assertNull(SystemFunctions.ifNull(null, null));
  }

  @Test
  void homogenizeArrayValuesPromotesNumericTypes() {
    List<Object> values = java.util.Arrays.asList(1, 2.5d, null);
    List<Object> homogenized = ArrayFunctions.homogenizeArrayValues(values);
    assertEquals(java.util.Arrays.asList(1.0, 2.5, null), homogenized);
  }
}
