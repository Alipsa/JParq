package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import se.alipsa.jparq.engine.NumericCastTarget;
import se.alipsa.jparq.engine.NumericCasts;

/** Verifies the shared numeric conversion helper's explicit null contract. */
class NumericCastsTest {
  /**
   * Preserves null values through both numeric conversion entry points.
   *
   * @param target
   *          the requested numeric target
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void preservesNull(NumericCastTarget target) {
    assertNull(NumericCasts.convert(null, target, target.name()));
    assertNull(NumericCasts.toBigDecimal(null));
  }
}
