package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import java.text.Normalizer;
import java.util.List;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.StringExpressions;
import se.alipsa.jparq.helper.StringExpressions.TrimMode;

/** Unit tests for {@link StringExpressions}. */
class StringExpressionsTest {

  @Test
  void handlesSubstringAndPositionEdgeCases() {
    assertEquals(1, StringExpressions.position("", "abc"));
    assertEquals("", StringExpressions.substring("abc", 10, null));
    assertNull(StringExpressions.substring(null, 1, 1));
    assertEquals("ab", StringExpressions.left("abc", 2));
    assertEquals("bc", StringExpressions.right("abc", 2));
  }

  @Test
  void trimsAndPadsUsingCustomCharacters() {
    assertEquals("center", StringExpressions.trim("***center***", "*", TrimMode.BOTH));
    assertEquals("!!!hi", StringExpressions.lpad("hi", 5, "!"));
    assertEquals("hi????", StringExpressions.rpad("hi", 6, "?"));
  }

  @Test
  void overlaysAndReplacesSegments() {
    assertEquals("abXYZef", StringExpressions.overlay("abcdef", "XYZ", 3, 2));
    assertEquals("abc", StringExpressions.replace("abc", "", "x"));
    assertNull(StringExpressions.replace(null, "a", "b"));
  }

  @Test
  void evaluatesLikeAndSimilarPatterns() {
    assertTrue(StringExpressions.like("Hello", "H%", true, null));
    assertFalse(StringExpressions.similarTo("cat", "c(d|e)t", null));
    assertThrows(IllegalArgumentException.class, () -> StringExpressions.like("a", "\\", false, '\\'));
    assertThrows(IllegalArgumentException.class, () -> StringExpressions.similarTo("a", "\\", '\\'));
  }

  @Test
  void convertsUnicodeAndNormalizesText() {
    assertEquals("AB", StringExpressions.charFromCodes(List.of(65, 66)));
    assertNull(StringExpressions.charFromCodes(List.of()));
    assertNull(StringExpressions.unicode(null));
    assertEquals(0, StringExpressions.unicode(""));

    String combined = "ê";
    String decomposed = Normalizer.normalize(combined, Normalizer.Form.NFD);
    assertEquals(decomposed, StringExpressions.normalize(combined, "NFD"));
  }
}
