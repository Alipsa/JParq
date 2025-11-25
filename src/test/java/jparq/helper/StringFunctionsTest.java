package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import java.text.Normalizer;
import java.util.List;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.StringFunctions;
import se.alipsa.jparq.helper.StringFunctions.TrimMode;

/** Unit tests for {@link StringFunctions}. */
class StringFunctionsTest {

  @Test
  void handlesSubstringAndPositionEdgeCases() {
    assertEquals(1, StringFunctions.position("", "abc"));
    assertEquals("", StringFunctions.substring("abc", 10, null));
    assertNull(StringFunctions.substring(null, 1, 1));
    assertEquals("ab", StringFunctions.left("abc", 2));
    assertEquals("bc", StringFunctions.right("abc", 2));
  }

  @Test
  void trimsAndPadsUsingCustomCharacters() {
    assertEquals("center", StringFunctions.trim("***center***", "*", TrimMode.BOTH));
    assertEquals("!!!hi", StringFunctions.lpad("hi", 5, "!"));
    assertEquals("hi????", StringFunctions.rpad("hi", 6, "?"));
  }

  @Test
  void overlaysAndReplacesSegments() {
    assertEquals("abXYZef", StringFunctions.overlay("abcdef", "XYZ", 3, 2));
    assertEquals("abc", StringFunctions.replace("abc", "", "x"));
    assertNull(StringFunctions.replace(null, "a", "b"));
  }

  @Test
  void evaluatesLikeAndSimilarPatterns() {
    assertTrue(StringFunctions.like("Hello", "H%", true, null));
    assertFalse(StringFunctions.similarTo("cat", "c(d|e)t", null));
    assertThrows(IllegalArgumentException.class, () -> StringFunctions.like("a", "\\", false, '\\'));
    assertThrows(IllegalArgumentException.class, () -> StringFunctions.similarTo("a", "\\", '\\'));
  }

  @Test
  void convertsUnicodeAndNormalizesText() {
    assertEquals("AB", StringFunctions.charFromCodes(List.of(65, 66)));
    assertNull(StringFunctions.charFromCodes(List.of()));
    assertNull(StringFunctions.unicode(null));
    assertEquals(0, StringFunctions.unicode(""));

    String combined = "ê";
    String decomposed = Normalizer.normalize(combined, Normalizer.Form.NFD);
    assertEquals(decomposed, StringFunctions.normalize(combined, "NFD"));
  }
}
