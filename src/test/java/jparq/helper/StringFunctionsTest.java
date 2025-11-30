package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import java.text.Normalizer;
import java.util.List;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.function.FunctionArguments;
import se.alipsa.jparq.engine.function.StringFunctions;
import se.alipsa.jparq.engine.function.StringFunctions.TrimMode;
import se.alipsa.jparq.helper.LiteralConverter;

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
  void coalesceReturnsFirstNonNull() throws Exception {
    Function func = (Function) CCJSqlParserUtil.parseExpression("coalesce(null, 'x', 'y')");
    FunctionArguments args = new FunctionArguments(func, null, (expr, record) -> LiteralConverter.toLiteral(expr));
    assertEquals("x", StringFunctions.coalesce(args));
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
