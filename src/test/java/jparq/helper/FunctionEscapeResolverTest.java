package jparq.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.helper.FunctionEscapeResolver;

/** Unit tests for {@link FunctionEscapeResolver}. */
class FunctionEscapeResolverTest {

  /**
   * Leaves escape-like text inside SQL quotes unchanged.
   *
   * @param quoted
   *          a quoted literal or identifier
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'{fn UCASE(''a'')}'", "$${fn UCASE('a')}$$", "\"{fn UCASE('a')}\"", "`{fn UCASE('a')}`", "'it''s {fn NOW()}'",
      "\"a\"\"{fn NOW()}\"", "`a``{fn NOW()}`", "$$'\"` {fn NOW()}$$"
  })
  void preservesQuotedEscapesAlongsideRealEscapes(String quoted) {
    String sql = "SELECT {fn LCASE('A')}, " + quoted + ", {fn UCASE('b')}";
    assertEquals("SELECT LOWER('A'), " + quoted + ", UPPER('b')",
        FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  /** Keeps quoted braces and escape text inside genuine function arguments. */
  @Test
  void preservesQuotedFunctionArguments() {
    String sql = "SELECT {fn LCASE('{fn UCASE(''A'')}')}, {fn UCASE($$a}b$$)}";
    assertEquals("SELECT LOWER('{fn UCASE(''A'')}'), UPPER($$a}b$$)",
        FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  /** Ignores punctuation in quoted arguments when finding a CONVERT separator. */
  @Test
  void convertEscapePreservesQuotedPunctuation() {
    assertEquals("SELECT CAST('a,b){}' AS VARCHAR)",
        FunctionEscapeResolver.resolveJdbcFunctionEscapes("SELECT {fn CONVERT('a,b){}', VARCHAR)}"));
    assertEquals("SELECT CAST($$a,b){}$$ AS VARCHAR)",
        FunctionEscapeResolver.resolveJdbcFunctionEscapes("SELECT {fn CONVERT($$a,b){}$$, VARCHAR)}"));
  }

  @Test
  void resolvesKnownNoArgFunctionsWithoutParentheses() {
    String curDate = FunctionEscapeResolver.resolveJdbcFunctionEscapes("SELECT {fn CURDATE()}").trim();
    String curTime = FunctionEscapeResolver.resolveJdbcFunctionEscapes("SELECT {fn curtime()}").trim();
    String now = FunctionEscapeResolver.resolveJdbcFunctionEscapes("SELECT {fn NOW()}").trim();
    assertEquals("CURRENT_DATE", curDate.substring("SELECT ".length()));
    assertEquals("CURRENT_TIME", curTime.substring("SELECT ".length()));
    assertEquals("CURRENT_TIMESTAMP", now.substring("SELECT ".length()));
  }

  @Test
  void resolvesKnownFunctionsWithArguments() {
    String sql = "SELECT {fn DAYOFWEEK(col)}";
    assertEquals("SELECT DAYOFWEEK(col)", FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
    String lcase = "select {fn LCASE(col)}";
    assertEquals("select LOWER(col)", FunctionEscapeResolver.resolveJdbcFunctionEscapes(lcase));
    String charFn = "SELECT {fn CHAR(65)}";
    assertEquals("SELECT CHAR(65)", FunctionEscapeResolver.resolveJdbcFunctionEscapes(charFn));
  }

  @Test
  void resolvesNumericFunctionsAndAliases() {
    String canonical = "SELECT {fn ABS(-10.5)}, {fn CEILING(4.2)}, {fn RAND(5)}, {fn TRUNCATE(4.567, 2)}, {fn PI()}";
    assertEquals("SELECT ABS(-10.5), CEILING(4.2), RAND(5), TRUNCATE(4.567, 2), PI()",
        FunctionEscapeResolver.resolveJdbcFunctionEscapes(canonical));

    String aliases = "SELECT {fn ceil(1.2)}, {fn pow(2,3)}, {fn random(7)}, {fn trunc(9.99,1)}";
    assertEquals("SELECT CEILING(1.2), POWER(2,3), RAND(7), TRUNCATE(9.99,1)",
        FunctionEscapeResolver.resolveJdbcFunctionEscapes(aliases));
  }

  @Test
  void resolvesSystemFunctions() {
    String sql = "SELECT {fn DATABASE}, {fn IFNULL(col, 'x')}, {fn USER}";
    assertEquals("SELECT DATABASE(), COALESCE(col, 'x'), USER()",
        FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void leavesUnknownFunctionsIntact() {
    String sql = "SELECT {fn CUSTOM_FN(1, 2)}";
    assertEquals("SELECT CUSTOM_FN(1, 2)", FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void returnsInputWhenNullOrEmpty() {
    assertNull(FunctionEscapeResolver.resolveJdbcFunctionEscapes(null));
    assertEquals("", FunctionEscapeResolver.resolveJdbcFunctionEscapes(""));
  }

  @Test
  void handlesCaseInsensitivityAndMixedSpacing() {
    String sql = "select {  Fn   dayofyear( col ) }";
    assertEquals("select DAYOFYEAR( col )", FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void ignoresStringsWithoutEscapes() {
    String sql = "SELECT 1";
    assertEquals(sql, FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void convertEscapeWithUsingClauseIsNotRewritten() {
    // CONVERT with USING clause should not be rewritten to CAST
    String sql = "SELECT {fn CONVERT('text', USING UTF8)}";
    assertEquals("SELECT CONVERT('text', USING UTF8)", FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void convertEscapeWithTypeCastIsRewrittenToCast() {
    // CONVERT with type (not USING) should be rewritten to CAST
    String sql = "SELECT {fn CONVERT(value, INTEGER)}";
    assertEquals("SELECT CAST(value AS INTEGER)", FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void convertEscapeWithNestedFunctionCall() {
    // Nested function calls with parentheses should be handled by
    // firstTopLevelComma
    String sql = "SELECT {fn CONVERT(CONCAT(a, b), VARCHAR)}";
    assertEquals("SELECT CAST(CONCAT(a, b) AS VARCHAR)", FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void convertEscapeWithMultipleCommasInNestedFunction() {
    // Edge case: nested function with multiple commas should find the top-level
    // comma
    String sql = "SELECT {fn CONVERT(SUBSTRING(text, 1, 5), VARCHAR)}";
    assertEquals("SELECT CAST(SUBSTRING(text, 1, 5) AS VARCHAR)",
        FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void convertEscapeWithDeeplyNestedParentheses() {
    // Edge case: deeply nested parentheses
    String sql = "SELECT {fn CONVERT(COALESCE(NULL, CONCAT('a', 'b'), 'default'), VARCHAR)}";
    assertEquals("SELECT CAST(COALESCE(NULL, CONCAT('a', 'b'), 'default') AS VARCHAR)",
        FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void convertEscapeWithNoCommaPassesThrough() {
    // Edge case: CONVERT with no comma (malformed) should pass through unchanged
    String sql = "SELECT {fn CONVERT(value)}";
    assertEquals("SELECT CONVERT(value)", FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }

  @Test
  void convertEscapeWithEmptyArgsPassesThrough() {
    // Edge case: CONVERT with empty args
    String sql = "SELECT {fn CONVERT()}";
    assertEquals("SELECT CONVERT()", FunctionEscapeResolver.resolveJdbcFunctionEscapes(sql));
  }
}
