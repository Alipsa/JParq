package jparq.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.FunctionEscapeResolver;

/** Unit tests for {@link FunctionEscapeResolver}. */
class FunctionEscapeResolverTest {

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
}
