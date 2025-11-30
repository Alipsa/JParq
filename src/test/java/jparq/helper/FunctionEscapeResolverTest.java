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
