package jparq.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.helper.SqlTextScanner;

/** Tests the shared quote boundaries used by SQL preprocessing. */
class SqlTextScannerTest {

  /**
   * Scans doubled quote delimiters and opaque dollar-quoted bodies.
   *
   * @param quoted
   *          the complete quoted region
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "''", "'a''b'", "\"a\"\"b\"", "`a``b`", "$$$$", "$$'\"`--/*$$"
  })
  void findsQuotedEndsAndMasksOnlyQuotes(String quoted) {
    String sql = "SELECT " + quoted + ", x";
    assertEquals(7 + quoted.length(), SqlTextScanner.quotedRegionEnd(sql, 7));
    assertEquals("SELECT " + " ".repeat(quoted.length()) + ", x", SqlTextScanner.maskQuotedRegions(sql));
  }

  /**
   * Rejects unmatched quotes consistently for both scanners.
   *
   * @param quoted
   *          an unterminated quoted region
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'a''", "\"a\"\"", "`a``", "$$unfinished"
  })
  void rejectsUnterminatedQuotes(String quoted) {
    assertThrows(IllegalArgumentException.class, () -> SqlTextScanner.quotedRegionEnd(quoted, 0));
    assertThrows(IllegalArgumentException.class, () -> SqlTextScanner.maskQuotedRegions(quoted));
  }

  /**
   * Keeps dollar signs in identifiers and tagged openers as ordinary SQL text.
   */
  @Test
  void preservesNonQuoteText() {
    String sql = "SELECT a$$b, é$$b, $tag$text$tag$, $1";
    assertEquals(-1, SqlTextScanner.quotedRegionEnd(sql, 0));
    assertEquals(sql, SqlTextScanner.maskQuotedRegions(sql));
  }
}
