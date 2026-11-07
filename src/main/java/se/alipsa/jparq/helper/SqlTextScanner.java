package se.alipsa.jparq.helper;

/** Shared SQL quote scanning for preprocessing before parsing. */
public final class SqlTextScanner {

  /** Prevents instantiation of this utility class. */
  private SqlTextScanner() {
  }

  /**
   * Finds a quoted region starting outside any other quote or comment. Single,
   * double, and backtick quotes support doubled delimiters; dollar quotes follow
   * {@link #dollarQuotedLiteralEnd(String, int)}.
   *
   * @param sql
   *          the SQL text to inspect
   * @param offset
   *          a valid character index in the SQL text
   * @return the exclusive end, or -1 if no quoted region starts at the offset
   * @throws IllegalArgumentException
   *           if an opening quote has no closing delimiter
   */
  public static int quotedRegionEnd(String sql, int offset) {
    char delimiter = sql.charAt(offset);
    if (delimiter == '$') {
      return dollarQuotedLiteralEnd(sql, offset);
    }
    if (delimiter != '\'' && delimiter != '"' && delimiter != '`') {
      return -1;
    }
    for (int i = offset + 1; i < sql.length(); i++) {
      if (sql.charAt(i) == delimiter) {
        if (i + 1 < sql.length() && sql.charAt(i + 1) == delimiter) {
          i++;
        } else {
          return i + 1;
        }
      }
    }
    throw new IllegalArgumentException("Unterminated quoted literal or identifier in SQL");
  }

  /**
   * Finds the exclusive end of an untagged dollar-quoted literal outside quotes
   * and comments. Dollar signs following an identifier character or another
   * dollar sign do not open literals. Tagged dollar quotes remain identifier
   * text.
   *
   * @param sql
   *          the SQL text to inspect
   * @param offset
   *          a valid character index in the SQL text
   * @return the exclusive end, or -1 if no literal starts at the offset
   * @throws IllegalArgumentException
   *           if an opening delimiter has no closing delimiter
   */
  public static int dollarQuotedLiteralEnd(String sql, int offset) {
    if (!sql.startsWith("$$", offset) || offset > 0
        && (sql.charAt(offset - 1) == '$' || Character.isUnicodeIdentifierPart(sql.codePointBefore(offset)))) {
      return -1;
    }
    int closing = sql.indexOf("$$", offset + 2);
    if (closing < 0) {
      throw new IllegalArgumentException("Unterminated dollar-quoted literal in SQL");
    }
    return closing + 2;
  }

  /**
   * Replaces quoted regions with NUL characters while retaining all character
   * offsets. The mask is neither whitespace nor SQL punctuation, so syntax
   * matching cannot consume quoted text as spacing or delimiters. The input must
   * have SQL comments removed before scanning.
   *
   * @param sql
   *          SQL text without comments
   * @return SQL text with quoted regions masked for syntax matching
   * @throws IllegalArgumentException
   *           if a quoted region is unterminated
   */
  public static String maskQuotedRegions(String sql) {
    StringBuilder masked = new StringBuilder(sql);
    for (int i = 0; i < sql.length(); i++) {
      int end = quotedRegionEnd(sql, i);
      if (end >= 0) {
        for (int j = i; j < end; j++) {
          masked.setCharAt(j, '\0');
        }
        i = end - 1;
      }
    }
    return masked.toString();
  }
}
