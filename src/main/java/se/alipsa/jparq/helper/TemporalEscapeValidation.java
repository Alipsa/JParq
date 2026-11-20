package se.alipsa.jparq.helper;

import java.util.Locale;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/**
 * Rejects JDBC temporal escape literals whose text fails strict conversion.
 *
 * <p>
 * JSqlParser builds {@code DateValue}, {@code TimeValue}, and
 * {@code TimestampValue} nodes with the lenient {@code java.sql}
 * {@code valueOf} parsers, which normalize out-of-range fields such as
 * {@code 2024-02-30} or {@code 25:00:00} instead of rejecting them. The parser
 * also discards the original text of date and time escapes, so this validator
 * re-checks the statement text and applies the same strict rules as the typed
 * {@code DATE}, {@code TIME}, and {@code TIMESTAMP} literals.
 */
public final class TemporalEscapeValidation {

  private TemporalEscapeValidation() {
  }

  /**
   * Validates every JDBC temporal escape literal outside quoted regions in the
   * statement text. List literals are descended into because their elements are
   * expressions that may carry escape literals. The input must have SQL comments
   * removed before scanning, matching the comment-free text the parser receives.
   *
   * @param sql
   *          statement text without SQL comments
   * @throws IllegalArgumentException
   *           if an escape literal carries an invalid temporal value, with a
   *           cause carrying SQLState 22007 for an invalid format or 22008 for
   *           out-of-range fields, or if a quoted region is unterminated
   */
  public static void validate(String sql) {
    if (sql == null || sql.indexOf('{') < 0) {
      return;
    }
    int index = 0;
    int length = sql.length();
    while (index < length) {
      index = nextRegion(sql, index);
    }
  }

  /**
   * Advances past the lexical region starting at the offset.
   *
   * @param sql
   *          statement text
   * @param index
   *          offset of the region start
   * @return offset of the first character after the region
   */
  private static int nextRegion(String sql, int index) {
    char current = sql.charAt(index);
    if (current == '\'' || current == '"' || current == '`' || current == '$') {
      int end = SqlTextScanner.quotedRegionEnd(sql, index);
      return end >= 0 ? end : index + 1;
    }
    if (current == '{') {
      int end = validateTemporalEscape(sql, index);
      return end >= 0 ? end : index + 1;
    }
    return index + 1;
  }

  /**
   * Validates a JDBC temporal escape literal starting at the opening brace,
   * re-checking the literal text with the strict temporal conversion helpers.
   *
   * @param sql
   *          statement text
   * @param index
   *          offset of the opening brace
   * @return offset of the first character after the literal, or -1 when the brace
   *         does not start a temporal escape
   * @throws IllegalArgumentException
   *           if the literal text fails strict temporal conversion
   */
  private static int validateTemporalEscape(String sql, int index) {
    int tagStart = index + 1;
    int tagEnd = tagStart;
    int length = sql.length();
    while (tagEnd < length && isTagCharacter(sql.charAt(tagEnd))) {
      tagEnd++;
    }
    String tag = sql.substring(tagStart, tagEnd).toUpperCase(Locale.ROOT);
    if (!tag.equals("D") && !tag.equals("T") && !tag.equals("TS")) {
      return -1;
    }
    int literalStart = skipWhitespace(sql, tagEnd);
    if (literalStart >= length || sql.charAt(literalStart) != '\'') {
      return -1;
    }
    int literalEnd = SqlTextScanner.quotedRegionEnd(sql, literalStart);
    int closing = skipWhitespace(sql, literalEnd);
    if (closing >= length || sql.charAt(closing) != '}') {
      return -1;
    }
    String text = sql.substring(literalStart + 1, literalEnd - 1).replace("''", "'");
    if (tag.equals("D")) {
      DateTimeFunctions.toDate(text);
    } else if (tag.equals("T")) {
      DateTimeFunctions.toTime(text);
    } else {
      DateTimeFunctions.toTimestamp(text);
    }
    return closing + 1;
  }

  /**
   * Identifies characters that can continue the tag of a temporal escape.
   *
   * @param c
   *          candidate character
   * @return whether the character continues an identifier run
   */
  private static boolean isTagCharacter(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '@' || c == '#';
  }

  /**
   * Skips Java whitespace characters between the escape tag, its literal, and the
   * closing brace.
   *
   * @param sql
   *          statement text
   * @param index
   *          offset to start from
   * @return offset of the first non-whitespace character
   */
  private static int skipWhitespace(String sql, int index) {
    int length = sql.length();
    int offset = index;
    while (offset < length && Character.isWhitespace(sql.charAt(offset))) {
      offset++;
    }
    return offset;
  }

}
