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
 * re-checks the raw statement text before parsing and applies the same strict
 * rules as the typed {@code DATE}, {@code TIME}, and {@code TIMESTAMP}
 * literals.
 */
public final class TemporalEscapeValidation {

  private TemporalEscapeValidation() {
  }

  /**
   * Validates every JDBC temporal escape literal outside quoted regions and
   * comments in the statement text. List literals are descended into because
   * their elements are expressions that may carry escape literals.
   *
   * @param sql
   *          statement text to inspect
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
    if (sql.startsWith("--", index) || sql.startsWith("//", index)) {
      return skipToLineEnd(sql, index);
    }
    if (sql.startsWith("/*", index)) {
      return skipBlockComment(sql, index);
    }
    if (current == '{') {
      Integer end = validateTemporalEscape(sql, index);
      return end == null ? index + 1 : end;
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
   * @return offset of the first character after the literal, or null when the
   *         brace does not start a temporal escape
   * @throws IllegalArgumentException
   *           if the literal text fails strict temporal conversion
   */
  private static Integer validateTemporalEscape(String sql, int index) {
    int tagStart = index + 1;
    int tagEnd = tagStart;
    int length = sql.length();
    while (tagEnd < length && isTagCharacter(sql.charAt(tagEnd))) {
      tagEnd++;
    }
    String tag = sql.substring(tagStart, tagEnd).toUpperCase(Locale.ROOT);
    if (!tag.equals("D") && !tag.equals("T") && !tag.equals("TS")) {
      return null;
    }
    int literalStart = skipWhitespace(sql, tagEnd);
    if (literalStart >= length || sql.charAt(literalStart) != '\'') {
      return null;
    }
    int literalEnd = SqlTextScanner.quotedRegionEnd(sql, literalStart);
    int closing = skipWhitespace(sql, literalEnd);
    if (closing >= length || sql.charAt(closing) != '}') {
      return null;
    }
    String text = sql.substring(literalStart + 1, literalEnd - 1);
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
   * Skips whitespace tokens recognized by the parser.
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
    while (offset < length) {
      char c = sql.charAt(offset);
      if (c != ' ' && c != '\t' && c != '\r' && c != '\n') {
        return offset;
      }
      offset++;
    }
    return offset;
  }

  /**
   * Advances past a line comment beginning with two characters.
   *
   * @param sql
   *          statement text
   * @param index
   *          offset of the comment start
   * @return offset of the line terminator or the end of the text
   */
  private static int skipToLineEnd(String sql, int index) {
    int i = index + 2;
    int length = sql.length();
    while (i < length && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
      i++;
    }
    return i;
  }

  /**
   * Advances past a possibly nested block comment.
   *
   * @param sql
   *          statement text
   * @param index
   *          offset of the opening delimiter
   * @return offset of the first character after the comment
   */
  private static int skipBlockComment(String sql, int index) {
    int depth = 1;
    int i = index + 2;
    int length = sql.length();
    while (i < length && depth > 0) {
      if (sql.startsWith("/*", i)) {
        depth++;
        i += 2;
      } else if (sql.startsWith("*/", i)) {
        depth--;
        i += 2;
      } else {
        i++;
      }
    }
    return i;
  }

}
