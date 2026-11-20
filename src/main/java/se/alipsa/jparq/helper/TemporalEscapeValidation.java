package se.alipsa.jparq.helper;

import java.sql.SQLDataException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/**
 * Rejects JDBC temporal escape literals whose text fails the JDBC escape shape
 * or strict temporal conversion.
 *
 * <p>
 * JSqlParser builds {@code DateValue}, {@code TimeValue}, and
 * {@code TimestampValue} nodes with the lenient {@code java.sql}
 * {@code valueOf} parsers, which normalize out-of-range fields such as
 * {@code 2024-02-30} or {@code 25:00:00} instead of rejecting them, and reject
 * text shapes the JDBC spec does not define, such as ISO date-time text for a
 * {@code {d}} literal. The parser also discards the original text of date and
 * time escapes, so this validator re-checks the statement text: a shape
 * mismatch fails with SQLState 22007, and a shape-conforming value that fails
 * the same strict rules as the typed {@code DATE}, {@code TIME}, and
 * {@code TIMESTAMP} literals fails with 22008.
 */
public final class TemporalEscapeValidation {

  private static final EscapeType DATE = new EscapeType("D", "DATE", Pattern.compile("\\d{4}-\\d{1,2}-\\d{1,2}"),
      "1970-01-01");
  private static final EscapeType TIME = new EscapeType("T", "TIME", Pattern.compile("\\d{2}:\\d{2}:\\d{2}"),
      "00:00:00");
  private static final EscapeType TIMESTAMP = new EscapeType("TS", "TIMESTAMP",
      Pattern.compile("\\d{4}-\\d{1,2}-\\d{1,2} \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?"), "1970-01-01 00:00:00");

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
   *           cause carrying SQLState 22007 for text that does not match the JDBC
   *           escape shape, 22008 for out-of-range fields, or if a quoted region
   *           is unterminated
   */
  public static void validate(String sql) {
    for (EscapeLiteral escape : temporalEscapes(sql)) {
      validateLiteral(escape);
    }
  }

  /**
   * Reports whether the statement text contains at least one temporal escape
   * literal outside quoted regions.
   *
   * @param sql
   *          statement text without SQL comments
   * @return whether a temporal escape literal is present
   */
  public static boolean hasTemporalEscape(String sql) {
    return !temporalEscapes(sql).isEmpty();
  }

  /**
   * Replaces the value of every temporal escape literal outside quoted regions
   * with a valid placeholder of the same shape, retaining all other text. The
   * result keeps the same token structure as the input, so parsing it confirms
   * whether a parse failure came from an escape value or from the statement
   * syntax. The input must have SQL comments removed before scanning.
   *
   * @param sql
   *          statement text without SQL comments
   * @return the statement text with placeholder escape values, or the input when
   *         it carries no temporal escape
   * @throws IllegalArgumentException
   *           if a quoted region is unterminated
   */
  public static String withPlaceholderValues(String sql) {
    List<EscapeLiteral> escapes = temporalEscapes(sql);
    if (escapes.isEmpty()) {
      return sql;
    }
    StringBuilder result = new StringBuilder(sql.length());
    int position = 0;
    for (EscapeLiteral escape : escapes) {
      result.append(sql, position, escape.literalStart() + 1);
      result.append(escape.type().placeholder());
      result.append('\'');
      position = escape.literalEnd();
    }
    return result.append(sql, position, sql.length()).toString();
  }

  /**
   * Collects the temporal escape literals outside quoted regions in text order.
   *
   * @param sql
   *          statement text without SQL comments
   * @return the escape literals, possibly empty
   * @throws IllegalArgumentException
   *           if a quoted region is unterminated
   */
  private static List<EscapeLiteral> temporalEscapes(String sql) {
    List<EscapeLiteral> escapes = new ArrayList<>();
    if (sql == null || sql.indexOf('{') < 0) {
      return escapes;
    }
    int index = 0;
    int length = sql.length();
    while (index < length) {
      char current = sql.charAt(index);
      if (current == '\'' || current == '"' || current == '`' || current == '$') {
        int end = SqlTextScanner.quotedRegionEnd(sql, index);
        index = end >= 0 ? end : index + 1;
      } else if (current == '{') {
        EscapeLiteral escape = temporalEscapeLiteral(sql, index);
        if (escape == null) {
          index++;
        } else {
          escapes.add(escape);
          index = escape.end();
        }
      } else {
        index++;
      }
    }
    return escapes;
  }

  /**
   * Validates one escape literal against the JDBC escape shape and, for
   * shape-conforming text, the strict temporal conversion helpers.
   *
   * @param escape
   *          the escape literal to validate
   * @throws IllegalArgumentException
   *           if the literal text fails the shape check with a cause carrying
   *           SQLState 22007, or the strict conversion with SQLState 22008
   */
  private static void validateLiteral(EscapeLiteral escape) {
    EscapeType type = escape.type();
    if (!type.shape().matcher(escape.text()).matches()) {
      String message = "Invalid " + type.typeName() + " value: " + SqlExceptions.diagnosticValue(escape.text());
      throw new IllegalArgumentException(message, new SQLDataException(message, "22007"));
    }
    switch (type.tag()) {
      case "D" -> DateTimeFunctions.toDate(escape.text());
      case "T" -> DateTimeFunctions.toTime(escape.text());
      default -> DateTimeFunctions.toTimestamp(escape.text());
    }
  }

  /**
   * Identifies a temporal escape literal starting at the opening brace.
   *
   * @param sql
   *          statement text
   * @param index
   *          offset of the opening brace
   * @return the escape literal, or null when the brace does not start a temporal
   *         escape
   * @throws IllegalArgumentException
   *           if the literal's quoted region is unterminated
   */
  private static EscapeLiteral temporalEscapeLiteral(String sql, int index) {
    int tagStart = index + 1;
    int tagEnd = tagStart;
    int length = sql.length();
    while (tagEnd < length && isTagCharacter(sql.charAt(tagEnd))) {
      tagEnd++;
    }
    EscapeType type = escapeType(sql.substring(tagStart, tagEnd));
    if (type == null) {
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
    String text = sql.substring(literalStart + 1, literalEnd - 1).replace("''", "'");
    return new EscapeLiteral(type, literalStart, literalEnd, closing + 1, text);
  }

  /**
   * Maps a scanned escape tag to its temporal escape type.
   *
   * @param tag
   *          the tag text between the opening brace and the literal
   * @return the escape type, or null for non-temporal tags
   */
  private static EscapeType escapeType(String tag) {
    return switch (tag.toUpperCase(Locale.ROOT)) {
      case "D" -> DATE;
      case "T" -> TIME;
      case "TS" -> TIMESTAMP;
      default -> null;
    };
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

  /**
   * A temporal escape tag with its type name, JDBC shape, and placeholder value.
   */
  private record EscapeType(String tag, String typeName, Pattern shape, String placeholder) {
  }

  /** A temporal escape literal found in the statement text. */
  private record EscapeLiteral(EscapeType type, int literalStart, int literalEnd, int end, String text) {
  }

}
