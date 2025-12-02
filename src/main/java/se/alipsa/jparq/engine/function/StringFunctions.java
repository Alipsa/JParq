package se.alipsa.jparq.engine.function;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import net.sf.jsqlparser.expression.Expression;

/** Utility methods implementing SQL string functions. */
public final class StringFunctions {

  /** Enumeration describing which sides should be trimmed. */
  public enum TrimMode {
    /** Trim characters from the start of the input only. */
    LEADING,
    /** Trim characters from the end of the input only. */
    TRAILING,
    /** Trim characters from both the start and end of the input. */
    BOTH
  }

  private StringFunctions() {
  }

  /**
   * Implementation of the COALESCE(expr1, expr2, ...) function.
   *
   * @param arguments
   *          function arguments resolver
   * @return the first non-null argument value, or {@code null} if all are null
   */
  public static Object coalesce(FunctionArguments arguments) {
    List<Expression> expressions = arguments.positionalExpressions();
    if (expressions.isEmpty()) {
      return null;
    }
    for (Expression expr : expressions) {
      Object value = arguments.evaluate(expr);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  /**
   * Return the number of Unicode code points in the supplied value.
   *
   * @param value
   *          value to inspect
   * @return number of characters or {@code null} when {@code value} is
   *         {@code null}
   */
  public static Integer charLength(Object value) {
    if (value == null) {
      return null;
    }
    String str = value.toString();
    return str.codePointCount(0, str.length());
  }

  /**
   * Return the number of UTF-8 encoded bytes required for the supplied value.
   *
   * @param value
   *          value to inspect
   * @return number of bytes or {@code null} when {@code value} is {@code null}
   */
  public static Integer octetLength(Object value) {
    if (value == null) {
      return null;
    }
    return value.toString().getBytes(StandardCharsets.UTF_8).length;
  }

  /**
   * ASCII code for the first character of the input.
   *
   * @param value
   *          value to inspect
   * @return ASCII code or {@code null} when {@code value} is {@code null} or
   *         empty
   */
  public static Integer ascii(Object value) {
    if (value == null) {
      return null;
    }
    String str = value.toString();
    if (str.isEmpty()) {
      return null;
    }
    return str.codePointAt(0);
  }

  /**
   * Locate substring in a string with optional start position.
   *
   * @param substring
   *          value to search for
   * @param input
   *          value to search within
   * @param start
   *          one-based start position (may be {@code null})
   * @return one-based position or {@code 0} when not found
   */
  public static Integer locate(Object substring, Object input, Number start) {
    if (substring == null || input == null) {
      return null;
    }
    String needle = substring.toString();
    String haystack = input.toString();
    int from = start == null ? 0 : Math.max(0, start.intValue() - 1);
    if (from >= haystack.length()) {
      return 0;
    }
    int idx = haystack.indexOf(needle, from);
    if (idx < 0) {
      return 0;
    }
    int codePoints = haystack.substring(0, idx).codePointCount(0, idx);
    return codePoints + 1;
  }

  /**
   * Locate the given {@code substring} inside {@code input} using one-based
   * indexing.
   *
   * @param substring
   *          value to search for
   * @param input
   *          value to search within
   * @return one-based position, {@code 0} when not found or {@code null} for null
   *         inputs
   */
  public static Integer position(Object substring, Object input) {
    if (substring == null || input == null) {
      return null;
    }
    String needle = substring.toString();
    String haystack = input.toString();
    if (needle.isEmpty()) {
      return 1;
    }
    int idx = haystack.indexOf(needle);
    if (idx < 0) {
      return 0;
    }
    if (idx == 0) {
      return 1;
    }
    int codePoints = haystack.substring(0, idx).codePointCount(0, idx);
    return codePoints + 1;
  }

  /**
   * Insert a replacement string at the given position after removing length
   * characters.
   *
   * @param input
   *          source string
   * @param start
   *          one-based start position
   * @param length
   *          number of characters to remove
   * @param replacement
   *          value to insert
   * @return resulting string or {@code null} when required inputs are missing
   */
  public static String insert(String input, Number start, Number length, String replacement) {
    if (input == null || start == null || length == null || replacement == null) {
      return null;
    }
    int[] cps = input.codePoints().toArray();
    int[] replCps = replacement.codePoints().toArray();
    int from = Math.max(0, start.intValue() - 1);
    int len = Math.max(0, length.intValue());
    int endExclusive = Math.min(cps.length, from + len);

    StringBuilder sb = new StringBuilder();
    sb.append(new String(cps, 0, Math.min(from, cps.length)));
    sb.append(new String(replCps, 0, replCps.length));
    if (endExclusive < cps.length) {
      sb.append(new String(cps, endExclusive, cps.length - endExclusive));
    }
    return sb.toString();
  }

  /**
   * Implementation of {@code SUBSTRING(string FROM start FOR length)}.
   *
   * @param input
   *          source string
   * @param start
   *          one-based start position
   * @param length
   *          optional length in characters (may be {@code null})
   * @return extracted substring or {@code null} when {@code input} or
   *         {@code start} is {@code null}
   */
  public static String substring(String input, Number start, Number length) {
    if (input == null || start == null) {
      return null;
    }
    int[] cps = input.codePoints().toArray();
    int from = Math.max(0, start.intValue() - 1);
    if (from >= cps.length) {
      return "";
    }
    int toExclusive;
    if (length == null) {
      toExclusive = cps.length;
    } else {
      toExclusive = Math.min(cps.length, from + Math.max(0, length.intValue()));
    }
    return new String(cps, from, Math.max(0, toExclusive - from));
  }

  /**
   * Repeat the supplied string {@code count} times.
   *
   * @param input
   *          source string
   * @param count
   *          repetition count
   * @return repeated string or {@code null} when inputs are missing
   */
  public static String repeat(String input, Number count) {
    if (input == null || count == null) {
      return null;
    }
    int n = Math.max(0, count.intValue());
    return input.repeat(n);
  }

  /**
   * Create a string consisting of {@code count} space characters.
   *
   * @param count
   *          number of spaces
   * @return space-filled string or {@code null} when {@code count} is null
   */
  public static String space(Number count) {
    if (count == null) {
      return null;
    }
    int n = Math.max(0, count.intValue());
    return " ".repeat(n);
  }

  /**
   * Implementation of {@code LEFT(string, count)}.
   *
   * @param input
   *          source string
   * @param count
   *          number of characters to keep from the left
   * @return resulting substring or {@code null} when {@code input} or
   *         {@code count} is {@code null}
   */
  public static String left(String input, Number count) {
    if (input == null || count == null) {
      return null;
    }
    int n = Math.max(0, count.intValue());
    int[] cps = input.codePoints().toArray();
    int end = Math.min(n, cps.length);
    return new String(cps, 0, end);
  }

  /**
   * Implementation of {@code RIGHT(string, count)}.
   *
   * @param input
   *          source string
   * @param count
   *          number of characters to keep from the right
   * @return resulting substring or {@code null} when {@code input} or
   *         {@code count} is {@code null}
   */
  public static String right(String input, Number count) {
    if (input == null || count == null) {
      return null;
    }
    int n = Math.max(0, count.intValue());
    int[] cps = input.codePoints().toArray();
    if (n >= cps.length) {
      return input;
    }
    return new String(cps, cps.length - n, n);
  }

  /**
   * Concatenate arguments following SQL semantics that ignore {@code NULL}
   * values.
   *
   * @param values
   *          values to concatenate
   * @return concatenated string or {@code null} if all values are {@code null}
   */
  public static String concat(List<Object> values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    boolean any = false;
    for (Object value : values) {
      if (value == null) {
        continue;
      }
      sb.append(value);
      any = true;
    }
    return any ? sb.toString() : null;
  }

  /**
   * Convert the supplied value to upper case using the root locale.
   *
   * @param value
   *          value to convert
   * @return upper-case representation or {@code null}
   */
  public static String upper(Object value) {
    return value == null ? null : value.toString().toUpperCase(Locale.ROOT);
  }

  /**
   * Convert the supplied value to lower case using the root locale.
   *
   * @param value
   *          value to convert
   * @return lower-case representation or {@code null}
   */
  public static String lower(Object value) {
    return value == null ? null : value.toString().toLowerCase(Locale.ROOT);
  }

  /**
   * Trim characters from the supplied string following {@code TRIM} semantics.
   *
   * @param input
   *          source string
   * @param characters
   *          characters to remove (defaults to space)
   * @param mode
   *          which side(s) to trim
   * @return trimmed string or {@code null} when {@code input} is {@code null}
   */
  public static String trim(String input, String characters, TrimMode mode) {
    if (input == null) {
      return null;
    }
    String chars = (characters == null || characters.isEmpty()) ? " " : characters;
    int[] trimSet = chars.codePoints().distinct().toArray();
    int[] cps = input.codePoints().toArray();
    int start = 0;
    int end = cps.length;
    if (mode == TrimMode.LEADING || mode == TrimMode.BOTH) {
      while (start < end && contains(trimSet, cps[start])) {
        start++;
      }
    }
    if (mode == TrimMode.TRAILING || mode == TrimMode.BOTH) {
      while (end > start && contains(trimSet, cps[end - 1])) {
        end--;
      }
    }
    if (start == 0 && end == cps.length) {
      return input;
    }
    return new String(cps, start, end - start);
  }

  /**
   * Pad the left side of {@code input} until reaching {@code length} characters.
   *
   * @param input
   *          source string
   * @param length
   *          desired total length
   * @param fill
   *          optional padding string (defaults to space)
   * @return padded string or {@code null} when {@code input} or {@code length} is
   *         {@code null}
   */
  public static String lpad(String input, Number length, String fill) {
    if (input == null || length == null) {
      return null;
    }
    int target = Math.max(0, length.intValue());
    int[] cps = input.codePoints().toArray();
    if (target == 0) {
      return "";
    }
    if (cps.length >= target) {
      return new String(cps, 0, target);
    }
    int padCount = target - cps.length;
    String pad = buildPadding(fill, padCount);
    return pad + input;
  }

  /**
   * Pad the right side of {@code input} until reaching {@code length} characters.
   *
   * @param input
   *          source string
   * @param length
   *          desired total length
   * @param fill
   *          optional padding string (defaults to space)
   * @return padded string or {@code null} when {@code input} or {@code length} is
   *         {@code null}
   */
  public static String rpad(String input, Number length, String fill) {
    if (input == null || length == null) {
      return null;
    }
    int target = Math.max(0, length.intValue());
    int[] cps = input.codePoints().toArray();
    if (target == 0) {
      return "";
    }
    if (cps.length >= target) {
      return new String(cps, 0, target);
    }
    int padCount = target - cps.length;
    String pad = buildPadding(fill, padCount);
    return input + pad;
  }

  private static String buildPadding(String fill, int padCount) {
    String filler = (fill == null || fill.isEmpty()) ? " " : fill;
    int[] fillCps = filler.codePoints().toArray();
    List<Integer> buffer = new ArrayList<>();
    while (buffer.size() < padCount) {
      for (int cp : fillCps) {
        if (buffer.size() == padCount) {
          break;
        }
        buffer.add(cp);
      }
    }
    int[] cps = buffer.stream().mapToInt(Integer::intValue).toArray();
    return new String(cps, 0, cps.length);
  }

  private static boolean contains(int[] cps, int candidate) {
    for (int cp : cps) {
      if (cp == candidate) {
        return true;
      }
    }
    return false;
  }

  /**
   * Implementation of
   * {@code OVERLAY(string PLACING replacement FROM start FOR length)}.
   *
   * @param input
   *          source string
   * @param replacement
   *          replacement text
   * @param start
   *          one-based start position
   * @param length
   *          optional length of the region to replace
   * @return resulting string or {@code null} when required inputs are
   *         {@code null}
   */
  public static String overlay(String input, String replacement, Number start, Number length) {
    if (input == null || replacement == null || start == null) {
      return null;
    }
    int[] base = input.codePoints().toArray();
    int[] repl = replacement.codePoints().toArray();
    int from = Math.max(0, start.intValue() - 1);
    int count = length == null ? repl.length : Math.max(0, length.intValue());
    int end = Math.min(base.length, from + count);
    StringBuilder sb = new StringBuilder();
    sb.append(new String(base, 0, Math.min(from, base.length)));
    sb.append(new String(repl, 0, repl.length));
    if (end < base.length) {
      sb.append(new String(base, end, base.length - end));
    }
    return sb.toString();
  }

  /**
   * Replace all occurrences of {@code search} with {@code replacement} following
   * SQL semantics.
   *
   * @param input
   *          source string
   * @param search
   *          substring to replace
   * @param replacement
   *          replacement value
   * @return string with replacements applied or {@code null} when any parameter
   *         is {@code null}
   */
  public static String replace(String input, String search, String replacement) {
    if (input == null || search == null || replacement == null) {
      return null;
    }
    if (search.isEmpty()) {
      return input;
    }
    return input.replace(search, replacement);
  }

  /**
   * Compute the Soundex code for the supplied value.
   *
   * @param value
   *          input value
   * @return four-character Soundex code or {@code null} when input is null or
   *         empty
   */
  public static String soundex(Object value) {
    if (value == null) {
      return null;
    }
    String text = value.toString().trim().toUpperCase(Locale.ROOT);
    if (text.isEmpty()) {
      return null;
    }
    char first = text.charAt(0);
    StringBuilder code = new StringBuilder();
    code.append(first);
    char prev = mapSoundexChar(first);
    for (int i = 1; i < text.length() && code.length() < 4; i++) {
      char mapped = mapSoundexChar(text.charAt(i));
      if (mapped == '0' || mapped == prev) {
        continue;
      }
      code.append(mapped);
      prev = mapped;
    }
    while (code.length() < 4) {
      code.append('0');
    }
    return code.substring(0, 4);
  }

  /**
   * Calculate the similarity between two strings based on their Soundex codes.
   *
   * @param left
   *          first value
   * @param right
   *          second value
   * @return integer between 0 and 4 indicating matching positions in the Soundex
   *         codes
   */
  public static Integer difference(Object left, Object right) {
    String l = soundex(left);
    String r = soundex(right);
    if (l == null || r == null) {
      return null;
    }
    int matches = 0;
    for (int i = 0; i < Math.min(l.length(), r.length()); i++) {
      if (l.charAt(i) == r.charAt(i)) {
        matches++;
      }
    }
    return matches;
  }

  private static char mapSoundexChar(char ch) {
    return switch (ch) {
      case 'B', 'F', 'P', 'V' -> '1';
      case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z' -> '2';
      case 'D', 'T' -> '3';
      case 'L' -> '4';
      case 'M', 'N' -> '5';
      case 'R' -> '6';
      default -> '0';
    };
  }

  /**
   * Evaluate SQL {@code LIKE} and {@code ILIKE} semantics.
   *
   * @param input
   *          the input text
   * @param pattern
   *          the LIKE pattern
   * @param caseInsensitive
   *          whether the match should ignore case
   * @param escape
   *          optional escape character (may be {@code null})
   * @return {@code true} if the pattern matches; otherwise {@code false}
   */
  public static boolean like(String input, String pattern, boolean caseInsensitive, Character escape) {
    if (input == null || pattern == null) {
      return false;
    }
    String regex = toLikeRegex(pattern, escape);
    int flags = Pattern.DOTALL;
    if (caseInsensitive) {
      flags |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
    }
    Pattern compiled = Pattern.compile(regex, flags);
    return compiled.matcher(input).matches();
  }

  /**
   * Converts a SQL {@code LIKE} pattern to a Java regular expression string.
   * Supports SQL wildcards {@code %} (matches any sequence of characters) and {@code _} (matches any single character),
   * as well as an optional escape character to treat wildcards as literals.
   *
   * @param pattern
   *          the SQL LIKE pattern to convert
   * @param escape
   *          optional escape character (may be {@code null}); if present, the next character is treated as a literal
   * @return the equivalent Java regular expression string
   * @throws IllegalArgumentException if the escape character appears at the end of the pattern
   */
  public static String toLikeRegex(String pattern, Character escape) {
    StringBuilder regex = new StringBuilder();
    regex.append('^');
    for (int i = 0; i < pattern.length(); i++) {
      char ch = pattern.charAt(i);
      if (escape != null && ch == escape.charValue()) {
        if (i + 1 >= pattern.length()) {
          throw new IllegalArgumentException("Invalid LIKE pattern: escape at end of pattern");
        }
        char next = pattern.charAt(++i);
        appendLiteral(regex, next, false);
        continue;
      }
      switch (ch) {
        case '%': {
          regex.append(".*");
          break;
        }
        case '_': {
          regex.append('.');
          break;
        }
        default: {
          appendLiteral(regex, ch, false);
        }
      }
    }
    regex.append('$');
    return regex.toString();
  }

  /**
   * Evaluate SQL {@code SIMILAR TO} semantics by translating the pattern into a
   * Java regular expression.
   *
   * @param input
   *          the value to test
   * @param pattern
   *          the SIMILAR TO pattern
   * @param escape
   *          optional escape character (may be {@code null})
   * @return {@code true} if {@code input} matches the pattern; otherwise
   *         {@code false}
   */
  public static boolean similarTo(String input, String pattern, Character escape) {
    if (input == null || pattern == null) {
      return false;
    }
    String regex = toSimilarRegex(pattern, escape);
    Pattern compiled = Pattern.compile(regex, Pattern.DOTALL | Pattern.UNICODE_CASE);
    return compiled.matcher(input).matches();
  }

  private static String toSimilarRegex(String pattern, Character escape) {
    StringBuilder regex = new StringBuilder();
    regex.append('^');
    boolean inCharClass = false;
    for (int i = 0; i < pattern.length(); i++) {
      char ch = pattern.charAt(i);
      if (escape != null && ch == escape.charValue()) {
        if (i + 1 >= pattern.length()) {
          throw new IllegalArgumentException("Invalid SIMILAR TO pattern: escape at end of pattern");
        }
        char next = pattern.charAt(++i);
        appendLiteral(regex, next, inCharClass);
        continue;
      }
      switch (ch) {
        case '%': {
          regex.append(inCharClass ? "%" : ".*");
          break;
        }
        case '_': {
          regex.append(inCharClass ? "_" : ".");
          break;
        }
        case '[': {
          regex.append('[');
          inCharClass = true;
          break;
        }
        case ']': {
          regex.append(']');
          inCharClass = false;
          break;
        }
        case '(':
        case ')':
        case '|':
        case '*':
        case '+':
        case '?':
        case '{':
        case '}':
        case '^':
        case '$': {
          regex.append(ch);
          break;
        }
        default: {
          appendLiteral(regex, ch, inCharClass);
        }
      }
    }
    regex.append('$');
    return regex.toString();
  }

  /**
   * Evaluate {@code REGEXP_LIKE} with optional modifier flags.
   *
   * @param input
   *          the value to test
   * @param pattern
   *          the regular expression pattern
   * @param options
   *          optional flags ({@code i}, {@code m}, {@code n}/{@code s},
   *          {@code x}, {@code c})
   * @return {@code true} when the pattern matches, {@code false} when it does not
   *         and {@code null} when {@code input} or {@code pattern} is
   *         {@code null}
   */
  public static Boolean regexpLike(String input, String pattern, String options) {
    if (input == null || pattern == null) {
      return null;
    }
    int flags = 0;
    if (options != null) {
      String opts = options.toLowerCase(Locale.ROOT);
      for (int i = 0; i < opts.length(); i++) {
        char opt = opts.charAt(i);
        switch (opt) {
          case 'i' -> flags |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
          case 'm' -> flags |= Pattern.MULTILINE;
          case 'n', 's' -> flags |= Pattern.DOTALL;
          case 'x' -> flags |= Pattern.COMMENTS;
          case 'c' -> {
            // explicit case-sensitive flag, ignore since default
          }
          default -> {
            // ignore unknown flags
          }
        }
      }
    }
    Pattern compiled = Pattern.compile(pattern, flags);
    return compiled.matcher(input).find();
  }

  private static void appendLiteral(StringBuilder regex, char ch, boolean inCharClass) {
    if (inCharClass) {
      if (ch == '\\' || ch == '-' || ch == '^') {
        regex.append('\\');
      }
      regex.append(ch);
    } else {
      if ("\\.{}()*+?^$|".indexOf(ch) >= 0) {
        regex.append('\\');
      }
      regex.append(ch);
    }
  }

  /**
   * Convert a list of Unicode code points into a string.
   *
   * @param codes
   *          code points to convert
   * @return constructed string or {@code null} when {@code codes} is {@code null}
   *         or empty
   */
  public static String charFromCodes(List<Object> codes) {
    if (codes == null || codes.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (Object code : codes) {
      if (code == null) {
        continue;
      }
      int cp = ((Number) code).intValue();
      sb.appendCodePoint(cp);
    }
    return sb.toString();
  }

  /**
   * Return the Unicode code point of the first character in {@code value}.
   *
   * @param value
   *          value to inspect
   * @return code point or {@code null} when {@code value} is {@code null}
   */
  public static Integer unicode(Object value) {
    if (value == null) {
      return null;
    }
    String str = value.toString();
    if (str.isEmpty()) {
      return 0;
    }
    return str.codePointAt(0);
  }

  /**
   * Normalize Unicode text using the specified normalization form.
   *
   * @param value
   *          value to normalize
   * @param formName
   *          normalization form ({@code NFC}, {@code NFD}, {@code NFKC},
   *          {@code NFKD})
   * @return normalized string or {@code null} when {@code value} is {@code null}
   */
  public static String normalize(Object value, Object formName) {
    if (value == null) {
      return null;
    }
    String str = value.toString();
    Form form = Form.NFC;
    if (formName != null) {
      form = switch (formName.toString().toUpperCase(Locale.ROOT)) {
        case "NFD" -> Form.NFD;
        case "NFKC" -> Form.NFKC;
        case "NFKD" -> Form.NFKD;
        default -> Form.NFC;
      };
    }
    return Normalizer.normalize(str, form);
  }
}
