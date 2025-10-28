package se.alipsa.jparq.helper;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Utility methods implementing SQL string functions. */
public final class StringExpressions {

  private StringExpressions() {
  }

  /** Count the number of Unicode characters. */
  public static Integer charLength(Object value) {
    if (value == null) {
      return null;
    }
    String str = value.toString();
    return str.codePointCount(0, str.length());
  }

  /** Count the number of UTF-8 bytes in the supplied value. */
  public static Integer octetLength(Object value) {
    if (value == null) {
      return null;
    }
    return value.toString().getBytes(StandardCharsets.UTF_8).length;
  }

  /** Position of {@code substring} within {@code input} (1-based). */
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

  /** SUBSTRING(string FROM start FOR length). */
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

  /** LEFT(string, count). */
  public static String left(String input, Number count) {
    if (input == null || count == null) {
      return null;
    }
    int n = Math.max(0, count.intValue());
    int[] cps = input.codePoints().toArray();
    int end = Math.min(n, cps.length);
    return new String(cps, 0, end);
  }

  /** RIGHT(string, count). */
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

  /** Concatenate arguments following SQL semantics (NULL values ignored). */
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

  public static String upper(Object value) {
    return value == null ? null : value.toString().toUpperCase(Locale.ROOT);
  }

  public static String lower(Object value) {
    return value == null ? null : value.toString().toLowerCase(Locale.ROOT);
  }

  public enum TrimMode {
    LEADING, TRAILING, BOTH
  }

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

  private static String toLikeRegex(String pattern, Character escape) {
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
   * Evaluate SQL {@code SIMILAR TO} semantics by translating the pattern into a Java regular
   * expression.
   *
   * @param input
   *          the value to test
   * @param pattern
   *          the SIMILAR TO pattern
   * @param escape
   *          optional escape character (may be {@code null})
   * @return {@code true} if {@code input} matches the pattern; otherwise {@code false}
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
        case '(': case ')': case '|': case '*': case '+': case '?': case '{': case '}': case '^': case '$': {
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
