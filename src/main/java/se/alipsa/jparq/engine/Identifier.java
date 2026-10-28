package se.alipsa.jparq.engine;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a SQL identifier while preserving whether it was quoted in the
 * original statement. Quoted identifiers retain their case sensitivity whereas
 * unquoted identifiers are normalized using lower-case semantics for lookup
 * purposes.
 */
public final class Identifier {

  private final String text;
  private final boolean quoted;

  private Identifier(String text, boolean quoted) {
    this.text = Objects.requireNonNull(text, "text");
    this.quoted = quoted;
  }

  /**
   * Create an {@link Identifier} from the raw text supplied by the parser.
   *
   * @param raw
   *          identifier text that may include surrounding quote characters
   * @return a populated {@link Identifier} or {@code null} when {@code raw} is
   *         {@code null} or blank
   */
  public static Identifier of(String raw) {
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    boolean quoted = isDelimitedIdentifier(trimmed);
    String unquoted = quoted ? trimmed.substring(1, trimmed.length() - 1) : trimmed;
    if (quoted) {
      char delimiter = trimmed.charAt(trimmed.length() - 1);
      String escaped = Character.toString(delimiter).repeat(2);
      unquoted = unquoted.replace(escaped, Character.toString(delimiter));
    }
    return new Identifier(unquoted, quoted);
  }

  /**
   * Checks whether the entire text is one delimited identifier.
   *
   * @param text
   *          trimmed identifier or expression text
   * @return true when matching delimiters surround only escaped interior
   *         delimiters
   */
  private static boolean isDelimitedIdentifier(String text) {
    if (text.length() < 2) {
      return false;
    }
    char first = text.charAt(0);
    char closing = first == '[' ? ']' : first;
    if ((first != '"' && first != '`' && first != '[') || text.charAt(text.length() - 1) != closing) {
      return false;
    }
    for (int i = 1; i < text.length() - 1; i++) {
      if (text.charAt(i) == closing) {
        if (i + 1 >= text.length() - 1 || text.charAt(i + 1) != closing) {
          return false;
        }
        i++;
      }
    }
    return true;
  }

  /**
   * Normalizes a name for filesystem catalog comparison using connection rules.
   * 
   * @param raw
   *          possibly quoted name
   * @param caseSensitive
   *          whether unquoted names retain their case
   * @return normalized text, or an empty string for a missing name
   */
  public static String normalize(String raw, boolean caseSensitive) {
    Identifier identifier = of(raw);
    if (identifier == null) {
      return "";
    }
    return caseSensitive || identifier.quoted() ? identifier.text() : identifier.normalized();
  }

  /**
   * Determine whether the identifier was quoted in the originating SQL statement.
   *
   * @return {@code true} when the identifier was quoted
   */
  public boolean quoted() {
    return quoted;
  }

  /**
   * Retrieve the unquoted identifier text with surrounding quote characters
   * removed.
   *
   * @return the unquoted identifier text
   */
  public String text() {
    return text;
  }

  /**
   * Compute the normalized text used for comparisons. Quoted identifiers retain
   * their original casing whereas unquoted identifiers are converted to lower
   * case to provide case-insensitive matching.
   *
   * @return normalized identifier text
   */
  public String normalized() {
    return quoted ? text : text.toLowerCase(Locale.ROOT);
  }

  /**
   * Produce a composite lookup key that encodes the quoting semantics so that
   * quoted and unquoted identifiers with the same characters do not collide.
   *
   * @return lookup key reflecting quoting semantics and normalized text
   */
  public String lookupKey() {
    return (quoted ? "Q:" : "U:") + normalized();
  }

  /**
   * Convenience factory to build a lookup key directly from raw identifier text.
   *
   * @param raw
   *          identifier text that may be quoted
   * @return lookup key reflecting quoting semantics, or {@code null} when the
   *         input is {@code null} or blank
   */
  public static String lookupKey(String raw) {
    Identifier identifier = of(raw);
    return identifier == null ? null : identifier.lookupKey();
  }

  /**
   * Looks up a name, preferring its exact key before an equivalent lowercase
   * name. Quoted names containing uppercase letters never use an unquoted
   * fallback. Unlike column-key fallback, this also resolves unquoted names to
   * quoted lowercase declarations because CTE declaration keys retain quoting.
   *
   * @param <T>
   *          mapped value type
   * @param names
   *          identifier mapping
   * @return the exact or equivalent mapped value, or null if absent
   */
  public <T> T resolve(Map<Identifier, T> names) {
    T exact = names.get(this);
    if (exact != null || (quoted && !text.equals(text.toLowerCase(Locale.ROOT)))) {
      return exact;
    }
    Identifier alternative = new Identifier(normalized(), !quoted);
    return names.get(alternative);
  }

  /**
   * Determine whether this identifier represents the same logical reference as
   * the supplied identifier, using SQL quoting semantics.
   *
   * @param other
   *          identifier to compare with (may be {@code null})
   * @return {@code true} when both identifiers match under SQL rules
   */
  public boolean matches(Identifier other) {
    return other != null && lookupKey().equals(other.lookupKey());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Identifier other)) {
      return false;
    }
    return matches(other);
  }

  @Override
  public int hashCode() {
    return lookupKey().hashCode();
  }

  @Override
  public String toString() {
    return text;
  }
}
