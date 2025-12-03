package se.alipsa.jparq.engine;

import java.util.Locale;
import java.util.Objects;
import net.sf.jsqlparser.schema.MultiPartName;

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
    boolean quoted = MultiPartName.isQuoted(trimmed);
    String unquoted = MultiPartName.unquote(trimmed);
    return new Identifier(unquoted, quoted);
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
