package se.alipsa.jparq.engine;

/**
 * Utility methods for working with SQL identifier strings.
 */
public final class IdentifierUtil {

  private IdentifierUtil() {
  }

  /**
   * Remove common quote characters surrounding an identifier while preserving
   * the inner value.
   *
   * @param identifier
   *          identifier text that may include leading and trailing quotes
   * @return the identifier without enclosing quotes; {@code null} when the
   *         input is {@code null}
   */
  public static String sanitizeIdentifier(String identifier) {
    if (identifier == null) {
      return null;
    }
    String trimmed = identifier.trim();
    if (trimmed.length() < 2) {
      return trimmed;
    }
    char first = trimmed.charAt(0);
    char last = trimmed.charAt(trimmed.length() - 1);
    if ((first == '"' && last == '"') || (first == '`' && last == '`')) {
      return trimmed.substring(1, trimmed.length() - 1);
    }
    if (first == '[' && last == ']') {
      return trimmed.substring(1, trimmed.length() - 1);
    }
    return trimmed;
  }
}

