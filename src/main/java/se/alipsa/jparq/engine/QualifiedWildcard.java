package se.alipsa.jparq.engine;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents a qualified wildcard projection such as {@code table.*} or
 * {@code alias.*}.
 */
public final class QualifiedWildcard implements SelectListItem {
  private final String qualifier;
  private final SourcePosition sourcePosition;

  /**
   * Create a qualified wildcard without source position information.
   *
   * @param qualifier
   *          the qualifier associated with the wildcard, e.g. {@code table}
   */
  public QualifiedWildcard(String qualifier) {
    this(qualifier, null);
  }

  /**
   * Create a qualified wildcard that captures the originating source position.
   *
   * @param qualifier
   *          the qualifier associated with the wildcard, e.g. {@code table}
   * @param sourcePosition
   *          the position where the wildcard appears within the SQL text, may be
   *          {@code null}
   */
  public QualifiedWildcard(String qualifier, SourcePosition sourcePosition) {
    if (qualifier == null) {
      throw new IllegalArgumentException("Qualifier must not be null");
    }
    String trimmed = qualifier.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("Qualifier must not be blank");
    }
    this.qualifier = trimmed;
    this.sourcePosition = sourcePosition;
  }

  /**
   * Retrieve the qualifier associated with the wildcard.
   *
   * @return qualifier text such as {@code table} or {@code alias}
   */
  public String qualifier() {
    return qualifier;
  }

  @Override
  public Optional<SourcePosition> sourcePosition() {
    return Optional.ofNullable(sourcePosition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(qualifier, sourcePosition);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof QualifiedWildcard other) {
      return Objects.equals(qualifier, other.qualifier) && Objects.equals(sourcePosition, other.sourcePosition);
    }
    return false;
  }

  @Override
  public String toString() {
    return qualifier + ".*";
  }
}
