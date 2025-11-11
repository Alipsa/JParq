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
  private final int selectListIndex;

  /**
   * Create a qualified wildcard without source position information.
   *
   * @param qualifier
   *          the qualifier associated with the wildcard, e.g. {@code table}
   */
  public QualifiedWildcard(String qualifier) {
    this(qualifier, null, -1);
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
    this(qualifier, sourcePosition, -1);
  }

  /**
   * Create a qualified wildcard that captures the originating source position
   * together with the index within the SELECT list.
   *
   * @param qualifier
   *          the qualifier associated with the wildcard, e.g. {@code table}
   * @param sourcePosition
   *          the position where the wildcard appears within the SQL text, may be
   *          {@code null}
   * @param selectListIndex
   *          zero-based index describing where in the SELECT list this wildcard
   *          appeared. Use {@code -1} when the index is unknown
   */
  public QualifiedWildcard(String qualifier, SourcePosition sourcePosition, int selectListIndex) {
    if (qualifier == null) {
      throw new IllegalArgumentException("Qualifier must not be null");
    }
    String trimmed = qualifier.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("Qualifier must not be blank");
    }
    this.qualifier = trimmed;
    this.sourcePosition = sourcePosition;
    this.selectListIndex = selectListIndex;
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

  /**
   * Retrieve the zero-based index describing where the wildcard appeared in the
   * SELECT list.
   *
   * @return the index within the SELECT list, or {@code -1} when unavailable
   */
  public int selectListIndex() {
    return selectListIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(qualifier, sourcePosition, selectListIndex);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof QualifiedWildcard other) {
      return Objects.equals(qualifier, other.qualifier) && Objects.equals(sourcePosition, other.sourcePosition)
          && selectListIndex == other.selectListIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return qualifier + ".*";
  }
}
