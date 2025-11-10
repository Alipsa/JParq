package se.alipsa.jparq.engine;

import java.util.Optional;

/**
 * Marker interface describing an element that appears in a SQL {@code SELECT}
 * list.
 */
public interface SelectListItem {

  /**
   * Provides the source position for the select item when available.
   *
   * @return an {@link Optional} describing the position, or
   *         {@link Optional#empty()} when unavailable
   */
  Optional<SourcePosition> sourcePosition();
}
