package se.alipsa.jparq.engine;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Hash-based DISTINCT. Preserves input order when there is no ORDER BY. Apply
 * after WHERE and projection, before ORDER BY and LIMIT.
 */
public final class DistinctOperator {

  /**
   * Deduplicate rows based on value equality across all projected columns.
   *
   * @param rows
   *          the projected rows
   * @return the deduplicated rows
   */
  public List<Object[]> apply(List<Object[]> rows) {
    if (rows == null || rows.isEmpty()) {
      return rows;
    }
    Set<RowKey> set = new LinkedHashSet<>(rows.size() * 2);
    for (Object[] row : rows) {
      set.add(RowKey.of(row));
    }
    return set.stream().map(RowKey::row).toList();
  }
}
