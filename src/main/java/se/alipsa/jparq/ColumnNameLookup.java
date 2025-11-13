package se.alipsa.jparq;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Utility helpers for resolving column names within result set metadata and data access paths.
 */
final class ColumnNameLookup {

  private ColumnNameLookup() {
  }

  /**
   * Resolve the canonical column name for the supplied column index.
   *
   * <p>
   * Canonical names are preferred when provided, followed by the underlying physical column names and finally the
   * projected labels exposed to the caller.
   * </p>
   *
   * @param canonicalNames
   *          canonical column names describing the schema (may be {@code null})
   * @param physicalNames
   *          physical column names mapped to the source table (may be {@code null})
   * @param labels
   *          projection labels (aliases) exposed to callers (may be {@code null})
   * @param column
   *          the 1-based column index
   * @return the canonical column name when available, otherwise {@code null}
   */
  static String canonicalName(
      List<String> canonicalNames, List<String> physicalNames, List<String> labels, int column) {
    int index = column - 1;
    if (index < 0) {
      return null;
    }
    String canonical = valueAt(canonicalNames, index);
    if (canonical != null) {
      return canonical;
    }
    String physical = valueAt(physicalNames, index);
    if (physical != null) {
      return physical;
    }
    return valueAt(labels, index);
  }

  /**
   * Build a case-insensitive lookup map for the supplied column metadata.
   *
   * @param columnCount
   *          the number of columns in the result set
   * @param canonicalNames
   *          canonical column names describing the schema (may be {@code null})
   * @param physicalNames
   *          physical column names mapped to the source table (may be {@code null})
   * @param labels
   *          projection labels (aliases) exposed to callers (may be {@code null})
   * @return an immutable map associating lower-cased column names to their 1-based column index
   */
  static Map<String, Integer> buildCaseInsensitiveIndex(
      int columnCount, List<String> canonicalNames, List<String> physicalNames, List<String> labels) {
    if (columnCount <= 0) {
      return Map.of();
    }
    Map<String, Integer> index = new LinkedHashMap<>();
    for (int i = 0; i < columnCount; i++) {
      int columnIndex = i + 1;
      addName(index, canonicalNames, i, columnIndex);
      addName(index, physicalNames, i, columnIndex);
      addName(index, labels, i, columnIndex);
    }
    return index.isEmpty() ? Map.of() : Collections.unmodifiableMap(index);
  }

  /**
   * Normalize a column name for case-insensitive lookups.
   *
   * @param name
   *          the column name to normalize (may be {@code null})
   * @return the normalized key, or an empty string when {@code name} is {@code null} or blank
   */
  static String normalizeKey(String name) {
    if (name == null) {
      return "";
    }
    String trimmed = name.trim();
    if (trimmed.isEmpty()) {
      return "";
    }
    return trimmed.toLowerCase(Locale.ROOT);
  }

  private static void addName(Map<String, Integer> index, List<String> names, int position, int columnIndex) {
    if (names == null || position < 0 || position >= names.size()) {
      return;
    }
    String value = names.get(position);
    if (value == null) {
      return;
    }
    String key = normalizeKey(value);
    if (!key.isEmpty()) {
      index.putIfAbsent(key, columnIndex);
    }
  }

  private static String valueAt(List<String> values, int index) {
    if (values == null || index < 0 || index >= values.size()) {
      return null;
    }
    String value = values.get(index);
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
