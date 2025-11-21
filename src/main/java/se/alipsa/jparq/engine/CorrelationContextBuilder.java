package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import se.alipsa.jparq.helper.JParqUtil;

/**
 * Utility for constructing qualifier-aware correlation contexts from projected
 * columns. The resulting mapping associates each visible qualifier (table name
 * or alias) with the canonical column names available in the current row,
 * allowing correlated sub queries to resolve aliases accurately.
 */
public final class CorrelationContextBuilder {

  private CorrelationContextBuilder() {
  }

  /**
   * Build a correlation mapping for the supplied qualifiers. Existing mappings
   * are preserved, and any projected columns missing from the mapping are added
   * using their canonical counterparts.
   *
   * @param qualifiers
   *          qualifiers (tables or aliases) visible to the current query
   * @param columnLabels
   *          labels exposed by the projection in result-set order
   * @param canonicalColumnNames
   *          canonical names corresponding to {@code columnLabels}; falls back to
   *          {@code columnLabels} when {@code null}
   * @param existingMapping
   *          qualifier to canonical column mapping provided by the reader (may be
   *          {@code null})
   * @return a normalized mapping containing all projected columns for each
   *         qualifier
   */
  public static Map<String, Map<String, String>> build(List<String> qualifiers, List<String> columnLabels,
      List<String> canonicalColumnNames, Map<String, Map<String, String>> existingMapping) {

    List<String> effectiveLabels = columnLabels == null ? List.of() : new ArrayList<>(columnLabels);
    List<String> canonicalColumns = canonicalColumnNames == null
        ? effectiveLabels
        : new ArrayList<>(canonicalColumnNames);

    Map<String, Map<String, String>> normalizedExisting = ColumnMappingUtil.normaliseQualifierMapping(existingMapping);
    Map<String, Map<String, String>> merged = new LinkedHashMap<>(normalizedExisting);

    if (qualifiers == null || qualifiers.isEmpty() || effectiveLabels.isEmpty()) {
      return merged.isEmpty() ? Map.of() : Map.copyOf(merged);
    }

    Map<String, String> projectionMapping = new LinkedHashMap<>();
    int limit = Math.min(effectiveLabels.size(), canonicalColumns.size());
    for (int i = 0; i < limit; i++) {
      String label = effectiveLabels.get(i);
      if (label == null || label.isBlank()) {
        continue;
      }
      String canonical = canonicalColumns.get(i);
      if (canonical == null || canonical.isBlank()) {
        canonical = label;
      }
      String normalizedLabel = label.toLowerCase(Locale.ROOT);
      String normalizedCanonical = canonical.toLowerCase(Locale.ROOT);
      projectionMapping.put(normalizedLabel, canonical);
      projectionMapping.putIfAbsent(normalizedCanonical, canonical);
    }

    for (String qualifier : qualifiers) {
      String normalizedQualifier = JParqUtil.normalizeQualifier(qualifier);
      if (normalizedQualifier == null) {
        continue;
      }
      Map<String, String> existing = merged.getOrDefault(normalizedQualifier, Map.of());
      Map<String, String> updated = new LinkedHashMap<>(existing);
      for (Map.Entry<String, String> entry : projectionMapping.entrySet()) {
        updated.putIfAbsent(entry.getKey(), entry.getValue());
      }
      merged.put(normalizedQualifier, Map.copyOf(updated));
    }
    return Map.copyOf(merged);
  }
}
