package se.alipsa.jparq.engine;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import se.alipsa.jparq.helper.JParqUtil;

/**
 * Utility methods for handling qualifier-aware column mappings when resolving
 * canonical field names in multi-table queries.
 */
public final class ColumnMappingUtil {

  private ColumnMappingUtil() {
  }

  /**
   * Resolve the canonical field name for the provided column reference.
   *
   * @param qualifier
   *          the table qualifier, may be {@code null}
   * @param columnName
   *          the column name to resolve
   * @param qualifierColumnMapping
   *          a map of normalized qualifier to column name mappings
   * @param unqualifiedColumnMapping
   *          a map of unqualified column names to canonical names
   * @param caseInsensitiveIndex
   *          a fallback case-insensitive index for single-table queries
   * @return the canonical field name to use when reading the record
   */
  public static String canonicalFieldName(String qualifier, String columnName,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping,
      Map<String, String> caseInsensitiveIndex) {

    Objects.requireNonNull(columnName, "columnName");
    Objects.requireNonNull(caseInsensitiveIndex, "caseInsensitiveIndex");

    String normalizedColumn = normalizeColumnKey(columnName);
    if (qualifier != null && !qualifier.isBlank()) {
      String normalizedQualifier = JParqUtil.normalizeQualifier(qualifier);
      if (normalizedQualifier != null) {
        Map<String, String> mapping = qualifierColumnMapping.getOrDefault(normalizedQualifier, Map.of());
        String canonical = mapping.get(normalizedColumn);
        if (canonical != null) {
          return canonical;
        }
      }
      throw new IllegalArgumentException("Unknown column '" + columnName + "' for qualifier '" + qualifier + "'");
    }
    if (!qualifierColumnMapping.isEmpty()) {
      String canonical = unqualifiedColumnMapping.get(normalizedColumn);
      if (canonical != null) {
        return canonical;
      }
      throw new IllegalArgumentException("Ambiguous column reference: " + columnName);
    }
    String canonical = caseInsensitiveIndex.get(normalizedColumn);
    return canonical != null ? canonical : columnName;
  }

  /**
   * Normalize a column identifier for lookup in the qualifier or case-insensitive
   * indices by stripping optional quoting characters and applying lower-case
   * semantics.
   *
   * @param columnName
   *          column identifier provided in the SQL expression
   * @return normalized lookup key, or {@code null} when the input is null
   */
  private static String normalizeColumnKey(String columnName) {
    if (columnName == null) {
      return null;
    }
    String normalized = JParqUtil.normalizeQualifier(columnName);
    if (normalized != null) {
      return normalized;
    }
    return columnName.toLowerCase(Locale.ROOT);
  }

  /**
   * Normalizes the qualifier mapping so lookups can be performed with
   * case-insensitive qualifiers and column names.
   *
   * @param source
   *          the raw qualifier mapping from the join reader
   * @return an immutable map using normalized qualifiers and column names
   */
  public static Map<String, Map<String, String>> normaliseQualifierMapping(Map<String, Map<String, String>> source) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    Map<String, Map<String, String>> normalized = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
      String qualifier = JParqUtil.normalizeQualifier(entry.getKey());
      if (qualifier == null) {
        continue;
      }
      Map<String, String> inner = new HashMap<>();
      Map<String, String> value = entry.getValue();
      if (value != null) {
        for (Map.Entry<String, String> innerEntry : value.entrySet()) {
          inner.put(innerEntry.getKey().toLowerCase(Locale.ROOT), innerEntry.getValue());
        }
      }
      normalized.put(qualifier, Map.copyOf(inner));
    }
    return Map.copyOf(normalized);
  }

  /**
   * Normalizes unqualified column mappings to ensure case-insensitive lookups.
   *
   * @param source
   *          the raw mapping provided by the join reader
   * @return an immutable map keyed by lower-cased column names
   */
  public static Map<String, String> normaliseUnqualifiedMapping(Map<String, String> source) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    Map<String, String> normalized = new HashMap<>();
    for (Map.Entry<String, String> entry : source.entrySet()) {
      normalized.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
    }
    return Map.copyOf(normalized);
  }
}
