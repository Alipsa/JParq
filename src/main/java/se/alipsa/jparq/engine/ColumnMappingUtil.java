package se.alipsa.jparq.engine;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
    if (hasQualifier(qualifier)) {
      return resolveQualifiedReference(qualifier, columnName, normalizedColumn, qualifierColumnMapping,
          caseInsensitiveIndex);
    }

    if (!qualifierColumnMapping.isEmpty()) {
      String canonical = unqualifiedColumnMapping.get(normalizedColumn);
      if (canonical != null) {
        return canonical;
      }
      String insensitiveMatch = caseInsensitiveIndex.get(normalizedColumn);
      if (insensitiveMatch != null) {
        return insensitiveMatch;
      }
      String uniqueQualifierCandidate = resolveUniqueQualifierCandidate(normalizedColumn, qualifierColumnMapping);
      if (uniqueQualifierCandidate != null) {
        return uniqueQualifierCandidate;
      }
      Set<String> canonicalCandidates = new LinkedHashSet<>();
      for (Map<String, String> mapping : qualifierColumnMapping.values()) {
        String candidate = mapping.get(normalizedColumn);
        if (candidate != null) {
          canonicalCandidates.add(candidate);
        }
      }
      if (canonicalCandidates.size() == 1) {
        return canonicalCandidates.iterator().next();
      }
      if (!canonicalCandidates.isEmpty()) {
        throw new IllegalArgumentException("Ambiguous column reference '" + columnName + "'");
      }
      String fallback = caseInsensitiveIndex.get(normalizedColumn);
      if (fallback != null) {
        return fallback;
      }
      throw new IllegalArgumentException("Unknown column '" + columnName + "'");
    }

    String canonical = caseInsensitiveIndex.get(normalizedColumn);
    if (canonical != null) {
      return canonical;
    }
    throw new IllegalArgumentException("Unknown column '" + columnName + "'");
  }

  /**
   * Resolve a column reference when the qualifier is omitted by identifying a
   * single qualifier that defines the column.
   *
   * @param normalizedColumn
   *          the normalized lookup key for the column
   * @param qualifierColumnMapping
   *          normalized qualifier mappings supplied by the reader
   * @return the canonical column name if exactly one qualifier contains the
   *         column; otherwise {@code null}
   */
  private static String resolveUniqueQualifierCandidate(String normalizedColumn,
      Map<String, Map<String, String>> qualifierColumnMapping) {
    String candidate = null;
    int matchCount = 0;
    for (Map<String, String> mapping : qualifierColumnMapping.values()) {
      String canonical = mapping.get(normalizedColumn);
      if (canonical != null) {
        candidate = canonical;
        matchCount++;
        if (matchCount > 1) {
          return null;
        }
      }
    }
    return matchCount == 1 ? candidate : null;
  }

  /**
   * Resolve the canonical column name for ORDER BY or DISTINCT operations where
   * case-insensitive lookups are sufficient and missing mappings should fall back
   * to the original identifier. The unqualified mapping is consulted even when no
   * qualifier mappings are available to preserve case-insensitive single-table
   * lookups.
   *
   * @param columnName
   *          the column name to resolve (may include quoting)
   * @param qualifier
   *          the table qualifier, may be {@code null}
   * @param qualifierColumnMapping
   *          normalized qualifier mapping as produced by
   *          {@link #normaliseQualifierMapping(Map)}; may be {@code null}
   * @param unqualifiedColumnMapping
   *          normalized unqualified mapping as produced by
   *          {@link #normaliseUnqualifiedMapping(Map)}; may be {@code null}
   * @return the canonical column name if a mapping exists, otherwise the original
   *         {@code columnName}
   */
  public static String canonicalOrderColumn(String columnName, String qualifier,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping) {
    if (columnName == null) {
      return null;
    }
    Map<String, Map<String, String>> qualifierMapping = (qualifierColumnMapping == null)
        ? Map.of()
        : qualifierColumnMapping;
    Map<String, String> unqualifiedMapping = (unqualifiedColumnMapping == null) ? Map.of() : unqualifiedColumnMapping;

    String normalizedColumn = normalizeColumnKey(columnName);

    if (qualifier != null && !qualifier.isBlank() && !qualifierMapping.isEmpty()) {
      String normalizedQualifier = JParqUtil.normalizeQualifier(qualifier);
      if (normalizedQualifier != null) {
        Map<String, String> mapping = qualifierMapping.getOrDefault(normalizedQualifier, Map.of());
        String canonical = mapping.get(normalizedColumn);
        if (canonical != null) {
          return canonical;
        }
      }
    }

    String canonical = unqualifiedMapping.get(normalizedColumn);
    if (canonical != null) {
      return canonical;
    }

    return columnName;
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

  /**
   * Determine whether the provided qualifier should participate in the resolution
   * process.
   *
   * @param qualifier
   *          optional table qualifier from the SQL statement
   * @return {@code true} when the qualifier contains non-blank characters
   */
  private static boolean hasQualifier(String qualifier) {
    return qualifier != null && !qualifier.isBlank();
  }

  /**
   * Resolve a column reference that includes a table qualifier.
   *
   * @param qualifier
   *          the qualifier provided by the SQL statement
   * @param columnName
   *          the original column identifier
   * @param normalizedColumn
   *          the normalized lookup key for {@code columnName}
   * @param qualifierColumnMapping
   *          normalized qualifier mapping supplied by the reader
   * @param caseInsensitiveIndex
   *          fallback mapping used for single-table queries
   * @return the canonical field name for the column
   */
  private static String resolveQualifiedReference(String qualifier, String columnName, String normalizedColumn,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> caseInsensitiveIndex) {
    String normalizedQualifier = JParqUtil.normalizeQualifier(qualifier);
    if (normalizedQualifier == null) {
      throw new IllegalArgumentException("Unknown column '" + columnName + "' for qualifier '" + qualifier + "'");
    }

    Map<String, String> mapping = qualifierColumnMapping.get(normalizedQualifier);
    if (mapping == null || mapping.isEmpty()) {
      String fallback = caseInsensitiveIndex.get(normalizedColumn);
      if (fallback != null) {
        return fallback;
      }
      throw new IllegalArgumentException("Unknown column '" + columnName + "' for qualifier '" + qualifier + "'");
    }

    String canonical = mapping.get(normalizedColumn);
    if (canonical != null) {
      return canonical;
    }
    throw new IllegalArgumentException("Unknown column '" + columnName + "' for qualifier '" + qualifier + "'");
  }
}
