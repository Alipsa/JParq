package se.alipsa.jparq.engine;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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

    String normalizedColumn = normalizeLookupKey(columnName);
    if (normalizedColumn == null) {
      return columnName;
    }
    if (hasQualifier(qualifier)) {
      return resolveQualifiedReference(qualifier, columnName, normalizedColumn, qualifierColumnMapping,
          caseInsensitiveIndex);
    }

    if (!qualifierColumnMapping.isEmpty()) {
      String canonical = unqualifiedColumnMapping.get(normalizedColumn);
      if (canonical == null) {
        String alternative = alternativeColumnKey(normalizedColumn);
        if (alternative != null) {
          canonical = unqualifiedColumnMapping.get(alternative);
        }
      }
      if (canonical != null) {
        return canonical;
      }
      canonical = resolveUnqualified(normalizedColumn, qualifierColumnMapping, caseInsensitiveIndex);
      if (canonical == null) {
        String alternative = alternativeColumnKey(normalizedColumn);
        if (alternative != null) {
          canonical = resolveUnqualified(alternative, qualifierColumnMapping, caseInsensitiveIndex);
        }
      }
      if (canonical != null) {
        return canonical;
      }
      throw new IllegalArgumentException("Ambiguous column reference: " + columnName);
    }

    String canonical = caseInsensitiveIndex.get(normalizedColumn);
    if (canonical == null) {
      String alternative = alternativeColumnKey(normalizedColumn);
      if (alternative != null) {
        canonical = caseInsensitiveIndex.get(alternative);
      }
    }
    return canonical != null ? canonical : columnName;
  }

  private static String resolveUnqualified(String normalizedColumn,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> caseInsensitiveIndex) {
    String candidate = null;
    for (Map<String, String> mapping : qualifierColumnMapping.values()) {
      if (mapping == null || mapping.isEmpty()) {
        continue;
      }
      String canonical = mapping.get(normalizedColumn);
      if (canonical == null) {
        continue;
      }
      if (candidate != null && !candidate.equals(canonical)) {
        return null;
      }
      candidate = canonical;
    }
    if (candidate != null) {
      return candidate;
    }
    return caseInsensitiveIndex.get(normalizedColumn);
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

    String normalizedColumn = normalizeLookupKey(columnName);
    if (normalizedColumn == null) {
      return columnName;
    }

    if (qualifier != null && !qualifier.isBlank() && !qualifierMapping.isEmpty()) {
      String canonical = resolveQualifiedOrderColumn(normalizedColumn, qualifier, qualifierMapping);
      if (canonical != null) {
        return canonical;
      }
    }

    String canonical = unqualifiedMapping.get(normalizedColumn);
    if (canonical == null) {
      String alternative = alternativeColumnKey(normalizedColumn);
      if (alternative != null) {
        canonical = unqualifiedMapping.get(alternative);
      }
    }
    if (canonical != null) {
      return canonical;
    }

    return columnName;
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
      String qualifier = normalizeLookupKey(entry.getKey());
      if (qualifier == null) {
        continue;
      }
      Map<String, String> inner = new HashMap<>();
      Map<String, String> value = entry.getValue();
      if (value != null) {
        for (Map.Entry<String, String> innerEntry : value.entrySet()) {
          String columnKey = normalizeLookupKey(innerEntry.getKey());
          if (columnKey != null) {
            inner.put(columnKey, innerEntry.getValue());
          }
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
      String key = normalizeLookupKey(entry.getKey());
      if (key != null) {
        normalized.put(key, entry.getValue());
      }
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
    String normalizedQualifier = normalizeLookupKey(qualifier);
    if (normalizedQualifier == null) {
      throw new IllegalArgumentException("Unknown column '" + columnName + "' for qualifier '" + qualifier + "'");
    }

    Map<String, String> mapping = qualifierColumnMapping.get(normalizedQualifier);
    if (mapping == null || mapping.isEmpty()) {
      String fallback = caseInsensitiveIndex.get(normalizedColumn);
      if (fallback == null) {
        String alternative = alternativeColumnKey(normalizedColumn);
        if (alternative != null) {
          fallback = caseInsensitiveIndex.get(alternative);
        }
      }
      if (fallback != null) {
        return fallback;
      }
      throw new IllegalArgumentException("Unknown column '" + columnName + "' for qualifier '" + qualifier
          + "' (available columns: " + caseInsensitiveIndex.keySet() + ")");
    }

    String canonical = mapping.get(normalizedColumn);
    if (canonical == null) {
      String alternative = alternativeColumnKey(normalizedColumn);
      if (alternative != null) {
        canonical = mapping.get(alternative);
      }
    }
    if (canonical != null) {
      return canonical;
    }
    throw new IllegalArgumentException("Unknown column '" + columnName + "' for qualifier '" + qualifier
        + "' (available columns: " + mapping.keySet() + ")");
  }

  /**
   * Resolve the canonical column for ORDER BY/DISTINCT clauses where a qualifier
   * is present.
   *
   * @param normalizedColumn
   *          the normalized column lookup key
   * @param qualifier
   *          the qualifier provided in the SQL statement
   * @param qualifierMapping
   *          normalized qualifier mapping supplied by the reader
   * @return the canonical column name or {@code null} when not found
   */
  private static String resolveQualifiedOrderColumn(String normalizedColumn, String qualifier,
      Map<String, Map<String, String>> qualifierMapping) {
    String normalizedQualifier = normalizeLookupKey(qualifier);
    if (normalizedQualifier == null) {
      return null;
    }
    Map<String, String> mapping = qualifierMapping.getOrDefault(normalizedQualifier, Map.of());
    if (mapping.isEmpty()) {
      return null;
    }
    String canonical = mapping.get(normalizedColumn);
    if (canonical != null) {
      return canonical;
    }
    String alternative = alternativeColumnKey(normalizedColumn);
    if (alternative == null) {
      return null;
    }
    return mapping.get(alternative);
  }

  private static boolean looksLikeLookupKey(String value) {
    if (value == null || value.length() < 2) {
      return false;
    }
    char prefix = Character.toUpperCase(value.charAt(0));
    return (prefix == 'U' || prefix == 'Q') && value.charAt(1) == ':';
  }

  private static String normalizeLookupKey(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if (looksLikeLookupKey(trimmed)) {
      String prefix = trimmed.substring(0, 2).toUpperCase(Locale.ROOT);
      String body = trimmed.substring(2);
      while (looksLikeLookupKey(body)) {
        body = body.substring(2);
      }
      if ("U:".equals(prefix)) {
        return "U:" + body.toLowerCase(Locale.ROOT);
      }
      if ("Q:".equals(prefix)) {
        return "Q:" + body;
      }
      return null;
    }
    return Identifier.lookupKey(trimmed);
  }

  private static String alternativeColumnKey(String normalizedColumn) {
    if (normalizedColumn == null || !normalizedColumn.startsWith("Q:") || normalizedColumn.length() <= 2) {
      return null;
    }
    String body = normalizedColumn.substring(2);
    String lowerCase = body.toLowerCase(Locale.ROOT);
    if (!body.equals(lowerCase)) {
      return null;
    }
    return "U:" + lowerCase;
  }
}
