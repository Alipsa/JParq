package se.alipsa.jparq.engine;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Utility methods for handling qualifier-aware column mappings when resolving
 * canonical field names in multi-table queries.
 */
public final class ColumnMappingUtil {

  private ColumnMappingUtil() {
  }

  /**
   * Collects physical names and projection aliases eligible for outer fallback.
   * Inner schemas must still be checked before substituting any candidate.
   *
   * @param fieldIndex
   *          normalized names of physical fields
   * @param unqualified
   *          unqualified outer column mappings
   * @param qualified
   *          qualifier mappings, including projection aliases
   * @return normalized candidate column names
   */
  public static Set<String> correlationColumns(Map<String, String> fieldIndex, Map<String, String> unqualified,
      Map<String, Map<String, String>> qualified) {
    Set<String> names = new LinkedHashSet<>(fieldIndex.keySet());
    names.addAll(unqualified.keySet());
    qualified.values().forEach(mapping -> names.addAll(mapping.keySet()));
    return Set.copyOf(names);
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
   * @throws IllegalArgumentException
   *           if the SQL name is ambiguous
   */
  public static String canonicalFieldName(String qualifier, String columnName,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping,
      Map<String, String> caseInsensitiveIndex) {

    Objects.requireNonNull(columnName, "columnName");
    Objects.requireNonNull(caseInsensitiveIndex, "caseInsensitiveIndex");
    String key = normalizeLookupKey(columnName);
    if (key != null && !hasQualifier(qualifier) && "".equals(caseInsensitiveIndex.get(key))) {
      throw SqlExceptions.ambiguousColumn(columnName);
    }
    String canonical = resolveCanonicalFieldName(qualifier, columnName, qualifierColumnMapping,
        unqualifiedColumnMapping, caseInsensitiveIndex);
    // Correlation mappings can contain SQL references instead of physical fields.
    // Rebind a lowercase delimited target through its unquoted source key before
    // reading values when that name exists in the source schema. This includes
    // qualified joins and aggregate input expressions.
    Identifier target = Identifier.of(canonical);
    String sourceKey = target == null ? null : alternativeColumnKey(target.lookupKey());
    if (sourceKey != null && caseInsensitiveIndex.containsKey(sourceKey)) {
      canonical = resolveCanonicalFieldName(qualifier, target.text(), qualifierColumnMapping, unqualifiedColumnMapping,
          caseInsensitiveIndex);
    }
    if ("".equals(canonical)) {
      throw SqlExceptions.ambiguousColumn(columnName);
    }
    return canonical;
  }

  /**
   * Resolves qualified, unqualified, and fallback lookup paths.
   *
   * @param qualifier
   *          optional SQL qualifier
   * @param columnName
   *          SQL column identifier
   * @param qualifierColumnMapping
   *          mappings for qualified SQL names
   * @param unqualifiedColumnMapping
   *          mappings for unqualified SQL names
   * @param caseInsensitiveIndex
   *          fallback SQL name index
   * @return the resolved field or the original unresolved identifier
   */
  private static String resolveCanonicalFieldName(String qualifier, String columnName,
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
      throw SqlExceptions.ambiguousColumn(columnName);
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
    return canonicalOrderColumn(columnName, qualifier, qualifierColumnMapping, unqualifiedColumnMapping, columnName);
  }

  /**
   * Resolves an ordering or projection column, preserving an explicit fallback
   * only when no mapping exists.
   *
   * @param columnName
   *          SQL column identifier
   * @param qualifier
   *          optional table qualifier
   * @param qualifierColumnMapping
   *          normalized qualifier mappings, or null
   * @param unqualifiedColumnMapping
   *          normalized unqualified mappings, or null
   * @param fallback
   *          value to return when no mapping exists
   * @return the mapped field name, even if unchanged, or the fallback
   */
  public static String canonicalOrderColumn(String columnName, String qualifier,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping,
      String fallback) {
    if (columnName == null) {
      return null;
    }
    Map<String, Map<String, String>> qualifierMapping = (qualifierColumnMapping == null)
        ? Map.of()
        : qualifierColumnMapping;
    Map<String, String> unqualifiedMapping = (unqualifiedColumnMapping == null) ? Map.of() : unqualifiedColumnMapping;

    String normalizedColumn = normalizeLookupKey(columnName);
    if (normalizedColumn == null) {
      return fallback;
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

    return fallback;
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

    Map<String, String> mapping = qualifierColumns(qualifierColumnMapping, normalizedQualifier);
    if (mapping == null || mapping.isEmpty()) {
      String fallback = caseInsensitiveIndex.get(normalizedColumn);
      if (fallback == null) {
        String alternative = alternativeColumnKey(normalizedColumn);
        if (alternative != null) {
          fallback = caseInsensitiveIndex.get(alternative);
        }
      }
      if ("".equals(fallback)) {
        throw SqlExceptions
            .syntaxFailure("Unknown table qualifier '" + qualifier + "' for column '" + columnName + "'");
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
    Map<String, String> mapping = qualifierColumns(qualifierMapping, normalizedQualifier);
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

  /**
   * Looks up a qualifier, honoring exact quoted names before lowercase fallback.
   *
   * @param mappings
   *          normalized qualifier-to-column mappings
   * @param qualifier
   *          raw SQL qualifier or normalized lookup key
   * @return the qualifier's columns, or an empty map if it is unknown
   */
  public static Map<String, String> qualifierColumns(Map<String, Map<String, String>> mappings, String qualifier) {
    String key = normalizeLookupKey(qualifier);
    if (key == null) {
      return Map.of();
    }
    Map<String, String> columns = mappings.get(key);
    if (columns == null) {
      String alternative = alternativeColumnKey(key);
      if (alternative != null) {
        columns = mappings.get(alternative);
      }
    }
    return columns == null ? Map.of() : columns;
  }

  /**
   * Resolves a quoted lowercase identifier key to its unquoted equivalent.
   *
   * @param normalizedColumn
   *          normalized column or qualifier lookup key
   * @return the unquoted fallback key, or null when no fallback is allowed
   */
  public static String alternativeColumnKey(String normalizedColumn) {
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
