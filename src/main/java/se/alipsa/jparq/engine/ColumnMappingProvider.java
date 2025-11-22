package se.alipsa.jparq.engine;

import java.util.Map;

/**
 * Exposes qualifier-aware and unqualified column mappings for
 * {@link RecordReader} implementations that can assist with correlation and
 * name resolution.
 */
public interface ColumnMappingProvider {

  /**
   * Return the mapping from normalized qualifier to the normalized column name
   * mapping used for correlation.
   *
   * @return qualifier to column mapping
   */
  Map<String, Map<String, String>> qualifierColumnMapping();

  /**
   * Return the mapping from normalized column identifiers to their canonical
   * counterparts when the qualifier is omitted.
   *
   * @return mapping from normalized column name to canonical column identifier
   */
  Map<String, String> unqualifiedColumnMapping();
}
