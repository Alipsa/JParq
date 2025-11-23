package se.alipsa.jparq.engine;

import java.util.List;
import java.util.Map;

/**
 * Groups correlation-related mappings for {@link ValueExpressionEvaluator}.
 * This record encapsulates the parameters used to resolve qualified and
 * unqualified column references in subqueries and correlated expressions.
 *
 * @param outerQualifiers
 *          table names or aliases that belong to the outer query scope
 * @param qualifierColumnMapping
 *          mapping of qualifier (table/alias) to canonical column names used
 *          when resolving column references inside expressions
 * @param unqualifiedColumnMapping
 *          mapping of unqualified column names to canonical names for
 *          expressions referencing columns that are unique across all tables
 * @param correlationContext
 *          qualifier-aware correlation context used to resolve correlated
 *          columns in subqueries. This context is built from projection labels
 *          and canonical column names so that scalar and EXISTS subqueries can
 *          reference outer aliases even when they differ from the underlying
 *          field names.
 */
public record CorrelationMappings(List<String> outerQualifiers, Map<String, Map<String, String>> qualifierColumnMapping,
    Map<String, String> unqualifiedColumnMapping, Map<String, Map<String, String>> correlationContext) {

  /**
   * Creates an empty {@link CorrelationMappings} with no qualifiers or mappings.
   *
   * @return an empty correlation mappings instance
   */
  public static CorrelationMappings empty() {
    return new CorrelationMappings(List.of(), Map.of(), Map.of(), Map.of());
  }

  /**
   * Creates a {@link CorrelationMappings} with basic qualifier and column
   * mappings but no correlation context.
   *
   * @param outerQualifiers
   *          table names or aliases that belong to the outer query scope
   * @param qualifierColumnMapping
   *          mapping of qualifier (table/alias) to canonical column names
   * @param unqualifiedColumnMapping
   *          mapping of unqualified column names to canonical names
   * @return a correlation mappings instance with the specified mappings
   */
  public static CorrelationMappings of(List<String> outerQualifiers,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping) {
    return new CorrelationMappings(outerQualifiers, qualifierColumnMapping, unqualifiedColumnMapping, Map.of());
  }
}
