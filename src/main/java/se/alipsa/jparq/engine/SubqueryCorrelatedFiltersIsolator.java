package se.alipsa.jparq.engine;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import net.sf.jsqlparser.statement.select.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.alipsa.jparq.helper.JParqUtil;

/**
 * Utility that centralizes correlated subquery rewriting while retaining the
 * qualifier-aware correlation context. The isolator merges the caller-provided
 * outer qualifiers with any qualifiers discovered in the correlation context so
 * that derived table aliases remain available during predicate rewriting. It
 * also emits debug diagnostics describing the effective context supplied to the
 * subquery evaluator.
 */
public final class SubqueryCorrelatedFiltersIsolator {

  private static final Logger log = LoggerFactory.getLogger(SubqueryCorrelatedFiltersIsolator.class);

  private SubqueryCorrelatedFiltersIsolator() {
  }

  /**
   * Rewrite the supplied subquery, substituting correlated predicates using the
   * provided value resolver while preserving derived aliases captured in the
   * correlation context.
   *
   * @param subSelect
   *          the subquery to rewrite
   * @param outerQualifiers
   *          qualifiers visible to the outer query
   * @param correlatedColumns
   *          correlated column names discovered by the caller
   * @param correlationContext
   *          qualifier-aware column mapping built for the outer query
   * @param valueResolver
   *          function resolving correlated values by qualifier and column name
   * @return the {@link CorrelatedSubqueryRewriter.Result} describing the
   *         rewritten SQL and correlated references
   */
  public static CorrelatedSubqueryRewriter.Result isolate(Select subSelect,
      Collection<String> outerQualifiers, Set<String> correlatedColumns,
      Map<String, Map<String, String>> correlationContext,
      BiFunction<String, String, Object> valueResolver) {

    Map<String, Map<String, String>> normalizedContext = ColumnMappingUtil
        .normaliseQualifierMapping(correlationContext);
    Set<String> effectiveQualifiers = new LinkedHashSet<>();
    if (outerQualifiers != null) {
      for (String qualifier : outerQualifiers) {
        String normalized = JParqUtil.normalizeQualifier(qualifier);
        if (normalized != null) {
          effectiveQualifiers.add(normalized);
        }
      }
    }
    effectiveQualifiers.addAll(normalizedContext.keySet());

    CorrelatedSubqueryRewriter.Result rewritten = CorrelatedSubqueryRewriter
        .rewrite(subSelect, effectiveQualifiers, correlatedColumns, valueResolver);

    if (log.isDebugEnabled()) {
      log.debug(
          "Correlated rewrite for {} with qualifiers {} and context {} produced SQL {} and references {}",
          subSelect, effectiveQualifiers, normalizedContext, rewritten.sql(), rewritten.correlatedReferences());
    }
    return rewritten;
  }
}
