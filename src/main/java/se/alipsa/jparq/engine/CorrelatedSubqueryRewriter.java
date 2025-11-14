package se.alipsa.jparq.engine;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import se.alipsa.jparq.helper.JParqUtil;

/**
 * Utility that rewrites sub queries containing correlated column references by
 * substituting the column with the literal value sourced from the outer query
 * context.
 */
public final class CorrelatedSubqueryRewriter {

  private CorrelatedSubqueryRewriter() {
  }

  /**
   * Result from attempting to rewrite a correlated sub query.
   *
   * @param sql
   *          the SQL text to execute (possibly unchanged)
   * @param correlated
   *          {@code true} when at least one correlated column was substituted
   * @param correlatedColumns
   *          column names (unqualified) discovered to be correlated with the
   *          outer query
   * @param correlatedReferences
   *          qualified column references that were replaced when rewriting the
   *          sub query
   */
  public static record Result(String sql, boolean correlated, Set<String> correlatedColumns,
      Set<QualifiedColumn> correlatedReferences) {
  }

  /**
   * Representation of a column discovered to reference the outer query scope
   * during correlated sub query rewriting.
   *
   * @param qualifier
   *          normalized table qualifier associated with the column
   * @param column
   *          column name referenced from the outer query
   */
  public static record QualifiedColumn(String qualifier, String column) {
  }

  /**
   * Rewrite the supplied {@link Select} so that correlated column references
   * (columns qualified with an outer query alias) are replaced with literal
   * values provided by {@code valueResolver}.
   *
   * @param select
   *          the sub query to inspect
   * @param outerQualifiers
   *          names (table or alias) that belong to the outer query scope
   * @param valueResolver
   *          function mapping a column name to the value from the current outer
   *          row
   * @return a {@link Result} describing the rewritten SQL and correlated column
   *         usage
   */
  public static Result rewrite(Select select, Collection<String> outerQualifiers,
      Function<String, Object> valueResolver) {
    Objects.requireNonNull(valueResolver, "valueResolver");
    return rewrite(select, outerQualifiers,
        (qualifier, column) -> valueResolver.apply(column));
  }

  /**
   * Rewrite the supplied {@link Select} so that correlated column references
   * (columns qualified with an outer query alias) are replaced with literal
   * values provided by {@code valueResolver}.
   *
   * @param select
   *          the sub query to inspect
   * @param outerQualifiers
   *          names (table or alias) that belong to the outer query scope
   * @param valueResolver
   *          function mapping a normalized qualifier and column name to the
   *          value from the current outer row
   * @return a {@link Result} describing the rewritten SQL and correlated column
   *         usage
   */
  public static Result rewrite(Select select, Collection<String> outerQualifiers,
      BiFunction<String, String, Object> valueResolver) {
    Objects.requireNonNull(select, "select");
    Objects.requireNonNull(valueResolver, "valueResolver");
    if (outerQualifiers == null || outerQualifiers.isEmpty()) {
      return new Result(select.toString(), false, Set.of(), Set.of());
    }

    Set<String> normalized = outerQualifiers.stream().map(JParqUtil::normalizeQualifier)
        .filter(Objects::nonNull).collect(Collectors.toUnmodifiableSet());

    if (normalized.isEmpty()) {
      return new Result(select.toString(), false, Set.of(), Set.of());
    }

    StringBuilder buffer = new StringBuilder();
    AtomicBoolean correlated = new AtomicBoolean(false);
    Set<String> correlatedColumns = new LinkedHashSet<>();
    Set<QualifiedColumn> correlatedRefs = new LinkedHashSet<>();

    ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
      @Override
      public <S> StringBuilder visit(Column column, S context) {
        Table table = column.getTable();
        if (table != null) {
          String[] candidates = {
              table.getUnquotedName(),
              table.getFullyQualifiedName(),
              table.getName()
          };
          for (String candidate : candidates) {
            String normalizedCandidate = JParqUtil.normalizeQualifier(candidate);
            if (normalizedCandidate != null && normalized.contains(normalizedCandidate)) {
              final Object resolvedValue = valueResolver.apply(normalizedCandidate, column.getColumnName());
              final String literal = toSqlLiteral(resolvedValue);
              correlated.set(true);
              correlatedColumns.add(column.getColumnName());
              correlatedRefs.add(new QualifiedColumn(normalizedCandidate, column.getColumnName()));
              getBuilder().append(literal);
              return getBuilder();
            }
          }
        }
        return super.visit(column, context);
      }
    };

    SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, buffer);
    expressionDeParser.setSelectVisitor(selectDeParser);
    expressionDeParser.setBuilder(buffer);
    selectDeParser.setBuilder(buffer);

    select.accept((SelectVisitor<StringBuilder>) selectDeParser, null);

    String sqlText = buffer.toString();
    if (select instanceof ParenthesedSelect) {
      sqlText = "(" + sqlText + ")";
    }
    return new Result(sqlText, correlated.get(), Set.copyOf(correlatedColumns), Set.copyOf(correlatedRefs));
  }

  private static String toSqlLiteral(Object value) {
    Expression literal = toLiteralExpression(value);
    return literal.toString();
  }

  private static Expression toLiteralExpression(Object value) {
    if (value == null) {
      return new NullValue();
    }
    if (value instanceof Boolean bool) {
      return new BooleanValue(Boolean.TRUE.equals(bool) ? "TRUE" : "FALSE");
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      return new LongValue(value.toString());
    }
    if (value instanceof Float || value instanceof Double) {
      return new DoubleValue(value.toString());
    }
    if (value instanceof java.math.BigDecimal bd) {
      return new DoubleValue(bd.toPlainString());
    }
    if (value instanceof java.math.BigInteger bi) {
      return new LongValue(bi.toString());
    }
    if (value instanceof java.sql.Timestamp ts) {
      return new TimestampValue("'" + ts.toString() + "'");
    }
    if (value instanceof java.time.LocalDateTime ldt) {
      return new TimestampValue("'" + ldt.toString() + "'");
    }
    if (value instanceof java.sql.Date date) {
      return new DateValue("'" + date.toString() + "'");
    }
    if (value instanceof java.time.LocalDate ld) {
      return new DateValue("'" + ld.toString() + "'");
    }
    if (value instanceof java.sql.Time time) {
      return new TimeValue("'" + time.toString() + "'");
    }
    if (value instanceof java.time.LocalTime lt) {
      return new TimeValue("'" + lt.toString() + "'");
    }
    String text = value.toString().replace("'", "''");
    return new StringValue("'" + text + "'");
  }
}
