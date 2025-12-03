package se.alipsa.jparq.engine;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  public record Result(String sql, boolean correlated, Set<String> correlatedColumns,
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
  public record QualifiedColumn(String qualifier, String column) {
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
    return rewrite(select, outerQualifiers, (qualifier, column) -> valueResolver.apply(column));
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
   *          function mapping a normalized qualifier and column name to the value
   *          from the current outer row
   * @return a {@link Result} describing the rewritten SQL and correlated column
   *         usage
   */
  public static Result rewrite(Select select, Collection<String> outerQualifiers,
      BiFunction<String, String, Object> valueResolver) {
    return rewrite(select, outerQualifiers, Set.of(), valueResolver);
  }

  /**
   * Rewrite the supplied {@link Select} so that correlated column references are
   * replaced with literal values provided by {@code valueResolver}, including
   * unqualified columns present in the supplied correlation set.
   *
   * @param select
   *          the sub query to inspect
   * @param outerQualifiers
   *          names (table or alias) that belong to the outer query scope
   * @param correlatedColumns
   *          unqualified column names that should be treated as correlated when
   *          encountered in the sub query
   * @param valueResolver
   *          function mapping a normalized qualifier and column name to the value
   *          from the current outer row
   * @return a {@link Result} describing the rewritten SQL and correlated column
   *         usage
   */
  public static Result rewrite(Select select, Collection<String> outerQualifiers, Set<String> correlatedColumns,
      BiFunction<String, String, Object> valueResolver) {
    Objects.requireNonNull(select, "select");
    Objects.requireNonNull(valueResolver, "valueResolver");
    if (outerQualifiers == null || outerQualifiers.isEmpty()) {
      return new Result(select.toString(), false, Set.of(), Set.of());
    }

    Set<String> normalized = outerQualifiers.stream().map(CorrelatedSubqueryRewriter::normalizeIdentifier)
        .filter(Objects::nonNull).collect(Collectors.toUnmodifiableSet());

    Set<String> unqualified = correlatedColumns == null
        ? Set.of()
        : correlatedColumns.stream().filter(Objects::nonNull).map(CorrelatedSubqueryRewriter::normalizeIdentifier)
            .collect(Collectors.toUnmodifiableSet());

    if (normalized.isEmpty()) {
      return new Result(select.toString(), false, Set.of(), Set.of());
    }

    StringBuilder buffer = new StringBuilder();
    AtomicBoolean correlated = new AtomicBoolean(false);
    Set<String> correlatedColumnRefs = new LinkedHashSet<>();
    Set<QualifiedColumn> correlatedRefs = new LinkedHashSet<>();

    ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
      @Override
      public <S> StringBuilder visit(Column column, S context) {
        Table table = column.getTable();
        if (inlineQualifiedFromColumnIdentifier(column)) {
          return getBuilder();
        }
        if (table != null && inlineQualifiedFromTable(column, table)) {
          return getBuilder();
        }
        if (inlineUnqualifiedColumn(column)) {
          return getBuilder();
        }
        return super.visit(column, context);
      }

      private boolean inlineQualifiedFromColumnIdentifier(Column column) {
        if (column.getTable() != null) {
          return false;
        }
        String colName = column.getColumnName();
        if (colName == null) {
          return false;
        }
        int dot = colName.indexOf('.');
        if (dot <= 0 || dot + 1 >= colName.length()) {
          return false;
        }
        String qualifierPart = colName.substring(0, dot);
        String columnPart = colName.substring(dot + 1);
        String normalizedQualifier = normalizeIdentifier(qualifierPart);
        if (normalizedQualifier == null || !normalized.contains(normalizedQualifier)) {
          return false;
        }
        appendCorrelatedLiteral(normalizedQualifier, columnPart, valueResolver.apply(normalizedQualifier, columnPart));
        return true;
      }

      private boolean inlineQualifiedFromTable(Column column, Table table) {
        String[] candidates = {
            table.getUnquotedName(), table.getFullyQualifiedName(), table.getName()
        };
        for (String candidate : candidates) {
          String normalizedCandidate = normalizeIdentifier(candidate);
          if (normalizedCandidate != null && normalized.contains(normalizedCandidate)) {
            appendCorrelatedLiteral(normalizedCandidate, column.getColumnName(),
                valueResolver.apply(normalizedCandidate, column.getColumnName()));
            return true;
          }
        }
        return false;
      }

      private boolean inlineUnqualifiedColumn(Column column) {
        String columnName = column.getColumnName();
        if (unqualified.isEmpty() || columnName == null) {
          return false;
        }
        String normalizedColumn = normalizeIdentifier(columnName);
        if (!unqualified.contains(normalizedColumn)) {
          return false;
        }
        appendCorrelatedLiteral(null, columnName, valueResolver.apply(null, columnName));
        return true;
      }

      private void appendCorrelatedLiteral(String qualifier, String columnName, Object resolvedValue) {
        final String literal = toSqlLiteral(resolvedValue);
        correlated.set(true);
        correlatedColumnRefs.add(columnName);
        correlatedRefs.add(new QualifiedColumn(qualifier, columnName));
        getBuilder().append(literal);
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
    if (!correlated.get()) {
      FallbackResult fallback = inlineCorrelatedLiterals(sqlText, normalized, valueResolver);
      if (fallback.correlated()) {
        correlated.set(true);
        correlatedColumnRefs.addAll(fallback.columns());
        correlatedRefs.addAll(fallback.references());
        sqlText = fallback.sql();
      }
    }
    return new Result(sqlText, correlated.get(), Set.copyOf(correlatedColumnRefs), Set.copyOf(correlatedRefs));
  }

  private record FallbackResult(String sql, boolean correlated, Set<String> columns, Set<QualifiedColumn> references) {
  }

  private static FallbackResult inlineCorrelatedLiterals(String sql, Set<String> normalizedQualifiers,
      BiFunction<String, String, Object> valueResolver) {
    if (normalizedQualifiers == null || normalizedQualifiers.isEmpty()) {
      return new FallbackResult(sql, false, Set.of(), Set.of());
    }
    String alternation = normalizedQualifiers.stream().filter(Objects::nonNull)
        .map(CorrelatedSubqueryRewriter::qualifierRegex).collect(Collectors.joining("|"));
    if (alternation.isEmpty()) {
      return new FallbackResult(sql, false, Set.of(), Set.of());
    }
    Pattern pattern = Pattern.compile("\\b(" + alternation + ")\\.([A-Za-z0-9_]+)\\b");
    Matcher matcher = pattern.matcher(sql);
    StringBuffer buf = new StringBuffer();
    boolean matched = false;
    Set<String> columns = new LinkedHashSet<>();
    Set<QualifiedColumn> refs = new LinkedHashSet<>();
    while (matcher.find()) {
      matched = true;
      String qualifier = matcher.group(1);
      String column = matcher.group(2);
      String normalizedQualifier = normalizeIdentifier(qualifier);
      Object value = valueResolver.apply(normalizedQualifier, column);
      String literal = toSqlLiteral(value);
      columns.add(column);
      refs.add(new QualifiedColumn(normalizedQualifier, column));
      matcher.appendReplacement(buf, Matcher.quoteReplacement(literal));
    }
    matcher.appendTail(buf);
    return new FallbackResult(buf.toString(), matched, Set.copyOf(columns), Set.copyOf(refs));
  }

  private static String normalizeIdentifier(String text) {
    if (text == null) {
      return null;
    }
    String trimmed = text.trim();
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

  private static boolean looksLikeLookupKey(String value) {
    if (value == null || value.length() < 2) {
      return false;
    }
    char prefix = Character.toUpperCase(value.charAt(0));
    return (prefix == 'U' || prefix == 'Q') && value.charAt(1) == ':';
  }

  private static String qualifierRegex(String normalizedQualifier) {
    if (normalizedQualifier == null || normalizedQualifier.isBlank()) {
      return "";
    }
    if (normalizedQualifier.startsWith("Q:")) {
      return Pattern.quote(normalizedQualifier.substring(2));
    }
    if (normalizedQualifier.startsWith("U:")) {
      return "(?i:" + Pattern.quote(normalizedQualifier.substring(2)) + ")";
    }
    return Pattern.quote(normalizedQualifier);
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
