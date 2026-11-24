package se.alipsa.jparq.engine;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import net.sf.jsqlparser.expression.Expression;

/**
 * Preserves SQLState classifications and diagnostic context across engine
 * boundaries.
 */
public final class SqlExceptions {
  /** Shared diagnostic prefix for unsupported Boolean predicates. */
  public static final String UNSUPPORTED_PREDICATE = "Unsupported predicate:";
  /** Diagnostic marker replacing private correlated value bindings. */
  public static final String OUTER_VALUE = "<outer value>";
  private static final int DIAGNOSTIC_VALUE_LIMIT = 64;
  private static final Set<String> CLASSIFIED_STATES = Set.of("21000", "42000", "42702", "42803", "22001", "22005",
      "22003", "22007", "22008", "22018", "22021", "2201W", "2201X", "0A000", "54000", "HY024");
  private static final Pattern CORRELATED_BINDING = Pattern
      .compile("__JPARQ_CORRELATED_VALUE\\s*\\(\\s*'[^']*'\\s*,\\s*\\d+\\s*\\)", Pattern.CASE_INSENSITIVE);

  /**
   * Renders an expression for diagnostics without exposing private correlated
   * bindings. Rendered-text replacement also covers bindings inside nested
   * subqueries.
   *
   * @param expression
   *          the expression to describe
   * @return SQL text with each private binding replaced by a neutral outer-value
   *         token
   */
  public static String describe(Expression expression) {
    return describe(expression.toString());
  }

  /**
   * Redacts private correlated bindings from rendered SQL or column labels.
   *
   * @param text
   *          the diagnostic text, possibly null
   * @return text with each private binding replaced by a neutral outer-value
   *         token, or "null" for null input
   */
  public static String describe(String text) {
    return CORRELATED_BINDING.matcher(String.valueOf(text)).replaceAll(OUTER_VALUE);
  }

  /**
   * Redacts private correlated bindings from the rendered text of diagnostic
   * objects whose {@code toString()} embeds expressions, such as window frame
   * elements and offsets.
   *
   * @param text
   *          the diagnostic object, possibly null
   * @return text with each private binding replaced by a neutral outer-value
   *         token, or "null" for null input
   */
  public static String describe(Object text) {
    return describe(String.valueOf(text));
  }

  /**
   * Removes generated preparation context while retaining the user's argument and
   * the underlying error detail. The nested query may appear in its original form
   * or already redacted to the outer-value marker.
   *
   * @param text
   *          nested diagnostic text, possibly null
   * @param sql
   *          the generated nested query
   * @param original
   *          the original user expression
   * @return redacted diagnostics without the nested preparation prefix
   */
  public static String describeSubqueryFailure(String text, String sql, String original) {
    if (text == null) {
      return describe((String) null);
    }
    String redactedSql = describe(sql);
    String stripped = text.replace("Failed to prepare query: " + sql + ": ", "")
        .replace("Failed to prepare query: " + redactedSql + ": ", "");
    return describe(stripped.replace(sql, original).replace(redactedSql, original));
  }

  /** Prevents utility class instantiation. */
  private SqlExceptions() {
  }

  /**
   * Bounds values embedded in conversion diagnostics without splitting surrogate
   * pairs.
   *
   * @param value
   *          the input value
   * @return at most 64 UTF-16 code units plus an ellipsis when truncated
   */
  public static String diagnosticValue(Object value) {
    String text = String.valueOf(value);
    if (text.length() <= DIAGNOSTIC_VALUE_LIMIT) {
      return text;
    }
    int end = DIAGNOSTIC_VALUE_LIMIT;
    if (Character.isHighSurrogate(text.charAt(end - 1)) && Character.isLowSurrogate(text.charAt(end))) {
      end--;
    }
    return text.substring(0, end) + "…";
  }

  /**
   * Classifies a negative row count or a NULL, fractional, non-finite, or
   * nonnumeric bound row count.
   *
   * @param clause
   *          the LIMIT, OFFSET, or FETCH clause name
   * @param value
   *          the invalid value
   * @return a data exception with SQLState 2201X for OFFSET or 2201W otherwise
   */
  public static SQLDataException invalidRowCount(String clause, Object value) {
    return new SQLDataException("Invalid " + clause + " row count: " + diagnosticValue(value),
        "OFFSET".equals(clause) ? "2201X" : "2201W");
  }

  /**
   * Creates an engine failure for numeric values outside the target range.
   *
   * @param message
   *          the diagnostic message
   * @return an engine exception carrying SQLState 22003
   */
  public static IllegalArgumentException numericRangeFailure(String message) {
    return new IllegalArgumentException(message, new SQLDataException(message, "22003"));
  }

  /**
   * Creates an engine failure carrying the JDBC syntax classification.
   *
   * @param message
   *          the diagnostic message
   * @return an engine exception carrying SQLState 42000
   */
  public static IllegalArgumentException syntaxFailure(String message) {
    return new IllegalArgumentException(message, new SQLSyntaxErrorException(message, "42000"));
  }

  /**
   * Creates an engine failure for a column outside the grouped query scope.
   *
   * @param message
   *          diagnostic message identifying the ungrouped column
   * @return an engine exception carrying SQLState 42803
   */
  public static IllegalArgumentException groupingFailure(String message) {
    return new IllegalArgumentException(message, new SQLSyntaxErrorException(message, "42803"));
  }

  /**
   * Creates a failure for an implementation resource limit.
   *
   * @param message
   *          diagnostic message
   * @return engine failure carrying SQLState 54000
   */
  public static IllegalArgumentException implementationLimit(String message) {
    return implementationLimit(message, null);
  }

  /**
   * Creates an implementation-limit failure retaining its original cause.
   *
   * @param message
   *          diagnostic message
   * @param cause
   *          original failure, or null
   * @return engine failure carrying SQLState 54000
   */
  public static IllegalArgumentException implementationLimit(String message, Throwable cause) {
    return new IllegalArgumentException(message, new SQLException(message, "54000", cause));
  }

  /**
   * Creates a failure for an invalid configuration attribute value.
   *
   * @param message
   *          diagnostic message
   * @return engine failure carrying SQLState HY024
   */
  public static IllegalArgumentException invalidAttribute(String message) {
    return invalidAttribute(message, null);
  }

  /**
   * Creates an invalid-attribute failure retaining its original cause.
   *
   * @param message
   *          diagnostic message
   * @param cause
   *          original failure, or null
   * @return engine failure carrying SQLState HY024
   */
  public static IllegalArgumentException invalidAttribute(String message, Throwable cause) {
    return new IllegalArgumentException(message, new SQLException(message, "HY024", cause));
  }

  /**
   * Describes incompatible ARRAY element types consistently across evaluation and
   * inference.
   *
   * @param first
   *          the first SQL type name
   * @param second
   *          the incompatible SQL type name
   * @return a failure carrying SQLState 42000
   */
  public static IllegalArgumentException incompatibleArrayElements(String first, String second) {
    return syntaxFailure("ARRAY elements must share a common type; found " + first + " and " + second);
  }

  /**
   * Creates an engine failure for a scalar subquery returning multiple rows.
   *
   * @return an engine exception carrying SQLState 21000
   */
  public static IllegalArgumentException scalarCardinalityFailure() {
    String message = "Scalar subquery returned more than one row";
    return new IllegalArgumentException(message, new SQLDataException(message, "21000"));
  }

  /**
   * Creates an engine failure carrying the JDBC unsupported-feature
   * classification.
   *
   * @param message
   *          the diagnostic message
   * @return an engine exception carrying SQLState 0A000
   */
  public static IllegalArgumentException unsupportedFeature(String message) {
    return unsupportedFeature(message, null);
  }

  /**
   * Creates an unsupported-feature failure retaining its original cause.
   *
   * @param message
   *          the diagnostic message
   * @param cause
   *          the original failure, or null
   * @return an engine exception carrying SQLState 0A000
   */
  public static IllegalArgumentException unsupportedFeature(String message, Throwable cause) {
    return new IllegalArgumentException(message, new SQLFeatureNotSupportedException(message, "0A000", cause));
  }

  /**
   * Creates an engine failure for an ambiguous SQL column reference.
   *
   * @param column
   *          the referenced SQL column
   * @return an engine exception carrying SQLState 42702
   */
  public static IllegalArgumentException ambiguousColumn(String column) {
    String message = "Ambiguous column reference: " + column;
    return new IllegalArgumentException(message, new SQLSyntaxErrorException(message, "42702"));
  }

  /**
   * Creates a syntax failure naming an unsupported predicate without private
   * bindings.
   *
   * @param expression
   *          the invalid predicate
   * @return an engine exception carrying SQLState 42000 and a redacted diagnostic
   */
  public static IllegalArgumentException unsupportedPredicate(Expression expression) {
    return syntaxFailure(UNSUPPORTED_PREDICATE + " " + describe(expression));
  }

  /**
   * Finds classified data, syntax, feature, resource, and configuration failures
   * inside engine wrappers.
   *
   * @param failure
   *          the exception or wrapper to inspect
   * @return the SQLState 21000, 42000, 42702, 42803, 22001, 22005, 22003, 22007,
   *         22008, 22018, 22021, 2201W, 2201X, 0A000, 54000, or HY024 exception,
   *         or null without a recognized state
   */
  public static SQLException findSqlStateFailure(Throwable failure) {
    return findSqlStateFailure(causeChain(failure));
  }

  /**
   * Finds a classified failure among the given cause chain links.
   *
   * @param chain
   *          the cause chain links to inspect
   * @return the classified exception, or null without a recognized state
   */
  private static SQLException findSqlStateFailure(List<Throwable> chain) {
    for (Throwable current : chain) {
      if (current instanceof SQLException sql && sql.getSQLState() != null
          && CLASSIFIED_STATES.contains(sql.getSQLState())) {
        return sql;
      }
    }
    return null;
  }

  /**
   * Walks a failure's cause chain once, visiting each link at most once so a
   * cyclic chain (which {@code initCause} allows) cannot loop forever.
   *
   * @param failure
   *          the exception to inspect, possibly null
   * @return the links from the given failure to its root, each visited at most
   *         once
   */
  private static List<Throwable> causeChain(Throwable failure) {
    List<Throwable> chain = new ArrayList<>();
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable current = failure; current != null && visited.add(current); current = current.getCause()) {
      chain.add(current);
    }
    return chain;
  }

  /**
   * Wraps an engine failure without discarding its context or SQLState.
   *
   * @param context
   *          the operation that failed
   * @param failure
   *          the original failure, retained as the cause
   * @return a classified JDBC exception, or a plain SQLException that appends the
   *         redacted failure detail (and, without a recognized classification,
   *         the redacted innermost SQLException detail) to the verbatim context
   *         and keeps the SQLState and vendor code of the first state-bearing
   *         SQLException in the chain; with several SQLExceptions in the chain,
   *         that state may come from a different, outer exception than the
   *         appended innermost detail. The context the caller passes is not
   *         redacted, including the failure's own message when the caller passes
   *         it as the context, and when the failure is already the classified
   *         exception with a message equal to the context, it is returned
   *         unchanged with its own text
   */
  public static SQLException wrap(String context, Throwable failure) {
    return wrap(context, failure, UnaryOperator.identity());
  }

  /**
   * Wraps an engine failure with the context that matches its classification: the
   * classified context when the cause chain carries a recognized SQLState, the
   * fallback context otherwise.
   *
   * @param classifiedContext
   *          the operation context for a failure carrying a recognized
   *          classification
   * @param fallbackContext
   *          the operation context for a failure without a recognized
   *          classification
   * @param failure
   *          the original failure, retained as the cause
   * @return the failure wrapped with the matching context, with the same
   *         classification and detail rules as the two-argument wrap
   */
  public static SQLException wrap(String classifiedContext, String fallbackContext, Throwable failure) {
    // The chain is walked once and reused by the wrapped call through the
    // precomputed links.
    List<Throwable> chain = causeChain(failure);
    return wrap(findSqlStateFailure(chain) == null ? fallbackContext : classifiedContext, failure,
        UnaryOperator.identity(), chain);
  }

  /**
   * Preserves JDBC classifications while rendering all diagnostic text for the
   * caller.
   *
   * @param context
   *          the failed operation
   * @param failure
   *          the original failure, retained as the cause
   * @param diagnostics
   *          renderer for context and nested messages, accepting null text
   * @return a classified JDBC exception with rendered diagnostics; without a
   *         recognized classification a plain SQLException is returned that
   *         appends the redacted failure detail and the redacted innermost
   *         SQLException detail to the rendered context and keeps the SQLState
   *         and vendor code of the first state-bearing SQLException in the chain.
   *         The context the caller passes is not redacted, including the
   *         failure's own message when the caller passes it as the context, and
   *         when the failure is already the classified exception with a message
   *         equal to the context, it is returned unchanged with its own text
   */
  public static SQLException wrap(String context, Throwable failure, UnaryOperator<String> diagnostics) {
    return wrap(context, failure, diagnostics, causeChain(failure));
  }

  /**
   * Wraps an engine failure using cause chain links computed by the caller.
   *
   * @param context
   *          the failed operation
   * @param failure
   *          the original failure, retained as the cause
   * @param diagnostics
   *          renderer for context and nested messages, accepting null text
   * @param chain
   *          the failure's cause chain links, as computed by
   *          {@link #causeChain(Throwable)}
   * @return the wrapped failure, exactly as the public overload without
   *         precomputed links would wrap it
   */
  private static SQLException wrap(String context, Throwable failure, UnaryOperator<String> diagnostics,
      List<Throwable> chain) {
    SQLException classified = findSqlStateFailure(chain);
    if (classified != null && failure == classified && Objects.equals(context, classified.getMessage())
        && Objects.equals(context, diagnostics.apply(context))) {
      return classified;
    }
    // The caller's context stays verbatim; when it is null, the failure's own
    // text is a fallback and is redacted like any other detail.
    StringBuilder message = new StringBuilder(context == null ? describe(String.valueOf(failure)) : context);
    appendDetail(message, failure == null ? null : failure.getMessage());
    if (classified != null) {
      appendDetail(message, classified.getMessage());
    } else {
      // Without a recognized classification the direct failure may be a wrapper
      // that carries context but not the reason; also surface the innermost
      // SQLException's message so the root cause is not hidden in the chain.
      SQLException innermost = innermostSqlException(chain);
      appendDetail(message, innermost == null ? null : innermost.getMessage());
    }
    String rendered = diagnostics.apply(message.toString());
    if (classified != null) {
      if (classified.getSQLState().startsWith("42")) {
        return new SQLSyntaxErrorException(rendered, classified.getSQLState(), classified.getErrorCode(), failure);
      }
      if ("0A000".equals(classified.getSQLState())) {
        return new SQLFeatureNotSupportedException(rendered, classified.getSQLState(), classified.getErrorCode(),
            failure);
      }
      if ("54000".equals(classified.getSQLState()) || "HY024".equals(classified.getSQLState())) {
        return new SQLException(rendered, classified.getSQLState(), classified.getErrorCode(), failure);
      }
      return new SQLDataException(rendered, classified.getSQLState(), classified.getErrorCode(), failure);
    }
    // Without a recognized classification, retain the state and vendor code of
    // the first state-bearing SQLException in the chain rather than dropping
    // them, even when the direct failure is an ordinary runtime wrapper.
    SQLException stateful = firstStatefulSqlException(chain);
    if (stateful != null) {
      return new SQLException(rendered, stateful.getSQLState(), stateful.getErrorCode(), failure);
    }
    return new SQLException(rendered, failure);
  }

  /**
   * Appends a detail to a diagnostic message, redacted, when it is non-blank and
   * not already carried by the message, in raw or redacted form. The caller's
   * context stays verbatim; redacting the detail keeps a raw correlated binding
   * out of the message even on caller paths whose diagnostics do no redaction of
   * their own. A detail is carried when the message equals it or carries it as a
   * complete ": "-separated segment, so a coincidental substring match cannot
   * drop a short detail.
   *
   * @param message
   *          the message being built
   * @param detail
   *          the detail to append, possibly null
   */
  private static void appendDetail(StringBuilder message, String detail) {
    if (detail == null || detail.isBlank()) {
      return;
    }
    String text = message.toString();
    String redacted = describe(detail);
    if (!detailPresent(text, detail) && !detailPresent(text, redacted)) {
      message.append(": ").append(redacted);
    }
  }

  /**
   * Reports whether a message already carries a detail, as the whole message or
   * as a complete ": "-separated segment.
   *
   * @param text
   *          the message built so far
   * @param detail
   *          the detail to look for
   * @return true when the message equals the detail or the detail forms a whole
   *         segment, bounded by ": " or the ends of the message
   */
  private static boolean detailPresent(String text, String detail) {
    if (text.equals(detail)) {
      return true;
    }
    for (int index = text.indexOf(detail); index >= 0; index = text.indexOf(detail, index + 1)) {
      boolean startsSegment = index == 0 || text.startsWith(": ", index - 2);
      int end = index + detail.length();
      if (startsSegment && (end == text.length() || text.startsWith(": ", end))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Finds the deepest {@code SQLException} in a cause chain, the link that
   * usually carries the actual reason.
   *
   * @param chain
   *          the cause chain links to inspect
   * @return the innermost SQLException, or null when none
   */
  private static SQLException innermostSqlException(List<Throwable> chain) {
    for (int index = chain.size() - 1; index >= 0; index--) {
      if (chain.get(index) instanceof SQLException sql) {
        return sql;
      }
    }
    return null;
  }

  /**
   * Finds the first state-bearing {@code SQLException} in a cause chain.
   *
   * @param chain
   *          the cause chain links to inspect
   * @return the first SQLException with a non-null SQLState, or null when none
   */
  private static SQLException firstStatefulSqlException(List<Throwable> chain) {
    for (Throwable current : chain) {
      if (current instanceof SQLException sql && sql.getSQLState() != null) {
        return sql;
      }
    }
    return null;
  }
}
