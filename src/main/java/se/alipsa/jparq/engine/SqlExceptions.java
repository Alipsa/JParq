package se.alipsa.jparq.engine;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.util.Objects;
import java.util.Set;
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
  private static final Set<String> CLASSIFIED_STATES = Set.of("42000", "42702", "22003", "22007", "22008", "22018",
      "0A000");
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
   * Finds classified syntax, numeric, temporal data, or unsupported feature
   * errors inside engine wrappers.
   *
   * @param failure
   *          the exception or wrapper to inspect
   * @return the SQLState 42000, 42702, 22003, 22007, 22008, 22018, or 0A000
   *         exception, or null without a recognized state
   */
  public static SQLException findSqlStateFailure(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof SQLException sql && sql.getSQLState() != null
          && CLASSIFIED_STATES.contains(sql.getSQLState())) {
        return sql;
      }
      current = current.getCause();
    }
    return null;
  }

  /**
   * Wraps an engine failure without discarding its context or SQLState.
   *
   * @param context
   *          the operation that failed
   * @param failure
   *          the original failure, retained as the cause
   * @return a classified JDBC exception, or an ordinary SQLException without a
   *         known state
   */
  public static SQLException wrap(String context, Throwable failure) {
    SQLException classified = findSqlStateFailure(failure);
    if (classified == null) {
      return new SQLException(context, failure);
    }
    if (failure == classified && Objects.equals(context, classified.getMessage())) {
      return classified;
    }
    StringBuilder message = new StringBuilder(context == null ? failure.toString() : context);
    String detail = failure.getMessage();
    if (detail != null && !detail.isBlank() && message.indexOf(detail) < 0) {
      message.append(": ").append(detail);
    }
    String classifiedDetail = classified.getMessage();
    if (classifiedDetail != null && !classifiedDetail.isBlank() && message.indexOf(classifiedDetail) < 0) {
      message.append(": ").append(classifiedDetail);
    }
    if ("42000".equals(classified.getSQLState()) || "42702".equals(classified.getSQLState())) {
      return new SQLSyntaxErrorException(message.toString(), classified.getSQLState(), classified.getErrorCode(),
          failure);
    }
    if ("0A000".equals(classified.getSQLState())) {
      return new SQLFeatureNotSupportedException(message.toString(), classified.getSQLState(),
          classified.getErrorCode(), failure);
    }
    return new SQLDataException(message.toString(), classified.getSQLState(), classified.getErrorCode(), failure);
  }
}
