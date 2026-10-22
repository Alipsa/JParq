package se.alipsa.jparq.engine;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Objects;
import java.util.regex.Pattern;
import net.sf.jsqlparser.expression.Expression;

/**
 * Preserves SQLState classifications and diagnostic context across engine
 * boundaries.
 */
public final class SqlExceptions {
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
    return CORRELATED_BINDING.matcher(expression.toString()).replaceAll("<outer value>");
  }

  /** Prevents utility class instantiation. */
  private SqlExceptions() {
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
   * Finds classified syntax, numeric range, or numeric conversion errors inside
   * engine wrappers.
   *
   * @param failure
   *          the exception or wrapper to inspect
   * @return the SQLState 42000, 22003, or 22018 exception, or null without a
   *         recognized state
   */
  public static SQLException findSqlStateFailure(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof SQLException sql && ("22003".equals(sql.getSQLState()) || "42000".equals(sql.getSQLState())
          || "22018".equals(sql.getSQLState()))) {
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
    if ("42000".equals(classified.getSQLState())) {
      return new SQLSyntaxErrorException(message.toString(), classified.getSQLState(), classified.getErrorCode(),
          failure);
    }
    return new SQLDataException(message.toString(), classified.getSQLState(), classified.getErrorCode(), failure);
  }
}
