package se.alipsa.jparq.engine;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Objects;

/**
 * Preserves SQLState classifications and diagnostic context across engine
 * boundaries.
 */
public final class SqlExceptions {
  /** Prevents utility class instantiation. */
  private SqlExceptions() {
  }

  /**
   * Finds classified syntax or numeric errors inside engine wrappers.
   *
   * @param failure
   *          the exception or wrapper to inspect
   * @return the SQLState 42000 or 22003 exception, or null without either state
   */
  public static SQLException findSqlStateFailure(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof SQLException sql
          && ("22003".equals(sql.getSQLState()) || "42000".equals(sql.getSQLState()))) {
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
