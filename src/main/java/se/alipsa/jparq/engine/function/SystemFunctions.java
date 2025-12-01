package se.alipsa.jparq.engine.function;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Collection of JDBC system functions (e.g. database metadata helpers).
 */
public final class SystemFunctions {

  private static final ThreadLocal<Deque<SystemContext>> CONTEXT = ThreadLocal.withInitial(ArrayDeque::new);

  private SystemFunctions() {
  }

  /**
   * Return a representation of the current database name.
   *
   * @return the database identifier or {@code "JParq"} when unknown
   */
  public static String database() {
    SystemContext ctx = currentContext();
    return ctx != null && ctx.databaseName() != null && !ctx.databaseName().isBlank() ? ctx.databaseName() : "JParq";
  }

  /**
   * Return the current user running the query.
   *
   * @return the {@code user.name} system property or {@code null} when
   *         unavailable
   */
  public static String user() {
    SystemContext ctx = currentContext();
    String systemUser = System.getProperty("user.name");
    return ctx != null && ctx.userName() != null && !ctx.userName().isBlank() ? ctx.userName() : systemUser;
  }

  /**
   * MySQL-style {@code IFNULL(expr1, expr2)}.
   *
   * @param first
   *          first expression value
   * @param second
   *          fallback expression value
   * @return {@code first} when non-null; otherwise {@code second}
   */
  public static Object ifNull(Object first, Object second) {
    return first != null ? first : second;
  }

  /**
   * Install per-execution system context for system functions.
   *
   * @param databaseName
   *          the database identifier to expose via {@code DATABASE()}
   * @param userName
   *          the user identifier to expose via {@code USER()}
   */
  public static void setContext(String databaseName, String userName) {
    CONTEXT.get().push(new SystemContext(databaseName, userName));
  }

  /**
   * Clear any system context previously installed for the current thread.
   */
  public static void clearContext() {
    Deque<SystemContext> stack = CONTEXT.get();
    if (stack.isEmpty()) {
      CONTEXT.remove();
    } else {
      stack.pop();
      if (stack.isEmpty()) {
        CONTEXT.remove();
      }
    }
  }

  private static SystemContext currentContext() {
    Deque<SystemContext> stack = CONTEXT.get();
    if (stack == null || stack.isEmpty()) {
      return null;
    }
    return stack.peek();
  }

  private record SystemContext(String databaseName, String userName) {
  }
}
