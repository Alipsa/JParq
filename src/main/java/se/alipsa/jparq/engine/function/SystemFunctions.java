package se.alipsa.jparq.engine.function;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Collection of JDBC system functions (e.g. database metadata helpers).
 */
public final class SystemFunctions {

  private static final ThreadLocal<Deque<ContextScope>> CONTEXT = ThreadLocal.withInitial(ArrayDeque::new);

  private SystemFunctions() {
  }

  /**
   * Return a representation of the current database name.
   *
   * @return the database identifier or {@code "JParq"} when unknown
   */
  public static String database() {
    ContextScope ctx = currentContext();
    return ctx != null && ctx.databaseName != null && !ctx.databaseName.isBlank() ? ctx.databaseName : "JParq";
  }

  /**
   * Return the current user running the query.
   *
   * @return the {@code user.name} system property or {@code null} when
   *         unavailable
   */
  public static String user() {
    ContextScope ctx = currentContext();
    String systemUser = System.getProperty("user.name");
    return ctx != null && ctx.userName != null && !ctx.userName.isBlank() ? ctx.userName : systemUser;
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
    openContext(databaseName, userName);
  }

  /**
   * Clear any system context previously installed for the current thread.
   */
  public static void clearContext() {
    Deque<ContextScope> stack = CONTEXT.get();
    pruneClosed(stack);
    if (!stack.isEmpty()) {
      stack.pop().close();
    }
  }

  /**
   * Installs a context with an identity that can be closed from another thread.
   *
   * @param databaseName
   *          database identifier
   * @param userName
   *          current user
   * @return idempotent handle for exactly this context
   */
  public static ContextScope openContext(String databaseName, String userName) {
    Deque<ContextScope> stack = CONTEXT.get();
    pruneClosed(stack);
    ContextScope context = new ContextScope(databaseName, userName);
    // pruneClosed may remove the ThreadLocal when the stack was empty.
    CONTEXT.set(stack);
    stack.push(context);
    return context;
  }

  /**
   * Removes closed handles only from the calling thread's own context stack.
   *
   * @param stack
   *          calling thread's context stack
   */
  private static void pruneClosed(Deque<ContextScope> stack) {
    stack.removeIf(context -> context.closed);
    if (stack.isEmpty()) {
      CONTEXT.remove();
    }
  }

  /**
   * Resolves the current live context, including after cross-thread cancellation.
   *
   * @return top live context or null
   */
  private static ContextScope currentContext() {
    Deque<ContextScope> stack = CONTEXT.get();
    pruneClosed(stack);
    return stack.peek();
  }

  /**
   * An identity-bearing context that never pops another thread's live context.
   */
  public static final class ContextScope implements AutoCloseable {
    private final String databaseName;
    private final String userName;
    private volatile boolean closed;

    /**
     * Records the function values for one execution.
     *
     * @param databaseName
     *          database identifier
     * @param userName
     *          current user
     */
    private ContextScope(String databaseName, String userName) {
      this.databaseName = databaseName;
      this.userName = userName;
    }

    @Override
    public void close() {
      closed = true;
      pruneClosed(CONTEXT.get());
    }
  }
}
