package se.alipsa.jparq.engine.function;

/**
 * Collection of JDBC system functions (e.g. database metadata helpers).
 */
public final class SystemFunctions {

  private SystemFunctions() {
  }

  /**
   * Return a representation of the current database name.
   *
   * @return the database identifier or {@code "JParq"} when unknown
   */
  public static String database() {
    return "JParq";
  }

  /**
   * Return the current user running the query.
   *
   * @return the {@code user.name} system property or {@code null} when
   *         unavailable
   */
  public static String user() {
    return System.getProperty("user.name");
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
}
