package se.alipsa.jparq.helper;

import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import se.alipsa.jparq.JParqDatabaseMetaData.JdbcDateTimeFunction;

/**
 * Resolves JDBC escape syntax function calls (e.g. {@code {fn CURDATE()}}) to
 * the native SQL function names understood by the engine before parsing.
 */
public final class FunctionEscapeResolver {

  private static final Pattern FUNCTION_ESCAPE = Pattern
      .compile("\\{\\s*fn\\s+([^\\s\\(\\}]+)\\s*(\\([^}]*\\))?\\s*\\}", Pattern.CASE_INSENSITIVE);
  private static final Map<String, JdbcDateTimeFunction> DATE_TIME_LOOKUP = Stream.of(JdbcDateTimeFunction.values())
      .collect(Collectors.toUnmodifiableMap(f -> f.jdbcName().toUpperCase(Locale.ROOT), f -> f));

  private FunctionEscapeResolver() {
  }

  /**
   * Resolve JDBC function escapes to native SQL equivalents. Unknown functions
   * are unwrapped (braces removed) but otherwise left unchanged.
   *
   * @param sql
   *          the SQL text that may contain JDBC escape functions
   * @return SQL with escapes replaced by native function calls
   */
  public static String resolveJdbcFunctionEscapes(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    Matcher matcher = FUNCTION_ESCAPE.matcher(sql);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String fnName = matcher.group(1);
      String args = matcher.group(2);
      JdbcDateTimeFunction dtFn = DATE_TIME_LOOKUP.get(fnName.toUpperCase(Locale.ROOT));
      String replacement;
      if (dtFn != null) {
        boolean hasArgs = args != null && !args.trim().isEmpty() && !"()".equals(args.trim());
        replacement = hasArgs || !dtFn.noArg() ? dtFn.sqlName() + (args == null ? "" : args) : dtFn.sqlName();
      } else {
        replacement = fnName + (args == null ? "" : args);
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }
}
