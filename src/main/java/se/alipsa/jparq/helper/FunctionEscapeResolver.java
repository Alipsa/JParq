package se.alipsa.jparq.helper;

import java.util.AbstractMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import se.alipsa.jparq.JParqDatabaseMetaData.JdbcDateTimeFunction;
import se.alipsa.jparq.JParqDatabaseMetaData.JdbcStringFunction;

/**
 * Resolves JDBC escape syntax function calls (e.g. {@code {fn CURDATE()}}) to
 * the native SQL function names understood by the engine before parsing.
 */
public final class FunctionEscapeResolver {

  private static final Pattern FUNCTION_ESCAPE = Pattern
      .compile("\\{\\s*fn\\s+([^\\s\\(\\}]+)\\s*(\\([^}]*\\))?\\s*\\}", Pattern.CASE_INSENSITIVE);
  private static final Map<String, FnMapping> FUNCTION_LOOKUP = Stream
      .concat(
          Stream.of(JdbcDateTimeFunction.values())
              .map(f -> new AbstractMap.SimpleEntry<>(f.jdbcName().toUpperCase(Locale.ROOT),
                  new FnMapping(f.sqlName(), f.noArg()))),
          Stream.of(JdbcStringFunction.values())
              .map(f -> new AbstractMap.SimpleEntry<>(f.jdbcName().toUpperCase(Locale.ROOT),
                  new FnMapping(f.sqlName(), false))))
      .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

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
      FnMapping mapping = FUNCTION_LOOKUP.get(fnName.toUpperCase(Locale.ROOT));
      String replacement;
      if (mapping != null) {
        replacement = mapping.noArg() ? mapping.sqlName() : mapping.sqlName() + (args == null ? "" : args);
      } else {
        replacement = fnName + (args == null ? "" : args);
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  private record FnMapping(String sqlName, boolean noArg) {
  }
}
