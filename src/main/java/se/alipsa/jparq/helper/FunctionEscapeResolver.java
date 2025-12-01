package se.alipsa.jparq.helper;

import java.util.AbstractMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import se.alipsa.jparq.JParqDatabaseMetaData.JdbcDateTimeFunction;
import se.alipsa.jparq.JParqDatabaseMetaData.JdbcNumericFunction;
import se.alipsa.jparq.JParqDatabaseMetaData.JdbcStringFunction;
import se.alipsa.jparq.JParqDatabaseMetaData.JdbcSystemFunction;

/**
 * Resolves JDBC escape syntax function calls (e.g. {@code {fn CURDATE()}}) to
 * the native SQL function names understood by the engine before parsing.
 */
public final class FunctionEscapeResolver {

  private static final Pattern FUNCTION_ESCAPE = Pattern
      .compile("\\{\\s*fn\\s+([^\\s\\(\\}]+)\\s*(\\([^}]*\\))?\\s*\\}", Pattern.CASE_INSENSITIVE);
  private static final Map<String, FnMapping> FUNCTION_LOOKUP = Stream
      .of(Stream.of(JdbcDateTimeFunction.values())
          .map(f -> new AbstractMap.SimpleEntry<>(f.jdbcName().toUpperCase(Locale.ROOT),
              new FnMapping(f.sqlName(), f.noArg(), false))),
          Stream.of(JdbcStringFunction.values())
              .map(f -> new AbstractMap.SimpleEntry<>(f.jdbcName().toUpperCase(Locale.ROOT),
                  new FnMapping(f.sqlName(), false, false))),
          Stream.of(JdbcNumericFunction.values()).flatMap(FunctionEscapeResolver::mapNumericFunction),
          Stream.of(JdbcSystemFunction.values()).map(FunctionEscapeResolver::mapSystemFunction))
      .flatMap(s -> s).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

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
        if (mapping.noArg()) {
          replacement = mapping.sqlName();
        } else if (args == null) {
          replacement = mapping.sqlName() + (mapping.appendEmptyArgs() ? "()" : "");
        } else {
          replacement = mapping.sqlName() + args;
        }
      } else {
        replacement = fnName + (args == null ? "" : args);
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  private static Stream<Map.Entry<String, FnMapping>> mapNumericFunction(JdbcNumericFunction function) {
    return function.jdbcNames().stream().map(name -> new AbstractMap.SimpleEntry<>(name.toUpperCase(Locale.ROOT),
        new FnMapping(function.sqlName(), false, false)));
  }

  private static Map.Entry<String, FnMapping> mapSystemFunction(JdbcSystemFunction function) {
    return new AbstractMap.SimpleEntry<>(function.jdbcName().toUpperCase(Locale.ROOT),
        new FnMapping(function.sqlName(), false, function.appendEmptyArgs()));
  }

  private record FnMapping(String sqlName, boolean noArg, boolean appendEmptyArgs) {
  }
}
