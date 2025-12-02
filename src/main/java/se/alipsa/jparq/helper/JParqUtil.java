package se.alipsa.jparq.helper;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.avro.generic.GenericData;
import se.alipsa.jparq.model.MetaDataResultSet;

/** Utility methods. */
public final class JParqUtil {

  private JParqUtil() {
  }

  /**
   * Parses a URL query string into a Properties object.
   *
   * @param qs
   *          the query string
   * @return a Properties object containing the key-value pairs
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static Properties parseUrlQuery(String qs) {
    Properties p = new Properties();
    if (qs == null || qs.isEmpty()) {
      return p;
    }
    String s = qs.charAt(0) == '?' ? qs.substring(1) : qs;
    for (String kv : s.split("&")) {
      if (kv.isEmpty()) {
        continue;
      }
      String[] arr = kv.split("=", 2);
      String k = URLDecoder.decode(arr[0], StandardCharsets.UTF_8);
      String v = arr.length == 2 ? URLDecoder.decode(arr[1], StandardCharsets.UTF_8) : "";
      if (!k.isEmpty()) {
        p.setProperty(k, v);
      }
    }
    return p;
  }

  /**
   * Converts a SQL LIKE pattern to a Java regex pattern.
   *
   * @param like
   *          the SQL LIKE pattern
   * @return the equivalent Java regex pattern
   */

  public static String sqlLikeToRegex(String like) {
    if (like == null) {
      return "";
    }
    StringBuilder re = new StringBuilder();
    for (int i = 0; i < like.length(); i++) {
      char c = like.charAt(i);
      switch (c) {
        case '%':
          re.append(".*");
          break;
        case '_':
          re.append('.');
          break;
        default:
          if ("\\.[]{}()*+-?^$|".indexOf(c) >= 0) {
            re.append('\\');
          }
          re.append(c);
      }
    }
    return re.toString();
  }

  /**
   * Creates a ResultSet from the given headers and rows.
   *
   * @param headers
   *          the column headers
   * @param rows
   *          the data rows
   * @return a ResultSet representing the data
   */
  public static ResultSet listResultSet(String[] headers, List<Object[]> rows) {
    return new MetaDataResultSet(headers, rows);
  }

  public static ResultSet listResultSet(List<String> headers, List<Object[]> rows) {
    String[] h = headers.toArray(new String[0]);
    return new MetaDataResultSet(h, rows);
  }

  /**
   * Normalizes a SQL table or column qualifier by removing quotes, backticks, and
   * brackets, and converting to lowercase.
   *
   * @param qualifier
   *          the qualifier to normalize (e.g., table name, column name)
   * @return the normalized qualifier, or null if the input is null or empty
   */
  public static String normalizeQualifier(String qualifier) {
    if (qualifier == null) {
      return null;
    }
    String trimmed = qualifier.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("`") && trimmed.endsWith("`"))) {
      trimmed = trimmed.substring(1, trimmed.length() - 1);
    }
    if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
      trimmed = trimmed.substring(1, trimmed.length() - 1);
    }
    return trimmed.toLowerCase(Locale.ROOT);
  }

  /**
   * Convert an array-like object into an {@link Iterable} so callers can process
   * it uniformly.
   *
   * @param raw
   *          the value that should be treated as an iterable collection
   * @return an iterable view of {@code raw}, or {@code null} when it is not
   *         array-like
   */
  public static Iterable<?> toIterable(Object raw) {
    if (raw == null) {
      return null;
    }
    if (raw instanceof GenericData.Array<?> array) {
      return array;
    }
    if (raw instanceof Iterable<?> iterable) {
      return iterable;
    }
    if (raw instanceof Object[] objects) {
      return Arrays.asList(objects);
    }
    return null;
  }
}
