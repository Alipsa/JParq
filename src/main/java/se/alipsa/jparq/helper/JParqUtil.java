package se.alipsa.jparq.helper;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;
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
}
