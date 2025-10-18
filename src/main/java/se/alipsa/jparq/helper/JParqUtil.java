package se.alipsa.jparq.helper;

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
    for (String kv : qs.split("&")) {
      String[] arr = kv.split("=", 2);
      if (arr.length == 2) {
        p.setProperty(arr[0], arr[1]);
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
    return like.replace("%", ".*").replace("_", ".");
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
