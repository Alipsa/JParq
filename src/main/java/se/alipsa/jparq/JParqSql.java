package se.alipsa.jparq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Consumer;

/**
 * Utility class to allow for easier querying of parquet files. Example usage: <code><pre>
 *   URL mtcarsUrl = WhereTest.class.getResource("/mtcars.parquet");
 *   Path dir = Paths.get(mtcarsUrl.toURI()).getParent();
 *   JParqSql jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
 *
 *   jparqSql.query("SELECT model, mpg, cyl FROM mtcars where cyl = 6", rs -> {
 *       try {
 *         while (rs.next()) {
 *           System.out.println(rs.getString(1));
 *         }
 *       } catch (SQLException e) {
 *         throw new RuntimeExceptions(e);
 *       }
 *     });
 * </pre></code>
 */
public class JParqSql {

  final String jdbcUrl;

  /**
   * @param url the jdbc url in the form "jdbc:jparq:path/to/dir"
   */
  public JParqSql(String url) {
    jdbcUrl = url;
  }

  public void query(String sql, Consumer<ResultSet> processor) {
    try (Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      processor.accept(rs);
    } catch (Exception e) {
      throw new RuntimeException("Query failed: " + sql, e);
    }
  }
}
