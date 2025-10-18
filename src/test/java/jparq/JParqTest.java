package jparq;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Basic tests for JParq functionality. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqTest {

  @Test
  void testSelectMtcarsLimit() throws SQLException, ClassNotFoundException, URISyntaxException {
    // Ensure the driver class is loaded and registered
    // Class.forName("se.alipsa.jparq.JParqDriver");

    URL mtcarsUrl = getClass().getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    String jdbcUrl = "jdbc:jparq:" + dir.toAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM mtcars LIMIT 5")) {

      ResultSetMetaData md = rs.getMetaData();
      int cols = md.getColumnCount();
      Assertions.assertTrue(cols > 0, "Expected at least one column");

      int rows = 0;
      while (rs.next()) {
        // touch a few getters to verify basic mapping works
        Object first = rs.getObject(1);
        if (first != null) {
          first.toString();
        }
        rows++;
      }
      Assertions.assertTrue(rows > 0 && rows <= 5, "Expected 1..5 rows, got " + rows);
    }
  }
}
