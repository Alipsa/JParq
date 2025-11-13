package jparq;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import jparq.usage.AcmeTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class JParqDatabaseMetaDataTest {

  private static JParqSql jparqSql;

  static String jdbcUrl;
  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = AcmeTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jdbcUrl = "jdbc:jparq:" + acmePath.toAbsolutePath();
  }

  @Test
  void databaseMetaDataTest() throws SQLException {
    try (Connection con = DriverManager.getConnection(jdbcUrl)) {
      var metaData = con.getMetaData();
      Assertions.assertEquals("JParq", metaData.getDatabaseProductName());
      Assertions.assertEquals("se.alipsa.jparq.JParqDriver", metaData.getDriverName());
      ResultSet rs = metaData.getTables(null, null, null, new String[]{
          "TABLE"
      });
      printResultSet(rs);
      System.out.println("-------------------");
      printResultSet(metaData.getColumns(null, null, null, null));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void printResultSet(ResultSet rs) throws SQLException {
    int rowCount = 0;
    while (rs.next()) {
      if (rowCount++ == 0) {
        for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
          System.out.print(rs.getMetaData().getColumnName(j + 1) + " ");
        }
        System.out.println();
      }
      for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
        System.out.print(rs.getString(j + 1) + " ");
      }
      System.out.println();
    }
  }
}
