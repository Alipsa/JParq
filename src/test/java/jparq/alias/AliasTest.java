package jparq.alias;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import jparq.WhereTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for alias operations. */
public class AliasTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = WhereTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testColumnAlias() {
    jparqSql.query("SELECT model as type, mpg as miles_per_gallon FROM mtcars", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;

        while (rs.next()) {
          String model = rs.getString("type");
          double mpg = rs.getDouble("miles_per_gallon");
          seen.add(model + " " + mpg);
          rows++;
        }

        assertEquals(32, rows, "Expected 32 rows");
      } catch (SQLException e) {
        System.err.println(String.join("\n", seen));
        fail(e);
      }
    });
  }

  @Test
  void testColumnAliasInOrderBy() {
    jparqSql.query("SELECT mpg AS miles_per_gallon, model FROM mtcars ORDER BY miles_per_gallon DESC", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;

        while (rs.next()) {
          String model = rs.getString("model");
          double mpg = rs.getDouble("miles_per_gallon");
          seen.add(model + " " + mpg);
          rows++;
        }

        assertEquals(32, rows, "Expected 32 rows");
      } catch (SQLException e) {
        System.err.println(String.join("\n", seen));
        fail(e);
      }
    });
  }

  @Test
  void testTableAlias() {
    jparqSql.query("SELECT t.model, t.mpg FROM mtcars t WHERE t.mpg > 25 ORDER BY t.mpg DESC LIMIT 3", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;

        while (rs.next()) {
          String model = rs.getString("model");
          double mpg = rs.getDouble("mpg");
          seen.add(model + " " + mpg);
          rows++;
        }

        assertEquals(3, rows, "Expected 3 rows");
      } catch (SQLException e) {
        System.err.println(String.join("\n", seen));
        fail(e);
      }
    });
  }

}
