package jparq;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class ComputedExpressionsTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = DistinctTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testMultiply() {
    jparqSql.query("SELECT model, mpg, mpg*2 AS double_mpg FROM mtcars", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount(), "Expected 3 column");

        int rows = 0;

        while (rs.next()) {
          String model = rs.getString("model");
          double mpg = rs.getDouble("mpg");
          double doubleMpg = rs.getDouble("double_mpg");
          assertEquals(mpg * 2, doubleMpg, 0.0001, "double_mpg should be mpg * 2 for model " + model);
          seen.add(model + ": " + mpg + " -> " + doubleMpg);
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
  void testPlus() {
    jparqSql.query("SELECT model, cyl, cyl+2 AS cyl2 FROM mtcars", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount(), "Expected 3 column");

        int rows = 0;

        while (rs.next()) {
          String model = rs.getString("model");
          int cyl = rs.getInt("cyl");
          int cyl2 = rs.getInt("cyl2");
          assertEquals(cyl + 2, cyl2, "cyl2 should be cyl + 2 for model " + model);
          seen.add(model + ": " + cyl + " -> " + cyl2);
          rows++;
        }

        assertEquals(32, rows, "Expected 32 rows");
      } catch (SQLException e) {
        System.err.println(String.join("\n", seen));
        fail(e);
      }
    });
  }
}
