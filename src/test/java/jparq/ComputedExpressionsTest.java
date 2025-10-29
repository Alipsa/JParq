package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

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

  @Test
  void testSimpleCaseExpression() {
    String sql = "SELECT model, cyl, CASE cyl WHEN 4 THEN 'four' WHEN 6 THEN 'six' "
        + "ELSE 'other' END AS cyl_label FROM mtcars";
    jparqSql.query(sql, rs -> {
          List<String> seen = new ArrayList<>();
          List<String> validated = new ArrayList<>();
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount(), "Expected 3 columns");

            while (rs.next()) {
              String model = rs.getString("model");
              String label = rs.getString("cyl_label");
              seen.add(model + ": " + label);

              if ("Mazda RX4".equals(model)) {
                assertEquals("six", label, "Mazda RX4 should map to 'six'");
                validated.add(model);
              } else if ("Merc 240D".equals(model)) {
                assertEquals("four", label, "Merc 240D should map to 'four'");
                validated.add(model);
              } else if ("Cadillac Fleetwood".equals(model)) {
                assertEquals("other", label, "Cadillac Fleetwood should map to 'other'");
                validated.add(model);
              }
            }

            assertTrue(validated.contains("Mazda RX4"), "Expected Mazda RX4 in result");
            assertTrue(validated.contains("Merc 240D"), "Expected Merc 240D in result");
            assertTrue(validated.contains("Cadillac Fleetwood"), "Expected Cadillac Fleetwood in result");
          } catch (SQLException e) {
            System.err.println(String.join("\n", seen));
            fail(e);
          }
        });
  }

  @Test
  void testSearchedCaseExpression() {
    String sql = "SELECT model, mpg, CASE WHEN mpg >= 30 THEN 'high' WHEN mpg >= 20 THEN 'medium' "
        + "ELSE 'low' END AS mpg_class FROM mtcars";
    jparqSql.query(sql, rs -> {
          List<String> seen = new ArrayList<>();
          List<String> validated = new ArrayList<>();
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount(), "Expected 3 columns");

            while (rs.next()) {
              String model = rs.getString("model");
              String mpgClass = rs.getString("mpg_class");
              seen.add(model + ": " + mpgClass);

              if ("Toyota Corolla".equals(model)) {
                assertEquals("high", mpgClass, "Toyota Corolla should be classified as high mpg");
                validated.add(model);
              } else if ("Mazda RX4".equals(model)) {
                assertEquals("medium", mpgClass, "Mazda RX4 should be classified as medium mpg");
                validated.add(model);
              } else if ("Cadillac Fleetwood".equals(model)) {
                assertEquals("low", mpgClass, "Cadillac Fleetwood should be classified as low mpg");
                validated.add(model);
              }
            }

            assertTrue(validated.contains("Toyota Corolla"), "Expected Toyota Corolla in result");
            assertTrue(validated.contains("Mazda RX4"), "Expected Mazda RX4 in result");
            assertTrue(validated.contains("Cadillac Fleetwood"), "Expected Cadillac Fleetwood in result");
          } catch (SQLException e) {
            System.err.println(String.join("\n", seen));
            fail(e);
          }
        });
  }
}
