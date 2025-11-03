package jparq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for ORDER BY operations. */
public class OrderByTest {

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
  void testOrderByMpgAsc() {
    jparqSql.query("SELECT model, mpg FROM mtcars ORDER BY mpg ASC", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;
        double prev = Double.NEGATIVE_INFINITY;

        while (rs.next()) {
          String model = rs.getString("model");
          double mpg = rs.getDouble("mpg");
          seen.add(model + " " + mpg);

          // non-decreasing
          assertTrue(mpg >= prev, "mpg must be non-decreasing: " + String.join(", ", seen));
          prev = mpg;
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
  void testOrderByMpgDesc() {
    jparqSql.query("SELECT model, mpg FROM mtcars ORDER BY mpg DESC", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;
        double prev = Double.POSITIVE_INFINITY;

        while (rs.next()) {
          String model = rs.getString("model");
          double mpg = rs.getDouble("mpg");
          seen.add(model + " " + mpg);

          // non-increasing
          assertTrue(mpg <= prev, "mpg must be non-increasing: " + String.join(", ", seen));
          prev = mpg;
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
  void testOrderByMultiKey() {
    // Sort by cyl ASC, then mpg DESC within each cyl group
    jparqSql.query("SELECT model, cyl, mpg FROM mtcars ORDER BY cyl ASC, mpg DESC", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount(), "Expected 3 columns");

        int rows = 0;
        Integer prevCyl = null;
        Double prevMpgInGroup = null;

        while (rs.next()) {
          String model = rs.getString("model");
          int cyl = rs.getInt("cyl");
          double mpg = rs.getDouble("mpg");
          seen.add(model + " cyl=" + cyl + " mpg=" + mpg);

          if (prevCyl == null) {
            prevCyl = cyl;
            prevMpgInGroup = mpg;
          } else {
            // primary key non-decreasing
            assertTrue(cyl >= prevCyl, "cyl must be non-decreasing: " + String.join(", ", seen));
            if (cyl == prevCyl) {
              // secondary key: mpg non-increasing within same cyl
              assertTrue(mpg <= prevMpgInGroup + 1e-9,
                  "mpg must be non-increasing within cyl=" + cyl + ": " + String.join(", ", seen));
            } else {
              // new group: reset secondary comparator
              prevCyl = cyl;
              prevMpgInGroup = mpg;
              rows++; // count the boundary row too
              continue;
            }
          }
          prevMpgInGroup = mpg;
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
  void testOrderByAlias() {
    jparqSql.query("""
        SELECT gear,
        AVG(mpg) AS avgMpg
        FROM mtcars
        GROUP BY gear
        order by avgMpg desc
        """, rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;
        double prev = Double.POSITIVE_INFINITY;
        List<Integer> gears = new ArrayList<>();
        List<Double> avgMpgs = new ArrayList<>();

        while (rs.next()) {
          int gear = rs.getInt("gear");
          double mpg = rs.getDouble("avgMpg");
          gears.add(gear);
          avgMpgs.add(mpg);
          seen.add(gear + " " + mpg);

          // non-increasing
          assertTrue(mpg <= prev, "avgMpg must be non-increasing: " + String.join(", ", seen));
          prev = mpg;
          rows++;
        }
        assertEquals(3, rows, "Expected 3 rows");
        double[] expectedAvgMpgs = Stream.of(24.533, 21.38, 16.107).mapToDouble(Double::doubleValue).toArray();
        double[] actualAvgMpgs = avgMpgs.stream().mapToDouble(Double::doubleValue).toArray();
        assertArrayEquals(expectedAvgMpgs, actualAvgMpgs, 0.001, "Unexpected avgMpg values");

      } catch (SQLException e) {
        System.err.println(String.join("\n", seen));
        fail(e);
      }
    });
  }

}
