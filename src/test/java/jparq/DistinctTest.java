package jparq;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for distinct operations. */
public class DistinctTest {

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
  void testDistinct() {
    jparqSql.query("select distinct mpg from mtcars", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount(), "Expected 1 column");

        int rows = 0;

        while (rs.next()) {
          String model = rs.getString("mpg");
          seen.add(model);
          rows++;
        }

        // se.alipsa.matrix.datasets.Dataset.mtcars()['mpg'].unique().size()
        assertEquals(25, rows, "Expected 25 rows");
      } catch (SQLException e) {
        System.err.println(String.join("\n", seen));
        fail(e);
      }
    });
  }

  // java
  @Test
  void testDistinctCylBetweenMpg() {
    jparqSql.query("select distinct cyl from mtcars where mpg between 10 and 20", rs -> {
      List<Integer> vals = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount(), "Expected 1 column (cyl)");

        while (rs.next()) {
          vals.add(rs.getInt(1));
        }

        // Expect only {6, 8} for mpg in [10, 20]
        assertEquals(2, vals.size(), "Expected 2 distinct cyl values");
        // Order of DISTINCT without ORDER BY is not guaranteed; assert membership
        boolean has6 = vals.contains(6);
        boolean has8 = vals.contains(8);
        if (!has6 || !has8) {
          fail("Expected cyl to contain 6 and 8, got: " + vals);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testDistinctCylWhereModelLikeMerc() {
    jparqSql.query("select distinct cyl from mtcars where model like('Merc%')", rs -> {
      List<Integer> vals = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount(), "Expected 1 column (cyl)");

        while (rs.next()) {
          vals.add(rs.getInt(1));
        }

        // Expect {4, 6, 8} among 'Merc%' models
        assertEquals(3, vals.size(), "Expected 3 distinct cyl values for Merc%");
        boolean has4 = vals.contains(4);
        boolean has6 = vals.contains(6);
        boolean has8 = vals.contains(8);
        if (!has4 || !has6 || !has8) {
          fail("Expected cyl to contain 4, 6, 8, got: " + vals);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testDistinctCylGearOrderByLimit() {
    jparqSql.query("select distinct cyl, gear from mtcars order by cyl desc, gear asc limit 5", rs -> {
      List<String> rows = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns (cyl, gear)");

        while (rs.next()) {
          rows.add(rs.getInt(1) + ":" + rs.getInt(2)); // "cyl:gear"
        }

        // DISTINCT before ORDER BY; then LIMIT 5
        // Expected top-5 pairs by cyl DESC then gear ASC:
        // 8:{3,5}, 6:{3,4,5}, ...
        List<String> expected = List.of("8:3", "8:5", "6:3", "6:4", "6:5");
        assertEquals(expected, rows, "Unexpected order/limit of distinct (cyl, gear)");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }
}