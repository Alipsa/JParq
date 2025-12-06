package jparq.aggregate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for aggregate functions. */
public class AggregateFunctionsTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = AggregateFunctionsTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testCount() {
    final long[] expectedCount = new long[1];

    jparqSql.query("SELECT mpg FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          expectedCount[0]++;
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    jparqSql.query("SELECT COUNT(*) AS total_rows, COUNT(mpg) AS mpg_count FROM mtcars", rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected two aggregate columns");

        assertTrue(rs.next(), "Aggregate query should return one row");
        assertEquals(expectedCount[0], rs.getLong("total_rows"));
        assertEquals(expectedCount[0], rs.getLong(2));
        assertFalse(rs.next(), "Only one aggregated row expected");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testSum() {
    final double[] manualSum = new double[1];

    jparqSql.query("SELECT hp FROM mtcars WHERE cyl = 4", rs -> {
      try {
        while (rs.next()) {
          manualSum[0] += rs.getDouble(1);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    jparqSql.query("SELECT SUM(hp) AS total_hp FROM mtcars WHERE cyl = 4", rs -> {
      try {
        assertTrue(rs.next());
        assertEquals(manualSum[0], rs.getDouble("total_hp"), 1e-8);
        assertFalse(rs.wasNull());
        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testAvg() {
    final double[] sum = new double[1];
    final int[] count = new int[1];

    jparqSql.query("SELECT mpg FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          sum[0] += rs.getDouble(1);
          count[0]++;
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    double expectedAvg = sum[0] / count[0];

    jparqSql.query("SELECT AVG(mpg) AS avg_mpg FROM mtcars", rs -> {
      try {
        assertTrue(rs.next());
        assertEquals(expectedAvg, rs.getDouble(1), 1e-10);
        assertEquals(expectedAvg, rs.getDouble("avg_mpg"), 1e-10);
        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testMin() {
    final double[] manualMin = {
        Double.POSITIVE_INFINITY
    };

    jparqSql.query("SELECT mpg FROM mtcars WHERE cyl = 8", rs -> {
      try {
        while (rs.next()) {
          manualMin[0] = Math.min(manualMin[0], rs.getDouble(1));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    jparqSql.query("SELECT MIN(mpg) AS min_mpg FROM mtcars WHERE cyl = 8", rs -> {
      try {
        assertTrue(rs.next());
        assertEquals(manualMin[0], rs.getDouble("min_mpg"), 1e-10);
        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testMax() {
    final double[] manualMax = {
        Double.NEGATIVE_INFINITY
    };

    jparqSql.query("SELECT hp FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          manualMax[0] = Math.max(manualMax[0], rs.getDouble(1));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    jparqSql.query("SELECT MAX(hp) FROM mtcars", rs -> {
      try {
        assertTrue(rs.next());
        assertEquals(manualMax[0], rs.getDouble(1), 1e-10);
        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testGroupByExpressionMatching() {
    Map<Integer, Long> expectedCounts = new LinkedHashMap<>();

    jparqSql.query("SELECT gear AS grp, COUNT(*) AS cnt FROM mtcars GROUP BY gear", rs -> {
      try {
        while (rs.next()) {
          expectedCounts.put(rs.getInt("grp"), rs.getLong("cnt"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertFalse(expectedCounts.isEmpty(), "Baseline grouping query should produce results");

    jparqSql.query("SELECT COALESCE(gear, 0) AS grp, COUNT(*) AS cnt FROM mtcars GROUP BY COALESCE(gear, 0)", rs -> {
      try {
        int rows = 0;
        while (rs.next()) {
          rows++;
          int groupValue = rs.getInt("grp");
          long count = rs.getLong("cnt");
          Long expected = expectedCounts.get(groupValue);
          assertNotNull(expected, "Unexpected group value returned: " + groupValue);
          assertEquals(expected.longValue(), count, "Functional GROUP BY expression should match base grouping counts");
        }
        assertEquals(expectedCounts.size(), rows, "COALESCE-based grouping should yield the same number of buckets");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void correlatedScalarSubqueryInAggregateUsesAlias() {
    Map<Integer, Double> expectedMax = new LinkedHashMap<>();

    jparqSql.query("SELECT cyl, MAX(hp) AS max_hp FROM mtcars GROUP BY cyl ORDER BY cyl", rs -> {
      try {
        while (rs.next()) {
          expectedMax.put(rs.getInt("cyl"), rs.getDouble("max_hp"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertFalse(expectedMax.isEmpty(), "Baseline aggregate query should produce results");

    jparqSql.query("SELECT cyl AS grp, MAX(hp) AS max_hp, "
        + "MAX((SELECT MAX(m2.hp) FROM mtcars m2 WHERE m2.cyl = mc.grp)) AS correlated_max "
        + "FROM mtcars mc GROUP BY cyl ORDER BY grp", rs -> {
          try {
            int rows = 0;
            while (rs.next()) {
              rows++;
              int group = rs.getInt("grp");
              double maxHp = rs.getDouble("max_hp");
              double correlatedMax = rs.getDouble("correlated_max");
              Double expected = expectedMax.get(group);
              assertNotNull(expected, "Unexpected group value returned: " + group);
              assertEquals(expected, maxHp, 1e-9);
              assertEquals(expected, correlatedMax, 1e-9);
            }
            assertEquals(expectedMax.size(), rows, "All groups should emit correlated aggregate values");
          } catch (SQLException e) {
            fail(e);
          }
        });
  }
}
