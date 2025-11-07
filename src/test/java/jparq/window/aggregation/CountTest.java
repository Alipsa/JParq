package jparq.window.aggregation;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering the COUNT window analytic function.
 */
public class CountTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = CountTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that COUNT(*) OVER (PARTITION BY ...) matches grouped totals.
   */
  @Test
  void testPartitionedCountMatchesGroupTotals() {
    Map<Integer, Integer> totalsByGear = new HashMap<>();
    String groupingSql = """
        SELECT gear, COUNT(*) AS total_per_gear
        FROM mtcars
        GROUP BY gear
        """;
    jparqSql.query(groupingSql, rs -> {
      try {
        while (rs.next()) {
          totalsByGear.put(rs.getInt("gear"), rs.getInt("total_per_gear"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(totalsByGear.isEmpty(), "Expected aggregated totals for each gear group");

    String windowSql = """
        SELECT gear, model,
               COUNT(*) OVER (PARTITION BY gear) AS window_count
        FROM mtcars
        ORDER BY gear, model
        """;
    Map<Integer, AtomicInteger> rowsPerGear = new HashMap<>();
    jparqSql.query(windowSql, rs -> {
      try {
        while (rs.next()) {
          int gear = rs.getInt("gear");
          int windowCount = rs.getInt("window_count");
          Integer expectedTotal = totalsByGear.get(gear);
          Assertions.assertNotNull(expectedTotal, "Grouped totals must contain an entry for gear " + gear);
          Assertions.assertEquals(expectedTotal.intValue(), windowCount,
              "Partitioned COUNT(*) must equal grouped total for gear " + gear);
          rowsPerGear.computeIfAbsent(gear, ignored -> new AtomicInteger()).incrementAndGet();
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(totalsByGear.keySet(), rowsPerGear.keySet(),
        "Each gear should appear in both grouped and window queries");
  }

  /**
   * Ensure that the default RANGE frame produces a running COUNT ordered by qsec.
   */
  @Test
  void testRunningCountUsesDefaultRangeFrame() {
    String sql = """
        SELECT qsec, model,
               COUNT(*) OVER (ORDER BY qsec ASC) AS running_count
        FROM mtcars
        ORDER BY qsec ASC, model
        """;

    List<Double> qsecs = new ArrayList<>();
    List<Integer> runningCounts = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          qsecs.add(rs.getDouble("qsec"));
          runningCounts.add(rs.getInt("running_count"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(qsecs.isEmpty(), "Expected qsec values for running count validation");
    Assertions.assertEquals(qsecs.size(), runningCounts.size(), "Each qsec entry must have a running count value");

    Map<Double, Integer> countsByQsec = new TreeMap<>();
    for (double qsec : qsecs) {
      countsByQsec.merge(qsec, 1, Integer::sum);
    }

    int cumulative = 0;
    Map<Double, Integer> expectedByQsec = new HashMap<>();
    for (Map.Entry<Double, Integer> entry : countsByQsec.entrySet()) {
      cumulative += entry.getValue();
      expectedByQsec.put(entry.getKey(), cumulative);
    }

    for (int i = 0; i < qsecs.size(); i++) {
      double qsec = qsecs.get(i);
      int running = runningCounts.get(i);
      int expected = expectedByQsec.get(qsec);
      Assertions.assertEquals(expected, running, "RANGE frame must accumulate all peers sharing the ORDER BY value");
    }
  }

  /**
   * Validate that COUNT(expression) ignores NULL values produced by the
   * expression.
   */
  @Test
  void testCountExpressionSkipsNulls() {
    Map<Integer, Integer> expectedCounts = new HashMap<>();
    String aggregateSql = """
        SELECT gear,
               COUNT(CASE WHEN carb > 3 THEN carb END) AS high_carb
        FROM mtcars
        GROUP BY gear
        """;
    jparqSql.query(aggregateSql, rs -> {
      try {
        while (rs.next()) {
          expectedCounts.put(rs.getInt("gear"), rs.getInt("high_carb"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    String windowSql = """
        SELECT gear, model,
               COUNT(CASE WHEN carb > 3 THEN carb END)
                 OVER (PARTITION BY gear) AS window_high_carb
        FROM mtcars
        ORDER BY gear, model
        """;

    jparqSql.query(windowSql, rs -> {
      try {
        while (rs.next()) {
          int gear = rs.getInt("gear");
          int windowCount = rs.getInt("window_high_carb");
          Integer expected = expectedCounts.get(gear);
          Assertions.assertNotNull(expected, "Expected grouped COUNT value for gear " + gear);
          Assertions.assertEquals(expected.intValue(), windowCount,
              "COUNT(expression) must ignore NULLs and match grouped result");
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }

  /**
   * Verify that COUNT(DISTINCT ...) window expressions are currently unsupported.
   */
  @Test
  void testCountDistinctIsRejected() {
    String sql = """
        SELECT gear,
               COUNT(DISTINCT model) OVER (PARTITION BY gear)
        FROM mtcars
        """;

    RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> jparqSql.query(sql, rs -> {
      // No-op; execution should fail during query preparation.
    }));

    Throwable cause = exception.getCause();
    Assertions.assertNotNull(cause, "Wrapped exception should expose the underlying cause");
    Assertions.assertTrue(cause instanceof java.sql.SQLException, "Expected a SQLException describing the failure");
    Throwable rootCause = cause.getCause();
    Assertions.assertTrue(rootCause instanceof IllegalArgumentException,
        "Root cause should be an IllegalArgumentException rejecting DISTINCT");
    Assertions.assertTrue(rootCause.getMessage().contains("COUNT does not support DISTINCT"),
        "Error message should indicate that COUNT DISTINCT is unsupported");
  }
}
