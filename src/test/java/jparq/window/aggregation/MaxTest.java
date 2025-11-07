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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests verifying the MAX window analytic function.
 */
public class MaxTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = MaxTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Ensure that partitioned MAX windows match grouped aggregation results.
   */
  @Test
  void testPartitionMaxMatchesGrouped() {
    Map<Integer, Double> maxByGear = new HashMap<>();
    String groupingSql = """
        SELECT gear, MAX(mpg) AS max_mpg
        FROM mtcars
        GROUP BY gear
        """;
    jparqSql.query(groupingSql, rs -> {
      try {
        while (rs.next()) {
          maxByGear.put(rs.getInt("gear"), rs.getDouble("max_mpg"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(maxByGear.isEmpty(), "Expected grouped maximums for each gear value");

    String windowSql = """
        SELECT gear, mpg,
               MAX(mpg) OVER (PARTITION BY gear) AS window_max_mpg
        FROM mtcars
        ORDER BY gear, mpg
        """;

    Map<Integer, AtomicInteger> rowsPerGear = new HashMap<>();
    jparqSql.query(windowSql, rs -> {
      try {
        while (rs.next()) {
          int gear = rs.getInt("gear");
          double windowMax = rs.getDouble("window_max_mpg");
          Double expected = maxByGear.get(gear);
          Assertions.assertNotNull(expected, "Grouped maximums must contain an entry for gear " + gear);
          Assertions.assertEquals(expected.doubleValue(), windowMax, 1e-9,
              "Partitioned MAX must equal grouped maximum for gear " + gear);
          rowsPerGear.computeIfAbsent(gear, ignored -> new AtomicInteger()).incrementAndGet();
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(maxByGear.keySet(), rowsPerGear.keySet(),
        "Each gear value should appear in both grouped and window queries");
  }

  /**
   * Verify that the default RANGE frame yields a cumulative maximum following the
   * ORDER BY clause.
   */
  @Test
  void testRunningMaxUsesDefaultRangeFrame() {
    String sql = """
        SELECT qsec, mpg,
               MAX(mpg) OVER (ORDER BY qsec) AS running_max
        FROM mtcars
        ORDER BY qsec, mpg
        """;

    List<Double> qsecs = new ArrayList<>();
    List<Double> mpgs = new ArrayList<>();
    List<Double> runningMaxes = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          qsecs.add(rs.getDouble("qsec"));
          mpgs.add(rs.getDouble("mpg"));
          runningMaxes.add(rs.getDouble("running_max"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(qsecs.isEmpty(), "Expected rows for running maximum validation");
    Assertions.assertEquals(qsecs.size(), mpgs.size(), "Each qsec entry must have an mpg value");
    Assertions.assertEquals(qsecs.size(), runningMaxes.size(), "Each row must expose a running maximum");

    double cumulativeMax = Double.NEGATIVE_INFINITY;
    int index = 0;
    while (index < qsecs.size()) {
      double currentQsec = qsecs.get(index);
      double groupMax = Double.NEGATIVE_INFINITY;
      int groupEnd = index;
      while (groupEnd < qsecs.size() && Double.compare(qsecs.get(groupEnd), currentQsec) == 0) {
        groupMax = Math.max(groupMax, mpgs.get(groupEnd));
        groupEnd++;
      }
      cumulativeMax = Math.max(cumulativeMax, groupMax);
      for (int i = index; i < groupEnd; i++) {
        Assertions.assertEquals(cumulativeMax, runningMaxes.get(i), 1e-9,
            "Running maximum must include all peers sharing qsec=" + currentQsec);
      }
      index = groupEnd;
    }
  }

  /**
   * Validate that ROWS frames calculate the maximum over the specified sliding
   * window.
   */
  @Test
  void testRowsFrameBoundaries() {
    String sql = """
        SELECT model, mpg,
               MAX(mpg) OVER (ORDER BY mpg ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS local_max
        FROM mtcars
        WHERE gear = 4
        ORDER BY mpg, model
        """;

    List<Double> mpgs = new ArrayList<>();
    List<Double> localMaxes = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          mpgs.add(rs.getDouble("mpg"));
          localMaxes.add(rs.getDouble("local_max"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(mpgs.isEmpty(), "Expected MPG values for sliding maximum validation");
    Assertions.assertEquals(mpgs.size(), localMaxes.size(), "Each MPG value must have a sliding maximum");

    for (int i = 0; i < mpgs.size(); i++) {
      double expected = mpgs.get(i);
      if (i > 0) {
        expected = Math.max(expected, mpgs.get(i - 1));
      }
      Assertions.assertEquals(expected, localMaxes.get(i), 1e-9,
          "ROWS frame should consider the current row and immediate predecessor when available");
    }
  }
}
