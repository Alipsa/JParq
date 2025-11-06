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
 * Integration tests verifying the MIN window analytic function.
 */
public class MinTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = MinTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Ensure that partitioned MIN windows match grouped aggregation results.
   */
  @Test
  void testPartitionMinMatchesGrouped() {
    Map<Integer, Double> minByGear = new HashMap<>();
    String groupingSql = """
        SELECT gear, MIN(mpg) AS min_mpg
        FROM mtcars
        GROUP BY gear
        """;
    jparqSql.query(groupingSql, rs -> {
      try {
        while (rs.next()) {
          minByGear.put(rs.getInt("gear"), rs.getDouble("min_mpg"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(minByGear.isEmpty(), "Expected grouped minimums for each gear value");

    String windowSql = """
        SELECT gear, mpg,
               MIN(mpg) OVER (PARTITION BY gear) AS window_min_mpg
        FROM mtcars
        ORDER BY gear, mpg
        """;

    Map<Integer, AtomicInteger> rowsPerGear = new HashMap<>();
    jparqSql.query(windowSql, rs -> {
      try {
        while (rs.next()) {
          int gear = rs.getInt("gear");
          double windowMin = rs.getDouble("window_min_mpg");
          Double expected = minByGear.get(gear);
          Assertions.assertNotNull(expected, "Grouped minimums must contain an entry for gear " + gear);
          Assertions.assertEquals(expected.doubleValue(), windowMin, 1e-9,
              "Partitioned MIN must equal grouped minimum for gear " + gear);
          rowsPerGear.computeIfAbsent(gear, ignored -> new AtomicInteger()).incrementAndGet();
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(minByGear.keySet(), rowsPerGear.keySet(),
        "Each gear value should appear in both grouped and window queries");
  }

  /**
   * Verify that the default RANGE frame yields a cumulative minimum following the
   * ORDER BY clause.
   */
  @Test
  void testRunningMinUsesDefaultRangeFrame() {
    String sql = """
        SELECT qsec, mpg,
               MIN(mpg) OVER (ORDER BY qsec) AS running_min
        FROM mtcars
        ORDER BY qsec, mpg
        """;

    List<Double> qsecs = new ArrayList<>();
    List<Double> mpgs = new ArrayList<>();
    List<Double> runningMins = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          qsecs.add(rs.getDouble("qsec"));
          mpgs.add(rs.getDouble("mpg"));
          runningMins.add(rs.getDouble("running_min"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(qsecs.isEmpty(), "Expected rows for running minimum validation");
    Assertions.assertEquals(qsecs.size(), mpgs.size(), "Each qsec entry must have an mpg value");
    Assertions.assertEquals(qsecs.size(), runningMins.size(), "Each row must expose a running minimum");

    double cumulativeMin = Double.POSITIVE_INFINITY;
    int index = 0;
    while (index < qsecs.size()) {
      double currentQsec = qsecs.get(index);
      double groupMin = Double.POSITIVE_INFINITY;
      int groupEnd = index;
      while (groupEnd < qsecs.size() && Double.compare(qsecs.get(groupEnd), currentQsec) == 0) {
        groupMin = Math.min(groupMin, mpgs.get(groupEnd));
        groupEnd++;
      }
      cumulativeMin = Math.min(cumulativeMin, groupMin);
      for (int i = index; i < groupEnd; i++) {
        Assertions.assertEquals(cumulativeMin, runningMins.get(i), 1e-9,
            "Running minimum must include all peers sharing qsec=" + currentQsec);
      }
      index = groupEnd;
    }
  }

  /**
   * Validate that ROWS frames calculate the minimum over the specified sliding
   * window.
   */
  @Test
  void testRowsFrameBoundaries() {
    String sql = """
        SELECT model, mpg,
               MIN(mpg) OVER (ORDER BY mpg ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS local_min
        FROM mtcars
        WHERE gear = 4
        ORDER BY mpg, model
        """;

    List<Double> mpgs = new ArrayList<>();
    List<Double> localMins = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          mpgs.add(rs.getDouble("mpg"));
          localMins.add(rs.getDouble("local_min"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(mpgs.isEmpty(), "Expected MPG values for sliding minimum validation");
    Assertions.assertEquals(mpgs.size(), localMins.size(), "Each MPG value must have a sliding minimum");

    for (int i = 0; i < mpgs.size(); i++) {
      double expected = mpgs.get(i);
      if (i > 0) {
        expected = Math.min(expected, mpgs.get(i - 1));
      }
      Assertions.assertEquals(expected, localMins.get(i), 1e-9,
          "ROWS frame should consider the current row and immediate predecessor when available");
    }
  }
}
