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
 * Integration tests covering the AVG window analytic function.
 */
public class AvgTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = AvgTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that a partitioned AVG window returns the same averages as a grouped
   * aggregation.
   */
  @Test
  void testPartitionedAvgMatchesGroupAverages() {
    Map<Integer, Double> averagesByCyl = new HashMap<>();
    String groupingSql = """
        SELECT cyl, AVG(mpg) AS avg_mpg
        FROM mtcars
        GROUP BY cyl
        """;
    jparqSql.query(groupingSql, rs -> {
      try {
        while (rs.next()) {
          averagesByCyl.put(rs.getInt("cyl"), rs.getDouble("avg_mpg"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(averagesByCyl.isEmpty(), "Expected aggregated averages for each cylinder group");

    String windowSql = """
        SELECT cyl, mpg,
               AVG(mpg) OVER (PARTITION BY cyl) AS window_avg_mpg
        FROM mtcars
        ORDER BY cyl, mpg DESC
        """;
    Map<Integer, AtomicInteger> rowsPerCylinder = new HashMap<>();
    jparqSql.query(windowSql, rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          double windowAvg = rs.getDouble("window_avg_mpg");
          Double expected = averagesByCyl.get(cyl);
          Assertions.assertNotNull(expected, "Grouped averages must contain an entry for cylinder " + cyl);
          Assertions.assertEquals(expected.doubleValue(), windowAvg, 1e-9,
              "Partitioned AVG must equal grouped average for cylinder " + cyl);
          rowsPerCylinder.computeIfAbsent(cyl, ignored -> new AtomicInteger()).incrementAndGet();
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(averagesByCyl.keySet(), rowsPerCylinder.keySet(),
        "Each cylinder should appear in both grouped and window queries");
  }

  /**
   * Ensure that the default RANGE frame produces a running average ordered by
   * miles per gallon.
   */
  @Test
  void testRunningAverageUsesDefaultRangeFrame() {
    String sql = """
        SELECT model, mpg, hp,
               AVG(hp) OVER (ORDER BY mpg DESC) AS running_avg_hp
        FROM mtcars
        ORDER BY mpg DESC, hp DESC, model
        """;

    List<Double> mpgValues = new ArrayList<>();
    List<Integer> horsepowerValues = new ArrayList<>();
    List<Double> runningAverages = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          mpgValues.add(rs.getDouble("mpg"));
          horsepowerValues.add(rs.getInt("hp"));
          runningAverages.add(rs.getDouble("running_avg_hp"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(horsepowerValues.isEmpty(), "Expected horsepower values for running average validation");
    Assertions.assertEquals(horsepowerValues.size(), runningAverages.size(),
        "Each horsepower entry must have a running average value");

    List<Double> expected = new ArrayList<>(runningAverages.size());
    double cumulativeSum = 0.0d;
    long cumulativeCount = 0L;
    int index = 0;
    while (index < horsepowerValues.size()) {
      double currentMpg = mpgValues.get(index);
      int groupEnd = index;
      double groupSum = 0.0d;
      long groupCount = 0L;
      while (groupEnd < horsepowerValues.size() && Double.compare(mpgValues.get(groupEnd), currentMpg) == 0) {
        groupSum += horsepowerValues.get(groupEnd);
        groupCount++;
        groupEnd++;
      }
      cumulativeSum += groupSum;
      cumulativeCount += groupCount;
      double average = cumulativeSum / cumulativeCount;
      for (int i = index; i < groupEnd; i++) {
        expected.add(average);
      }
      index = groupEnd;
    }

    Assertions.assertEquals(expected.size(), runningAverages.size(), "Expected running average for each output row");
    for (int i = 0; i < expected.size(); i++) {
      Assertions.assertEquals(expected.get(i), runningAverages.get(i), 1e-9,
          "RANGE frame must accumulate all peers sharing the ORDER BY value");
    }
  }

  /**
   * Validate ROWS frame semantics for bounded AVG windows.
   */
  @Test
  void testRowsFrameBoundaries() {
    String sql = """
        SELECT model, hp,
               AVG(hp) OVER (ORDER BY hp ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS local_avg
        FROM mtcars
        WHERE cyl = 4
        ORDER BY hp, model
        """;

    List<Integer> horsepowerValues = new ArrayList<>();
    List<Double> localAverages = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          horsepowerValues.add(rs.getInt("hp"));
          localAverages.add(rs.getDouble("local_avg"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(horsepowerValues.isEmpty(), "Expected horsepower values for bounded window validation");
    Assertions.assertEquals(horsepowerValues.size(), localAverages.size(),
        "Each horsepower entry must have a bounded window average");

    for (int i = 0; i < horsepowerValues.size(); i++) {
      double expected;
      if (i == 0) {
        expected = horsepowerValues.get(0);
      } else {
        expected = (horsepowerValues.get(i) + horsepowerValues.get(i - 1)) / 2.0d;
      }
      Assertions.assertEquals(expected, localAverages.get(i), 1e-9,
          "ROWS frame should average the current row with the immediate predecessor when available");
    }
  }
}
