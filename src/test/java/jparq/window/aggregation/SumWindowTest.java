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
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering the SUM window analytic function.
 */
public class SumWindowTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = SumWindowTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that a partitioned SUM window returns the same totals as a grouped aggregation.
   */
  @Test
  void testPartitionedSumMatchesGroupTotals() {
    Map<Integer, Long> totalsByCyl = new HashMap<>();
    String groupingSql = """
        SELECT cyl, SUM(hp) AS total_hp
        FROM mtcars
        GROUP BY cyl
        """;
    jparqSql.query(groupingSql, rs -> {
      try {
        while (rs.next()) {
          totalsByCyl.put(rs.getInt("cyl"), rs.getLong("total_hp"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(totalsByCyl.isEmpty(), "Expected aggregated totals for each cylinder group");

    String windowSql = """
        SELECT cyl, hp,
               SUM(hp) OVER (PARTITION BY cyl) AS window_total_hp
        FROM mtcars
        ORDER BY cyl, hp
        """;
    Map<Integer, AtomicInteger> rowsPerCylinder = new HashMap<>();
    jparqSql.query(windowSql, rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          long windowTotal = rs.getLong("window_total_hp");
          Long expectedTotal = totalsByCyl.get(cyl);
          Assertions.assertNotNull(expectedTotal,
              "Grouped totals must contain an entry for cylinder " + cyl);
          Assertions.assertEquals(expectedTotal.longValue(), windowTotal,
              "Partitioned SUM must equal grouped total for cylinder " + cyl);
          rowsPerCylinder.computeIfAbsent(cyl, ignored -> new AtomicInteger()).incrementAndGet();
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(totalsByCyl.keySet(), rowsPerCylinder.keySet(),
        "Each cylinder should appear in both grouped and window queries");
  }

  /**
   * Ensure that the default RANGE frame produces a running sum ordered by horsepower.
   */
  @Test
  void testRunningSumUsesDefaultRangeFrame() {
    String sql = """
        SELECT model, hp,
               SUM(hp) OVER (ORDER BY hp) AS running_hp
        FROM mtcars
        ORDER BY hp, model
        """;

    List<Integer> horsepowers = new ArrayList<>();
    List<Long> runningTotals = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          horsepowers.add(rs.getInt("hp"));
          runningTotals.add(rs.getLong("running_hp"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(horsepowers.isEmpty(), "Expected horsepower values for running sum validation");
    Assertions.assertEquals(horsepowers.size(), runningTotals.size(),
        "Each horsepower entry must have a running sum value");

    Map<Integer, Long> totalsByHorsepower = new HashMap<>();
    for (int hp : horsepowers) {
      totalsByHorsepower.merge(hp, (long) hp, Long::sum);
    }

    long cumulative = 0L;
    Map<Integer, Long> expectedByHorsepower = new HashMap<>();
    for (int hpValue : new TreeSet<>(totalsByHorsepower.keySet())) {
      cumulative += totalsByHorsepower.get(hpValue);
      expectedByHorsepower.put(hpValue, cumulative);
    }

    for (int i = 0; i < horsepowers.size(); i++) {
      int hp = horsepowers.get(i);
      long runningTotal = runningTotals.get(i);
      long expected = expectedByHorsepower.get(hp);
      Assertions.assertEquals(expected, runningTotal,
          "RANGE frame must accumulate all peers sharing the ORDER BY value");
    }
  }

  /**
   * Validate ROWS frame semantics for bounded SUM windows.
   */
  @Test
  void testRowsFrameBoundaries() {
    String sql = """
        SELECT model, hp,
               SUM(hp) OVER (ORDER BY hp ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS local_sum
        FROM mtcars
        WHERE cyl = 4
        ORDER BY hp, model
        """;

    List<Integer> horsepowers = new ArrayList<>();
    List<Long> localSums = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          horsepowers.add(rs.getInt("hp"));
          localSums.add(rs.getLong("local_sum"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(horsepowers.isEmpty(), "Expected horsepower values for bounded window validation");
    Assertions.assertEquals(horsepowers.size(), localSums.size(),
        "Each horsepower entry must have a bounded window sum");

    for (int i = 0; i < horsepowers.size(); i++) {
      long expected = horsepowers.get(i);
      if (i > 0) {
        expected += horsepowers.get(i - 1);
      }
      Assertions.assertEquals(expected, localSums.get(i),
          "ROWS frame should sum the current row with the immediate predecessor when available");
    }
  }
}
