package jparq.window.navigation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering the SQL standard NTH_VALUE window navigation
 * function.
 */
public class NthValueTest {

  private static final double DELTA = 0.0001;

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = NthValueTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that NTH_VALUE can denormalize the second highest horsepower per
   * cylinder partition.
   */
  @Test
  void testNthValueReturnsSecondHighestHorsepowerPerCylinder() {
    String sql = """
        SELECT cyl,
               model,
               hp,
               NTH_VALUE(hp, 2) OVER (
                 PARTITION BY cyl
                 ORDER BY hp DESC, model ASC
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS second_hp
        FROM mtcars
        ORDER BY cyl ASC, hp DESC, model ASC
        """;

    Map<Integer, Double> expectedSecondHp = Map.of(4, 109.0, 6, 123.0, 8, 264.0);
    Map<Integer, Double> observedPerCylinder = new HashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          double hp = rs.getDouble("hp");
          double secondHp = rs.getDouble("second_hp");

          assertEquals(expectedSecondHp.get(cyl), secondHp, DELTA,
              "NTH_VALUE must return the expected horsepower within the cylinder partition");
          observedPerCylinder.putIfAbsent(cyl, secondHp);
          if (Math.abs(secondHp - hp) < DELTA) {
            assertEquals(secondHp, hp, DELTA, "Rows matching the second place must equal the NTH_VALUE result");
          }
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });

    assertEquals(expectedSecondHp, observedPerCylinder,
        "Each partition must report the expected second highest horsepower");
  }

  /**
   * Verify that a ROWS CURRENT ROW frame yields null when the frame is smaller
   * than the requested N.
   */
  @Test
  void testNthValueRowsCurrentRowProducesNullWhenFrameTooSmall() {
    String sql = """
        SELECT cyl,
               model,
               NTH_VALUE(mpg, 2) OVER (
                 PARTITION BY cyl
                 ORDER BY mpg DESC
                 ROWS BETWEEN CURRENT ROW AND CURRENT ROW
               ) AS nth_mpg
        FROM mtcars
        ORDER BY cyl ASC, mpg DESC
        """;

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          Double nthMpg = rs.getDouble("nth_mpg");
          if (rs.wasNull()) {
            nthMpg = null;
          }
          assertNull(nthMpg, "Frame of size one must yield NULL for N=2");
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });
  }

  /**
   * Ensure that the default RANGE frame (UNBOUNDED PRECEDING TO CURRENT ROW)
   * grows as rows are processed.
   */
  @Test
  void testNthValueDefaultRangeExtendsWithRows() {
    String sql = """
        SELECT cyl,
               mpg,
               NTH_VALUE(mpg, 2) OVER (
                 PARTITION BY cyl
                 ORDER BY mpg DESC, model ASC
               ) AS second_mpg
        FROM mtcars
        ORDER BY cyl ASC, mpg DESC
        """;

    Map<Integer, List<Double>> partitionMpg = new HashMap<>();
    Map<Integer, List<Double>> partitionNth = new HashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          double mpg = rs.getDouble("mpg");
          double secondMpg = rs.getDouble("second_mpg");
          boolean wasNull = rs.wasNull();

          partitionMpg.computeIfAbsent(cyl, key -> new ArrayList<>()).add(mpg);
          partitionNth.computeIfAbsent(cyl, key -> new ArrayList<>()).add(wasNull ? null : secondMpg);
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });

    for (Map.Entry<Integer, List<Double>> entry : partitionMpg.entrySet()) {
      Integer cyl = entry.getKey();
      List<Double> mpgValues = entry.getValue();
      List<Double> nthValues = partitionNth.get(cyl);
      assertNotNull(nthValues, "Nth values must be collected for each partition");
      if (mpgValues.size() < 2) {
        continue;
      }
      double expectedSecond = mpgValues.get(1);
      for (int i = 0; i < mpgValues.size(); i++) {
        Double nth = nthValues.get(i);
        if (i == 0) {
          assertNull(nth, "First row of the partition must yield NULL for N=2");
        } else {
          assertEquals(expectedSecond, nth, DELTA,
              "Rows after the second position must expose the partition's second value");
        }
      }
    }
  }
}
