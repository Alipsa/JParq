package jparq.window.navigation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
 * Integration tests covering the SQL standard LAST_VALUE window navigation
 * function.
 */
public class LastValueTest {

  private static final double DELTA = 0.0001;

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = LastValueTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that LAST_VALUE can denormalize the heaviest weight per cylinder
   * partition.
   */
  @Test
  void testLastValueReturnsHeaviestWeightPerCylinder() {
    String sql = """
        SELECT cyl,
               model,
               wt,
               LAST_VALUE(wt) OVER (
                 PARTITION BY cyl
                 ORDER BY wt ASC, model ASC
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS max_wt
        FROM mtcars
        ORDER BY cyl ASC, wt DESC, model ASC
        """;

    Map<Integer, Double> expectedHeaviestWeight = Map.of(4, 3.19, 6, 3.46, 8, 5.424);
    Map<Integer, Double> observedWeights = new HashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          double weight = rs.getDouble("wt");
          double maxWeight = rs.getDouble("max_wt");

          assertEquals(expectedHeaviestWeight.get(cyl), maxWeight, DELTA,
              "LAST_VALUE must return the heaviest weight within the cylinder partition");
          observedWeights.putIfAbsent(cyl, maxWeight);
          if (Math.abs(maxWeight - weight) < DELTA) {
            assertEquals(maxWeight, weight, DELTA, "Rows with the heaviest weight must match LAST_VALUE result");
          }
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });

    assertEquals(expectedHeaviestWeight, observedWeights, "Each partition must report the expected heaviest weight");
  }

  /**
   * Verify that the default RANGE frame extends to include peers that share the
   * same ordering keys.
   */
  @Test
  void testLastValueDefaultRangeIncludesPeers() {
    String sql = """
        SELECT cyl,
               model,
               LAST_VALUE(model) OVER (
                 ORDER BY cyl ASC
               ) AS last_model,
               ROW_NUMBER() OVER (
                 ORDER BY cyl ASC
               ) AS seq
        FROM mtcars
        ORDER BY cyl ASC, seq ASC
        """;

    List<Integer> cylinders = new ArrayList<>();
    List<String> models = new ArrayList<>();
    List<String> lastModels = new ArrayList<>();
    List<Integer> sequences = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          cylinders.add(rs.getInt("cyl"));
          models.add(rs.getString("model"));
          lastModels.add(rs.getString("last_model"));
          sequences.add(rs.getInt("seq"));
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });

    assertEquals(32, cylinders.size(), "Expected to process all mtcars rows");

    Map<Integer, Integer> maxSequencePerCylinder = new HashMap<>();
    Map<Integer, String> lastModelPerCylinder = new HashMap<>();
    for (int i = 0; i < cylinders.size(); i++) {
      int cyl = cylinders.get(i);
      int sequence = sequences.get(i);
      if (sequence >= maxSequencePerCylinder.getOrDefault(cyl, Integer.MIN_VALUE)) {
        maxSequencePerCylinder.put(cyl, sequence);
        lastModelPerCylinder.put(cyl, models.get(i));
      }
    }

    for (int i = 0; i < cylinders.size(); i++) {
      int cyl = cylinders.get(i);
      String expectedLastModel = lastModelPerCylinder.get(cyl);
      assertEquals(expectedLastModel, lastModels.get(i),
          "Default RANGE frame must extend to the last peer within the ORDER BY group");
    }
  }

  /**
   * Verify that a ROWS frame extending to UNBOUNDED FOLLOWING yields the
   * partition tail.
   */
  @Test
  void testLastValueRowsFollowingReturnsPartitionTail() {
    String sql = """
        SELECT cyl,
               wt,
               LAST_VALUE(wt) OVER (
                 PARTITION BY cyl
                 ORDER BY wt ASC
                 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
               ) AS tail_weight
        FROM mtcars
        ORDER BY cyl ASC, wt ASC
        """;

    Map<Integer, Double> expectedHeaviestWeight = Map.of(4, 3.19, 6, 3.46, 8, 5.424);

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          double tailWeight = rs.getDouble("tail_weight");
          assertEquals(expectedHeaviestWeight.get(cyl), tailWeight, DELTA,
              "ROWS CURRENT ROW TO UNBOUNDED FOLLOWING must yield the partition tail value");
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });
  }
}
