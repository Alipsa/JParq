package jparq.windows.navigation;

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
 * Integration tests covering SQL standard window navigation functions that
 * inspect following rows.
 */
public class LeadTest {

  private static final double DELTA = 0.0001;

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = LeadTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that the LEAD function returns the next hp value and supplied default
   * when the offset exceeds the partition.
   */
  @Test
  void testLeadProducesNextHorsepowerValues() {
    String sql = """
        SELECT wt,
               hp,
               LEAD(hp, 1, 0) OVER (ORDER BY wt ASC, model ASC) AS next_hp,
               LEAD(hp, 1, hp) OVER (ORDER BY wt ASC, model ASC) - hp AS hp_diff
        FROM mtcars
        ORDER BY wt ASC, model ASC
        """;

    List<Double> nextHorsepower = new ArrayList<>();
    List<Double> horsepower = new ArrayList<>();
    List<Double> horsepowerDifferences = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          horsepower.add(rs.getDouble("hp"));
          nextHorsepower.add(rs.getDouble("next_hp"));
          horsepowerDifferences.add(rs.getDouble("hp_diff"));
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });

    for (int i = 0; i < nextHorsepower.size(); i++) {
      double expectedNext = (i + 1) < horsepower.size() ? horsepower.get(i + 1) : 0.0;
      assertEquals(expectedNext, nextHorsepower.get(i), DELTA,
          "LEAD must return the horsepower from the following row");
      double diffBaseline = (i + 1) < horsepower.size() ? horsepower.get(i + 1) : horsepower.get(i);
      double expectedDiff = diffBaseline - horsepower.get(i);
      assertEquals(expectedDiff, horsepowerDifferences.get(i), DELTA,
          "Difference must equal the change to the next row");
    }
  }

  /**
   * Verify that LEAD respects partitions and default values within each
   * partition.
   */
  @Test
  void testLeadRespectsPartitionsAndDefaults() {
    String sql = """
        SELECT cyl,
               model,
               hp,
               LEAD(hp, 1, -1.0) OVER (PARTITION BY cyl ORDER BY wt ASC, model ASC) AS next_hp
        FROM mtcars
        ORDER BY cyl ASC, wt ASC, model ASC
        """;

    Map<Integer, List<Double>> horsepowerByCylinder = new HashMap<>();
    Map<Integer, List<Double>> leadByCylinder = new HashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int cylinders = rs.getInt("cyl");
          double nextHp = rs.getDouble("next_hp");
          double currentHp = rs.getDouble("hp");
          horsepowerByCylinder.computeIfAbsent(cylinders, key -> new ArrayList<>()).add(currentHp);
          leadByCylinder.computeIfAbsent(cylinders, key -> new ArrayList<>()).add(nextHp);
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });

    leadByCylinder.forEach((cylinders, nextValues) -> {
      List<Double> hpValues = horsepowerByCylinder.get(cylinders);
      for (int i = 0; i < nextValues.size(); i++) {
        double expected = (i + 1) < hpValues.size() ? hpValues.get(i + 1) : -1.0;
        assertEquals(expected, nextValues.get(i), DELTA,
            "LEAD must reference the following row within the partition or the default value");
      }
    });
  }
}
