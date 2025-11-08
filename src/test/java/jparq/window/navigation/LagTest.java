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
 * Integration tests covering the SQL standard LAG window navigation function.
 */
public class LagTest {

  private static final double DELTA = 0.0001;

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = LagTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that the LAG function returns the previous mpg value and default value
   * when the offset exceeds the partition.
   */
  @Test
  void testLagProducesPreviousMpgValues() {
    String sql = """
        SELECT model,
               qsec,
               mpg,
               LAG(mpg, 1, 0.0) OVER (ORDER BY qsec ASC, model ASC) AS prev_mpg,
               mpg - LAG(mpg, 1, mpg) OVER (ORDER BY qsec ASC, model ASC) AS mpg_diff
        FROM mtcars
        ORDER BY qsec ASC, model ASC
        """;

    List<String> models = new ArrayList<>();
    List<Double> previousMpg = new ArrayList<>();
    List<Double> mpgDifferences = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          models.add(rs.getString("model"));
          previousMpg.add(rs.getDouble("prev_mpg"));
          mpgDifferences.add(rs.getDouble("mpg_diff"));
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });

    assertEquals("Ford Pantera L", models.get(0), "Fastest car must appear first when ordered by qsec");
    assertEquals(0.0, previousMpg.get(0), DELTA, "First row must use the default value");
    assertEquals(0.0, mpgDifferences.get(0), DELTA, "Difference must use the row value as default");

    assertEquals("Maserati Bora", models.get(1), "Second car must follow qsec ordering");
    assertEquals(15.8, previousMpg.get(1), DELTA, "Previous mpg must match the prior row");
    assertEquals(-0.8, mpgDifferences.get(1), DELTA, "Difference must reflect the change from prior row");

    int camaroIndex = models.indexOf("Camaro Z28");
    assertEquals(2, camaroIndex, "Camaro must be third after ordering by qsec");
    assertEquals(15.0, previousMpg.get(camaroIndex), DELTA,
        "Lag must return Maserati Bora mpg for the Camaro Z28");
    assertEquals(-1.7, mpgDifferences.get(camaroIndex), DELTA,
        "Difference must reflect the drop from the Maserati Bora");

    int dusterIndex = models.indexOf("Duster 360");
    assertEquals(4, dusterIndex, "Duster 360 must appear fifth by qsec ordering");
    assertEquals(19.7, previousMpg.get(dusterIndex), DELTA,
        "Lag must return Ferrari Dino mpg for the Duster 360");
    assertEquals(-5.4, mpgDifferences.get(dusterIndex), DELTA,
        "Difference must reflect the drop from the Ferrari Dino");
  }

  /**
   * Verify that LAG respects partitions and supplied default values.
   */
  @Test
  void testLagRespectsPartitionsAndDefaults() {
    String sql = """
        SELECT cyl,
               model,
               mpg,
               LAG(mpg, 1, -1.0) OVER (PARTITION BY cyl ORDER BY qsec ASC, model ASC) AS prev_mpg
        FROM mtcars
        ORDER BY cyl ASC, qsec ASC, model ASC
        """;

    Map<Integer, Double> previousByCylinder = new HashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          double previous = rs.getDouble("prev_mpg");
          double current = rs.getDouble("mpg");

          if (!previousByCylinder.containsKey(cyl)) {
            assertEquals(-1.0, previous, DELTA,
                "First row in each cylinder partition must use the default value");
          } else {
            assertEquals(previousByCylinder.get(cyl), previous, DELTA,
                "LAG must reference the preceding row within the partition");
          }

          previousByCylinder.put(cyl, current);
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });
  }
}
