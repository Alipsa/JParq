package jparq.window.navigation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering the SQL standard FIRST_VALUE window navigation function.
 */
public class FirstValueTest {

  private static final double DELTA = 0.0001;

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = FirstValueTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that FIRST_VALUE can denormalize the best mpg per cylinder partition.
   */
  @Test
  void testFirstValueReturnsBestMpgPerCylinder() {
    String sql = """
        SELECT cyl,
               model,
               mpg,
               FIRST_VALUE(mpg) OVER (
                 PARTITION BY cyl
                 ORDER BY mpg DESC, model ASC
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS best_mpg
        FROM mtcars
        ORDER BY cyl ASC, mpg DESC, model ASC
        """;

    Map<Integer, Double> expectedBestMpg = Map.of(4, 33.9, 6, 21.4, 8, 19.2);
    Map<Integer, Double> observedBestMpg = new HashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          double mpg = rs.getDouble("mpg");
          double bestMpg = rs.getDouble("best_mpg");

          assertEquals(expectedBestMpg.get(cyl), bestMpg, DELTA,
              "FIRST_VALUE must return the best mpg within the cylinder partition");
          observedBestMpg.putIfAbsent(cyl, bestMpg);
          if (mpg == bestMpg) {
            assertEquals(bestMpg, mpg, DELTA, "Best row must match FIRST_VALUE result");
          }
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });

    assertEquals(expectedBestMpg, observedBestMpg, "Each partition must report the expected best mpg");
  }

  /**
   * Verify that FIRST_VALUE respects ROWS frame specifications that start at the current row.
   */
  @Test
  void testFirstValueWithRowsFrameRespectsCurrentRow() {
    String sql = """
        SELECT cyl,
               model,
               mpg,
               FIRST_VALUE(model) OVER (
                 PARTITION BY cyl
                 ORDER BY mpg DESC, model ASC
                 ROWS BETWEEN CURRENT ROW AND CURRENT ROW
               ) AS first_model
        FROM mtcars
        ORDER BY cyl ASC, mpg DESC, model ASC
        """;

    AtomicInteger rowCount = new AtomicInteger();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          String model = rs.getString("model");
          String firstModel = rs.getString("first_model");
          assertEquals(model, firstModel, "ROWS CURRENT ROW frame must yield the current row value");
          rowCount.incrementAndGet();
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    });

    assertEquals(32, rowCount.get(), "Expected to process all mtcars rows");
  }
}
