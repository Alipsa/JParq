package jparq.window.ranking;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering the SQL standard RANK and DENSE_RANK window
 * functions.
 */
public class RankTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = RankTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be present on the test classpath");
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that RANK follows the SQL specification using an aggregate ordering
   * example from the mtcars data set.
   */
  @Test
  void testRankFollowsAggregateOrdering() {
    String sql = """
        WITH GearAverages AS (
            SELECT gear, AVG(mpg) AS avg_mpg
            FROM mtcars
            GROUP BY gear
        )
        SELECT gear, avg_mpg,
               RANK() OVER (ORDER BY avg_mpg DESC) AS mpg_rank
        FROM GearAverages
        ORDER BY avg_mpg DESC
        """;

    List<Integer> gears = new ArrayList<>();
    List<Double> averages = new ArrayList<>();
    List<Long> ranks = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          gears.add(rs.getInt("gear"));
          averages.add(rs.getDouble("avg_mpg"));
          ranks.add(rs.getLong("mpg_rank"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(List.of(4, 5, 3), gears, "Gear ranking must follow descending average mpg");
    Assertions.assertEquals(3, averages.size(), "Expected one aggregate row per gear value");
    Assertions.assertEquals(3, ranks.size(), "Expected a rank value for each aggregated row");
    Assertions.assertEquals(List.of(1L, 2L, 3L), ranks, "Ranks must start at one and increase sequentially");
    Assertions.assertEquals(24.533, averages.get(0), 0.001, "Gear 4 should have the highest average mpg");
    Assertions.assertEquals(21.380, averages.get(1), 0.001, "Gear 5 should have the second highest average mpg");
    Assertions.assertEquals(16.107, averages.get(2), 0.001, "Gear 3 should have the lowest average mpg");
  }

  @Test
  void testRankFollowsRankOrdering() {
    String sql = """
        WITH GearAverages AS (
          SELECT gear, AVG(mpg) AS avg_mpg
          FROM mtcars
          GROUP BY gear
        )
        SELECT gear, avg_mpg,
        RANK() OVER (ORDER BY avg_mpg DESC) AS mpg_rank
        FROM GearAverages
        ORDER BY mpg_rank
        """;

    List<Integer> gears = new ArrayList<>();
    List<Double> averages = new ArrayList<>();
    List<Long> ranks = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          gears.add(rs.getInt("gear"));
          averages.add(rs.getDouble("avg_mpg"));
          ranks.add(rs.getLong("mpg_rank"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(List.of(4, 5, 3), gears, "Gear ranking must follow descending average mpg");
    Assertions.assertEquals(3, averages.size(), "Expected one aggregate row per gear value");
    Assertions.assertEquals(3, ranks.size(), "Expected a rank value for each aggregated row");
    Assertions.assertEquals(List.of(1L, 2L, 3L), ranks, "Ranks must start at one and increase sequentially");
    Assertions.assertEquals(24.533, averages.get(0), 0.001, "Gear 4 should have the highest average mpg");
    Assertions.assertEquals(21.380, averages.get(1), 0.001, "Gear 5 should have the second highest average mpg");
    Assertions.assertEquals(16.107, averages.get(2), 0.001, "Gear 3 should have the lowest average mpg");
  }

  /**
   * Ensure that RANK assigns identical values to ties and introduces gaps after
   * groups of equal ORDER BY keys.
   */
  @Test
  void testRankHandlesTiesWithGaps() {
    LinkedHashMap<Long, Long> countsByCyl = new LinkedHashMap<>();
    jparqSql.query("SELECT cyl, COUNT(*) AS cnt FROM mtcars GROUP BY cyl ORDER BY cyl", rs -> {
      try {
        while (rs.next()) {
          long cyl = toLong(rs.getObject("cyl"));
          long count = toLong(rs.getObject("cnt"));
          countsByCyl.put(cyl, count);
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
    Assertions.assertFalse(countsByCyl.isEmpty(), "Expected cylinder counts from the base data set");

    LinkedHashMap<Long, Long> expectedRankByCyl = new LinkedHashMap<>();
    long processed = 0L;
    for (Map.Entry<Long, Long> entry : countsByCyl.entrySet()) {
      expectedRankByCyl.put(entry.getKey(), processed + 1);
      processed += entry.getValue();
    }
    final long totalRows = processed;

    String sql = """
        WITH RankedCars AS (
            SELECT model, cyl,
                   RANK() OVER (ORDER BY cyl) AS cyl_rank
            FROM mtcars
        )
        SELECT cyl, cyl_rank
        FROM RankedCars
        ORDER BY cyl
        """;

    LinkedHashMap<Long, Long> observedRanks = new LinkedHashMap<>();
    LinkedHashMap<Long, Long> observedCounts = new LinkedHashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          long cyl = toLong(rs.getObject("cyl"));
          long rank = toLong(rs.getObject("cyl_rank"));

          Long expectedRank = expectedRankByCyl.get(cyl);
          Assertions.assertNotNull(expectedRank, "Unexpected cylinder value in ranked result: " + cyl);
          Assertions.assertEquals(expectedRank.longValue(), rank,
              "Rank must advance by the number of rows processed so far");

          Long existing = observedRanks.putIfAbsent(cyl, rank);
          if (existing != null) {
            Assertions.assertEquals(existing.longValue(), rank, "All tied rows must share the same rank");
          }
          observedCounts.merge(cyl, 1L, Long::sum);
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(expectedRankByCyl.keySet(), observedRanks.keySet(),
        "Each cylinder value must produce a rank");
    Assertions.assertEquals(totalRows, countsByCyl.values().stream().mapToLong(Long::longValue).sum(),
        "Base counts should cover all vehicles");
    for (Map.Entry<Long, Long> entry : countsByCyl.entrySet()) {
      long cyl = entry.getKey();
      long baseCount = entry.getValue();
      long rankedCount = observedCounts.getOrDefault(cyl, 0L);
      Assertions.assertTrue(rankedCount >= baseCount,
          "Ranked rows must include every row from the base data set for cylinder " + cyl);
    }
  }

  /**
   * Verify that DENSE_RANK assigns consecutive ranks without gaps for identical
   * ORDER BY values.
   */
  @Test
  void testDenseRankProducesNoGaps() {
    String sql = """
        SELECT model, hp,
               DENSE_RANK() OVER (ORDER BY hp DESC) AS hp_dense_rank
        FROM mtcars
        ORDER BY hp DESC, model
        """;

    List<Integer> horsepower = new ArrayList<>();
    List<Long> denseRanks = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          horsepower.add(rs.getInt("hp"));
          denseRanks.add(rs.getLong("hp_dense_rank"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(horsepower.isEmpty(), "Expected horsepower values from the base data set");
    Assertions.assertEquals(horsepower.size(), denseRanks.size(), "Each row must produce a dense rank value");

    LinkedHashMap<Integer, Long> expectedRankByHp = new LinkedHashMap<>();
    long nextRank = 1L;
    Integer previousHp = null;
    for (int i = 0; i < horsepower.size(); i++) {
      Integer hp = horsepower.get(i);
      if (!hp.equals(previousHp)) {
        expectedRankByHp.putIfAbsent(hp, nextRank);
        previousHp = hp;
        nextRank++;
      }
      long observedRank = denseRanks.get(i);
      long expectedRank = expectedRankByHp.get(hp);
      Assertions.assertEquals(expectedRank, observedRank,
          "Dense rank must remain constant for equal ORDER BY values and advance without gaps");
    }

    long uniqueHpCount = expectedRankByHp.size();
    for (long rank = 1L; rank <= uniqueHpCount; rank++) {
      Assertions.assertTrue(expectedRankByHp.containsValue(rank),
          "Dense rank values must cover every integer from 1 to the number of unique ORDER BY values");
    }
  }

  /**
   * Verify that DENSE_RANK restarts for each partition and remains gapless within
   * partitions.
   */
  @Test
  void testDenseRankRespectsPartitions() {
    String sql = """
        SELECT cyl, hp,
               DENSE_RANK() OVER (PARTITION BY cyl ORDER BY hp DESC) AS cyl_hp_dense_rank
        FROM mtcars
        ORDER BY cyl, cyl_hp_dense_rank, hp DESC
        """;

    Map<Long, Integer> previousHpByCyl = new HashMap<>();
    Map<Long, Long> currentRankByCyl = new HashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          long cyl = toLong(rs.getObject("cyl"));
          int hp = rs.getInt("hp");
          long denseRank = rs.getLong("cyl_hp_dense_rank");

          Integer previousHp = previousHpByCyl.get(cyl);
          long expectedRank;
          if (previousHp == null) {
            expectedRank = 1L;
          } else if (previousHp.intValue() == hp) {
            expectedRank = currentRankByCyl.get(cyl);
          } else {
            expectedRank = currentRankByCyl.get(cyl) + 1L;
          }

          Assertions.assertEquals(expectedRank, denseRank,
              "Dense rank must reset and advance sequentially within each partition");

          previousHpByCyl.put(cyl, hp);
          currentRankByCyl.put(cyl, expectedRank);
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(previousHpByCyl.isEmpty(), "Expected to observe at least one partition");
  }

  /**
   * Convert a numeric result value to a primitive {@code long}.
   *
   * @param value
   *          the raw value retrieved from a {@link java.sql.ResultSet}
   * @return the numeric value as a {@code long}
   */
  private static long toLong(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof CharSequence sequence) {
      return Long.parseLong(sequence.toString());
    }
    throw new IllegalArgumentException("Unexpected numeric value type: " + value);
  }
}
