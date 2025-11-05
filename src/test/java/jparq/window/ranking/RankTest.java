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
 * Integration tests covering the SQL standard RANK, DENSE_RANK, PERCENT_RANK,
 * CUME_DIST and NTILE window functions.
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

  /**
   * Verify that PERCENT_RANK follows the SQL specification using an aggregate
   * ordering example from the mtcars data set.
   */
  @Test
  void testPercentRankFollowsAggregateOrdering() {
    String sql = """
        WITH GearAverages AS (
            SELECT gear, AVG(mpg) AS avg_mpg
            FROM mtcars
            GROUP BY gear
        )
        SELECT gear, avg_mpg,
               PERCENT_RANK() OVER (ORDER BY avg_mpg DESC) AS mpg_percent_rank
        FROM GearAverages
        ORDER BY avg_mpg DESC
        """;

    List<Integer> gears = new ArrayList<>();
    List<Double> averages = new ArrayList<>();
    List<Double> percentRanks = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          gears.add(rs.getInt("gear"));
          averages.add(rs.getDouble("avg_mpg"));
          percentRanks.add(rs.getDouble("mpg_percent_rank"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(List.of(4, 5, 3), gears, "Gear ordering must follow descending average mpg");
    Assertions.assertEquals(3, averages.size(), "Expected one aggregate row per gear value");
    Assertions.assertEquals(3, percentRanks.size(), "Expected a percent rank value for each aggregated row");

    Assertions.assertEquals(0.0, percentRanks.get(0), 0.0001,
        "First row in a partition must receive a percent rank of zero");
    Assertions.assertEquals(0.5, percentRanks.get(1), 0.0001,
        "Second gear should be halfway between the best and worst averages");
    Assertions.assertEquals(1.0, percentRanks.get(2), 0.0001,
        "Final row in a partition must receive a percent rank of one when multiple rows exist");
  }

  /**
   * Verify that CUME_DIST follows the SQL specification using an aggregate
   * ordering example from the mtcars data set.
   */
  @Test
  void testCumeDistFollowsAggregateOrdering() {
    String sql = """
        WITH GearAverages AS (
            SELECT gear, AVG(mpg) AS avg_mpg
            FROM mtcars
            GROUP BY gear
        )
        SELECT gear, avg_mpg,
               CUME_DIST() OVER (ORDER BY avg_mpg DESC) AS mpg_cume_dist
        FROM GearAverages
        ORDER BY avg_mpg DESC
        """;

    List<Integer> gears = new ArrayList<>();
    List<Double> averages = new ArrayList<>();
    List<Double> cumeDists = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          gears.add(rs.getInt("gear"));
          averages.add(rs.getDouble("avg_mpg"));
          cumeDists.add(rs.getDouble("mpg_cume_dist"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(List.of(4, 5, 3), gears, "Gear ordering must follow descending average mpg");
    Assertions.assertEquals(3, averages.size(), "Expected one aggregate row per gear value");
    Assertions.assertEquals(3, cumeDists.size(), "Expected a cumulative distribution value for each aggregated row");

    Assertions.assertEquals(1.0 / 3.0, cumeDists.get(0), 0.0001,
        "First ordered row must receive a cumulative distribution of 1/totalRows");
    Assertions.assertEquals(2.0 / 3.0, cumeDists.get(1), 0.0001,
        "Second row should reflect two thirds of the partitioned rows");
    Assertions.assertEquals(1.0, cumeDists.get(2), 0.0001,
        "Final row in a partition must receive a cumulative distribution of one");
  }

  /**
   * Ensure that queries projecting only the PERCENT_RANK window value still
   * populate the required partition and ordering columns during execution.
   */
  @Test
  void testPercentRankProjectionWithoutUnderlyingColumns() {
    String sql = """
        SELECT PERCENT_RANK() OVER (PARTITION BY cyl ORDER BY mpg) AS mpg_percent_rank
        FROM mtcars
        WHERE cyl = 4
        ORDER BY mpg_percent_rank
        """;

    List<Double> percentRanks = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          percentRanks.add(rs.getDouble("mpg_percent_rank"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(percentRanks.isEmpty(), "Expected percent ranks for four-cylinder cars");
    Assertions.assertEquals(0.0, percentRanks.get(0), 0.0001,
        "First ordered row must receive the minimum percent rank");
    Assertions.assertEquals(1.0, percentRanks.get(percentRanks.size() - 1), 0.0001,
        "Last ordered row must receive the maximum percent rank");

    double previous = Double.NEGATIVE_INFINITY;
    for (double value : percentRanks) {
      Assertions.assertTrue(value >= 0.0 && value <= 1.0, "Percent rank values must fall within [0, 1]");
      Assertions.assertTrue(value >= previous - 0.0000001, "Percent rank values must be non-decreasing after ordering");
      previous = value;
    }
  }

  /**
   * Ensure that queries projecting only the CUME_DIST window value still populate
   * the required partition and ordering columns during execution.
   */
  @Test
  void testCumeDistProjectionWithoutUnderlyingColumns() {
    final long[] totalRows = {
        0L
    };
    final long[] firstGroupCount = {
        0L
    };
    jparqSql.query("""
        SELECT mpg, COUNT(*) AS cnt
        FROM mtcars
        WHERE cyl = 4
        GROUP BY mpg
        ORDER BY mpg
        """, rs -> {
      try {
        boolean first = true;
        while (rs.next()) {
          long count = rs.getLong("cnt");
          totalRows[0] += count;
          if (first) {
            firstGroupCount[0] = count;
            first = false;
          }
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertTrue(totalRows[0] > 0, "Expected rows for the four-cylinder partition");

    String sql = """
        SELECT CUME_DIST() OVER (PARTITION BY cyl ORDER BY mpg) AS mpg_cume_dist
        FROM mtcars
        WHERE cyl = 4
        ORDER BY mpg_cume_dist
        """;

    List<Double> cumeDists = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          cumeDists.add(rs.getDouble("mpg_cume_dist"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(cumeDists.isEmpty(), "Expected cumulative distributions for four-cylinder cars");
    Assertions.assertEquals(1.0, cumeDists.get(cumeDists.size() - 1), 0.0001,
        "Last ordered row must receive a cumulative distribution of one");

    double previous = Double.NEGATIVE_INFINITY;
    for (double value : cumeDists) {
      Assertions.assertTrue(value > 0.0 && value <= 1.0, "Cumulative distribution values must fall within (0, 1]");
      Assertions.assertTrue(value >= previous - 0.0000001,
          "Cumulative distribution values must be non-decreasing after ordering");
      previous = value;
    }
    Assertions.assertEquals((double) firstGroupCount[0] / (double) totalRows[0], cumeDists.get(0), 0.0001,
        "First row must equal the proportion of the partition represented by its peer group");
  }

  /**
   * Verify that NTILE evenly distributes rows across quartiles when the total row
   * count is divisible by the requested bucket count.
   */
  @Test
  void testNtileDividesIntoQuartiles() {
    String sql = """
        SELECT hp,
               NTILE(4) OVER (ORDER BY hp) AS hp_quartile
        FROM mtcars
        ORDER BY hp
        """;

    List<Integer> quartiles = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          quartiles.add(rs.getInt("hp_quartile"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(32, quartiles.size(), "mtcars data set must provide 32 horsepower values");

    Map<Integer, Long> counts = new LinkedHashMap<>();
    int previous = Integer.MIN_VALUE;
    for (int quartile : quartiles) {
      Assertions.assertTrue(quartile >= 1 && quartile <= 4, "Quartile assignment must fall within [1, 4]");
      if (previous != Integer.MIN_VALUE) {
        Assertions.assertTrue(quartile >= previous, "Quartile assignments must be non-decreasing after ordering");
      }
      previous = quartile;
      counts.merge(quartile, 1L, Long::sum);
    }

    for (int tile = 1; tile <= 4; tile++) {
      Assertions.assertEquals(8L, counts.getOrDefault(tile, 0L),
          "Each quartile must contain exactly eight rows in the evenly divisible case");
    }
  }

  /**
   * Ensure that NTILE can execute without an ORDER BY clause and that rows are
   * bucketed according to the input sequence when no ordering is provided.
   */
  @Test
  void testNtileWithoutOrderByClause() {
    String sql = """
        SELECT NTILE(3) OVER () AS bucket
        FROM mtcars
        ORDER BY bucket
        """;

    Map<Integer, Long> counts = new LinkedHashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int bucket = rs.getInt("bucket");
          Assertions.assertTrue(bucket >= 1 && bucket <= 3, "Bucket assignments must fall within [1, 3]");
          counts.merge(bucket, 1L, Long::sum);
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(32L, counts.values().stream().mapToLong(Long::longValue).sum(),
        "NTILE must process every row in the mtcars data set");
    Assertions.assertEquals(11L, counts.getOrDefault(1, 0L),
        "First NTILE bucket must receive the first remainder row when no ordering is supplied");
    Assertions.assertEquals(11L, counts.getOrDefault(2, 0L),
        "Second NTILE bucket must receive the second remainder row when no ordering is supplied");
    Assertions.assertEquals(10L, counts.getOrDefault(3, 0L),
        "Final NTILE bucket must receive the base number of rows when no ordering is supplied");
  }

  /**
   * Ensure that NTILE distributes remainder rows to the earliest buckets when the
   * partition size is not evenly divisible by the bucket count.
   */
  @Test
  void testNtileBalancesUnevenPartitions() {
    Map<Integer, Long> partitionSizes = new LinkedHashMap<>();
    jparqSql.query("SELECT cyl, COUNT(*) AS cnt FROM mtcars GROUP BY cyl ORDER BY cyl", rs -> {
      try {
        while (rs.next()) {
          partitionSizes.put(rs.getInt("cyl"), rs.getLong("cnt"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(partitionSizes.isEmpty(), "Expected partition counts for the cylinder column");

    String sql = """
        SELECT cyl,
               NTILE(2) OVER (PARTITION BY cyl ORDER BY hp) AS hp_bucket
        FROM mtcars
        ORDER BY cyl, hp_bucket, hp
        """;

    Map<Integer, Map<Integer, Long>> countsByPartition = new LinkedHashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          int bucket = rs.getInt("hp_bucket");
          Assertions.assertTrue(bucket >= 1 && bucket <= 2, "Bucket assignments must fall within [1, 2]");
          countsByPartition.computeIfAbsent(cyl, ignored -> new LinkedHashMap<>()).merge(bucket, 1L, Long::sum);
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(partitionSizes.keySet(), countsByPartition.keySet(),
        "Each cylinder partition must receive NTILE results");

    for (Map.Entry<Integer, Long> entry : partitionSizes.entrySet()) {
      int cyl = entry.getKey();
      long total = entry.getValue();
      Map<Integer, Long> bucketCounts = countsByPartition.get(cyl);
      Assertions.assertNotNull(bucketCounts, "Missing bucket counts for cylinder " + cyl);

      long baseSize = total / 2L;
      long remainder = total % 2L;
      long expectedFirst = baseSize + (remainder > 0L ? 1L : 0L);
      long expectedSecond = baseSize;

      Assertions.assertEquals(expectedFirst, bucketCounts.getOrDefault(1, 0L),
          "First NTILE bucket must receive the extra remainder row for cylinder " + cyl);
      Assertions.assertEquals(expectedSecond, bucketCounts.getOrDefault(2, 0L),
          "Second NTILE bucket must contain the base share of rows for cylinder " + cyl);
      Assertions.assertEquals(total, bucketCounts.values().stream().mapToLong(Long::longValue).sum(),
          "NTILE buckets must account for every row in the partition for cylinder " + cyl);
    }
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
   * Verify that PERCENT_RANK resets for each partition and follows the
   * {@code (rank - 1) / (rows - 1)} formula when multiple rows are present.
   */
  @Test
  void testPercentRankRespectsPartitions() {
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

    String sql = """
        SELECT cyl, hp,
               PERCENT_RANK() OVER (PARTITION BY cyl ORDER BY hp DESC) AS cyl_hp_percent_rank
        FROM mtcars
        ORDER BY cyl, hp DESC
        """;

    Map<Long, Long> processedByCyl = new HashMap<>();
    Map<Long, Integer> previousHpByCyl = new HashMap<>();
    Map<Long, Long> currentRankByCyl = new HashMap<>();
    Map<Long, Double> previousPercentByCyl = new HashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          long cyl = toLong(rs.getObject("cyl"));
          int hp = rs.getInt("hp");
          double percentRank = rs.getDouble("cyl_hp_percent_rank");

          long processed = processedByCyl.merge(cyl, 1L, Long::sum);
          long expectedRank;
          Integer previousHp = previousHpByCyl.get(cyl);
          if (previousHp == null) {
            expectedRank = 1L;
          } else if (previousHp.intValue() == hp) {
            expectedRank = currentRankByCyl.get(cyl);
          } else {
            expectedRank = processed;
          }

          long totalRows = countsByCyl.getOrDefault(cyl, 0L);
          double expectedPercentRank = totalRows <= 1L ? 0.0 : (double) (expectedRank - 1L) / (double) (totalRows - 1L);

          Assertions.assertEquals(expectedPercentRank, percentRank, 0.0001,
              "Percent rank must follow the SQL formula within each partition");

          if (processed == 1L) {
            Assertions.assertEquals(0.0, percentRank, 0.0001,
                "First row of each partition must have a percent rank of zero");
          }
          Double previousPercent = previousPercentByCyl.put(cyl, percentRank);
          if (previousPercent != null) {
            Assertions.assertTrue(percentRank + 0.0001 >= previousPercent.doubleValue(),
                "Percent rank values must be non-decreasing within a partition");
          }

          Assertions.assertTrue(percentRank >= 0.0 && percentRank <= 1.0,
              "Percent rank must remain within the inclusive [0, 1] range");

          previousHpByCyl.put(cyl, hp);
          currentRankByCyl.put(cyl, expectedRank);
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(processedByCyl.isEmpty(), "Expected to observe at least one partition");
  }

  /**
   * Verify that CUME_DIST resets for each partition, remains within (0, 1] and
   * assigns identical values to peer rows.
   */
  @Test
  void testCumeDistRespectsPartitionsAndPeers() {
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

    String sql = """
        SELECT cyl, hp,
               CUME_DIST() OVER (PARTITION BY cyl ORDER BY hp DESC) AS cyl_hp_cume_dist
        FROM mtcars
        ORDER BY cyl, hp DESC, model
        """;

    Map<Long, List<CumeDistRow>> rowsByCyl = new LinkedHashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          long cyl = toLong(rs.getObject("cyl"));
          int hp = rs.getInt("hp");
          double cumeDist = rs.getDouble("cyl_hp_cume_dist");
          rowsByCyl.computeIfAbsent(cyl, ignored -> new ArrayList<>()).add(new CumeDistRow(hp, cumeDist));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(countsByCyl.keySet(), rowsByCyl.keySet(),
        "Each cylinder partition must appear in the cumulative distribution result");

    for (Map.Entry<Long, List<CumeDistRow>> entry : rowsByCyl.entrySet()) {
      long cyl = entry.getKey();
      List<CumeDistRow> rows = entry.getValue();
      long totalRows = countsByCyl.getOrDefault(cyl, 0L);
      Assertions.assertEquals(totalRows, rows.size(), "Each partition must emit one CUME_DIST value per input row");

      int index = 0;
      while (index < rows.size()) {
        int groupEnd = index;
        int hp = rows.get(index).horsepower();
        while (groupEnd + 1 < rows.size() && rows.get(groupEnd + 1).horsepower() == hp) {
          groupEnd++;
        }

        double expected = (double) (groupEnd + 1) / (double) totalRows;
        for (int i = index; i <= groupEnd; i++) {
          double actual = rows.get(i).cumeDist();
          Assertions.assertEquals(expected, actual, 0.0001, "Peer rows must share the same cumulative distribution");
          Assertions.assertTrue(actual > 0.0 && actual <= 1.0, "CUME_DIST values must be within the interval (0, 1]");
        }

        if (index == 0) {
          Assertions.assertEquals((double) (groupEnd + 1) / (double) totalRows, rows.get(0).cumeDist(), 0.0001,
              "First peer group must determine the minimum cumulative distribution value");
        }
        index = groupEnd + 1;
      }

      Assertions.assertEquals(1.0, rows.get(rows.size() - 1).cumeDist(), 0.0001,
          "Final row in a partition must have a cumulative distribution of one");
    }
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

  /**
   * Immutable view of a horsepower value and its associated CUME_DIST result.
   */
  private record CumeDistRow(int horsepower, double cumeDist) {
  }
}
