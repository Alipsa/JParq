package jparq.group;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests verifying GROUPING SETS support and GROUPING() semantics. */
public class GroupingSetsTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = GroupingSetsTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testGroupingSetsAggregates() {
    MtcarsHpSums.Aggregates sums = MtcarsHpSums.compute(jparqSql);

    List<ResultRow> actual = new ArrayList<>();
    jparqSql.query("SELECT cyl, gear, SUM(hp) AS total_hp, GROUPING(cyl) AS g_cyl, GROUPING(gear) AS g_gear "
        + "FROM mtcars GROUP BY GROUPING SETS ((cyl, gear), (cyl), ()) ORDER BY cyl, gear", rs -> {
          try {
            while (rs.next()) {
              Integer cyl = (Integer) rs.getObject("cyl");
              Integer gear = (Integer) rs.getObject("gear");
              double totalHp = rs.getDouble("total_hp");
              int groupingCyl = rs.getInt("g_cyl");
              int groupingGear = rs.getInt("g_gear");
              actual.add(new ResultRow(cyl, gear, totalHp, groupingCyl, groupingGear));
            }
          } catch (SQLException e) {
            fail(e);
          }
        });

    List<ResultRow> expected = new ArrayList<>();
    TreeSet<Integer> cylOrder = new TreeSet<>(sums.detailSums().keySet());
    for (Integer cyl : cylOrder) {
      Map<Integer, Double> gearMap = sums.detailSums().get(cyl);
      TreeSet<Integer> gearOrder = new TreeSet<>(gearMap.keySet());
      for (Integer gear : gearOrder) {
        expected.add(new ResultRow(cyl, gear, gearMap.get(gear), 0, 0));
      }
      expected.add(new ResultRow(cyl, null, sums.cylinderTotals().get(cyl), 0, 1));
    }
    expected.add(new ResultRow(null, null, sums.grandTotal(), 1, 1));

    assertEquals(expected.size(), actual.size(), "Unexpected number of rows from GROUPING SETS query");
    for (int i = 0; i < expected.size(); i++) {
      ResultRow exp = expected.get(i);
      ResultRow act = actual.get(i);
      assertEquals(exp.cyl(), act.cyl(), "Cylinder value mismatch at row " + i);
      assertEquals(exp.gear(), act.gear(), "Gear value mismatch at row " + i);
      assertEquals(exp.totalHp(), act.totalHp(), 1e-9, "Total HP mismatch at row " + i);
      assertEquals(exp.groupingCyl(), act.groupingCyl(), "Grouping(cyl) mismatch at row " + i);
      assertEquals(exp.groupingGear(), act.groupingGear(), "Grouping(gear) mismatch at row " + i);
    }
  }

  @Test
  void testGroupingFunctionInHaving() {
    MtcarsHpSums.Aggregates sums = MtcarsHpSums.compute(jparqSql);

    List<Double> grandTotals = new ArrayList<>();
    jparqSql.query("SELECT SUM(hp) AS total_hp FROM mtcars GROUP BY GROUPING SETS ((cyl), ()) HAVING GROUPING(cyl) = 1",
        rs -> {
          try {
            while (rs.next()) {
              grandTotals.add(rs.getDouble("total_hp"));
            }
          } catch (SQLException e) {
            fail(e);
          }
        });
    assertEquals(1, grandTotals.size(), "HAVING GROUPING(cyl) = 1 should return only the grand total");
    assertEquals(sums.grandTotal(), grandTotals.getFirst(), 1e-9, "Grand total mismatch");

    Map<Integer, Double> actualCylTotals = new HashMap<>();
    jparqSql.query(
        "SELECT cyl, SUM(hp) AS total_hp FROM mtcars GROUP BY GROUPING SETS ((cyl), ()) HAVING GROUPING(cyl) = 0",
        rs -> {
          try {
            while (rs.next()) {
              Integer cyl = (Integer) rs.getObject("cyl");
              double totalHp = rs.getDouble("total_hp");
              actualCylTotals.put(cyl, totalHp);
            }
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals(sums.cylinderTotals().size(), actualCylTotals.size(),
        "HAVING GROUPING(cyl) = 0 should emit one row per cylinder");
    sums.cylinderTotals().forEach((cyl, total) -> {
      assertTrue(actualCylTotals.containsKey(cyl), "Missing cylinder total for cyl = " + cyl);
      assertEquals(total, actualCylTotals.get(cyl), 1e-9, "Cylinder total mismatch for cyl = " + cyl);
    });
  }

  @Test
  void testOrderByGroupingFunction() {
    MtcarsHpSums.Aggregates sums = MtcarsHpSums.compute(jparqSql);
    List<Double> actualTotals = new ArrayList<>();
    jparqSql.query("SELECT SUM(hp) AS total_hp FROM mtcars GROUP BY GROUPING SETS ((cyl), ()) "
        + "ORDER BY GROUPING(cyl) DESC, cyl ASC", rs -> {
          try {
            while (rs.next()) {
              actualTotals.add(rs.getDouble("total_hp"));
            }
          } catch (SQLException e) {
            fail(e);
          }
        });

    List<Double> expectedTotals = new ArrayList<>();
    expectedTotals.add(sums.grandTotal());
    new TreeSet<>(sums.cylinderTotals().keySet()).forEach(cyl -> expectedTotals.add(sums.cylinderTotals().get(cyl)));

    assertEquals(expectedTotals.size(), actualTotals.size(), "Unexpected number of rows when ordering by GROUPING()");
    for (int i = 0; i < expectedTotals.size(); i++) {
      assertEquals(expectedTotals.get(i), actualTotals.get(i), 1e-9, "Mismatch at ordered row " + i);
    }
  }

  private record ResultRow(Integer cyl, Integer gear, double totalHp, int groupingCyl, int groupingGear) {
  }
}
