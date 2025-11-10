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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Tests verifying GROUP BY CUBE semantics together with GROUPING() behaviour.
 */
public class CubeTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = CubeTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testCubeGeneratesAllSubtotals() {
    MtcarsHpSums.Aggregates sums = MtcarsHpSums.compute(jparqSql);

    Map<Integer, Double> gearTotals = new HashMap<>();
    sums.detailSums().values()
        .forEach(gearMap -> gearMap.forEach((gear, total) -> gearTotals.merge(gear, total, Double::sum)));

    List<ResultRow> actual = new ArrayList<>();
    jparqSql.query("SELECT cyl, gear, SUM(hp) AS total_hp, GROUPING(cyl) AS g_cyl, GROUPING(gear) AS g_gear "
        + "FROM mtcars GROUP BY CUBE (cyl, gear) ORDER BY cyl, gear", rs -> {
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

    TreeSet<Integer> gearOrder = new TreeSet<>(gearTotals.keySet());
    for (Integer gear : gearOrder) {
      expected.add(new ResultRow(null, gear, gearTotals.get(gear), 1, 0));
    }
    expected.add(new ResultRow(null, null, sums.grandTotal(), 1, 1));

    assertEquals(expected.size(), actual.size(), "Unexpected number of rows from CUBE query");
    for (int i = 0; i < expected.size(); i++) {
      ResultRow exp = expected.get(i);
      ResultRow act = actual.get(i);
      assertEquals(exp.cyl(), act.cyl(), "Cylinder mismatch at row " + i);
      assertEquals(exp.gear(), act.gear(), "Gear mismatch at row " + i);
      assertEquals(exp.totalHp(), act.totalHp(), 1e-9, "Total HP mismatch at row " + i);
      assertEquals(exp.groupingCyl(), act.groupingCyl(), "GROUPING(cyl) mismatch at row " + i);
      assertEquals(exp.groupingGear(), act.groupingGear(), "GROUPING(gear) mismatch at row " + i);
    }

    Set<String> groupingLevels = new HashSet<>();
    actual.forEach(row -> groupingLevels.add(row.groupingCyl() + ":" + row.groupingGear()));
    assertEquals(Set.of("0:0", "0:1", "1:0", "1:1"), groupingLevels,
        "CUBE should produce all 2^N grouping level combinations");
  }

  @Test
  void testCubeIgnoresDuplicateDimensions() {
    MtcarsHpSums.Aggregates sums = MtcarsHpSums.compute(jparqSql);

    List<ResultRow> actual = new ArrayList<>();
    jparqSql.query("SELECT cyl, gear, SUM(hp) AS total_hp, GROUPING(cyl) AS g_cyl, GROUPING(gear) AS g_gear "
        + "FROM mtcars GROUP BY cyl, CUBE(cyl, gear) ORDER BY cyl, gear", rs -> {
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

    int detailRowCount = sums.detailSums().values().stream().mapToInt(Map::size).sum();
    int cylinderTotalCount = sums.cylinderTotals().size();
    assertEquals(detailRowCount + cylinderTotalCount, actual.size(),
        "Duplicate cube dimensions should not introduce extra rows");

    Set<String> seenDetailKeys = new HashSet<>();
    Set<Integer> seenCylinderTotals = new HashSet<>();

    actual.forEach(row -> {
      assertEquals(0, row.groupingCyl(), "GROUPING(cyl) should remain 0 when specified as a base column");
      if (row.gear() == null) {
        assertEquals(1, row.groupingGear(), "Cylinder totals should have GROUPING(gear) = 1");
        assertTrue(seenCylinderTotals.add(row.cyl()), "Duplicate cylinder total encountered for cyl = " + row.cyl());
        Double expected = sums.cylinderTotals().get(row.cyl());
        assertNotNull(expected, "Unexpected cylinder value in CUBE totals: " + row.cyl());
        assertEquals(expected, row.totalHp(), 1e-9, "Cylinder total mismatch for cyl = " + row.cyl());
      } else {
        assertEquals(0, row.groupingGear(), "Detail rows should have GROUPING(gear) = 0");
        Map<Integer, Double> gearTotals = sums.detailSums().get(row.cyl());
        assertNotNull(gearTotals, "Unexpected cylinder value in detail rows: " + row.cyl());
        Double expected = gearTotals.get(row.gear());
        assertNotNull(expected, "Unexpected gear value in detail rows: cyl = " + row.cyl() + ", gear = " + row.gear());
        assertTrue(seenDetailKeys.add(row.cyl() + ":" + row.gear()),
            "Duplicate detail row for cyl = " + row.cyl() + ", gear = " + row.gear());
        assertEquals(expected, row.totalHp(), 1e-9,
            "Detail total mismatch for cyl = " + row.cyl() + ", gear = " + row.gear());
      }
    });

    assertEquals(detailRowCount, seenDetailKeys.size(), "Expected one detail row per cyl/gear combination");
    assertEquals(cylinderTotalCount, seenCylinderTotals.size(), "Expected one total per cylinder");
  }

  @Test
  void testCubeGroupingFilters() {
    MtcarsHpSums.Aggregates sums = MtcarsHpSums.compute(jparqSql);

    Map<Integer, Double> gearTotals = new HashMap<>();
    sums.detailSums().values()
        .forEach(gearMap -> gearMap.forEach((gear, total) -> gearTotals.merge(gear, total, Double::sum)));

    List<Double> grandTotals = new ArrayList<>();
    jparqSql.query("SELECT SUM(hp) AS total_hp FROM mtcars GROUP BY CUBE (cyl, gear) "
        + "HAVING GROUPING(cyl) = 1 AND GROUPING(gear) = 1", rs -> {
          try {
            while (rs.next()) {
              grandTotals.add(rs.getDouble("total_hp"));
            }
          } catch (SQLException e) {
            fail(e);
          }
        });
    assertEquals(1, grandTotals.size(), "Expected a single grand total row from CUBE HAVING clause");
    assertEquals(sums.grandTotal(), grandTotals.getFirst(), 1e-9, "Grand total mismatch");

    Map<Integer, Double> cylinderTotals = new HashMap<>();
    jparqSql.query("SELECT cyl, SUM(hp) AS total_hp FROM mtcars GROUP BY CUBE (cyl, gear) "
        + "HAVING GROUPING(cyl) = 0 AND GROUPING(gear) = 1", rs -> {
          try {
            while (rs.next()) {
              Integer cyl = (Integer) rs.getObject("cyl");
              double totalHp = rs.getDouble("total_hp");
              cylinderTotals.put(cyl, totalHp);
            }
          } catch (SQLException e) {
            fail(e);
          }
        });
    assertEquals(sums.cylinderTotals().size(), cylinderTotals.size(),
        "CUBE HAVING filter should emit one total per cylinder");
    sums.cylinderTotals().forEach((cyl, total) -> {
      assertTrue(cylinderTotals.containsKey(cyl), "Missing cylinder total for cyl = " + cyl);
      assertEquals(total, cylinderTotals.get(cyl), 1e-9, "Cylinder total mismatch for cyl = " + cyl);
    });

    Map<Integer, Double> actualGearTotals = new HashMap<>();
    jparqSql.query("SELECT gear, SUM(hp) AS total_hp FROM mtcars GROUP BY CUBE (cyl, gear) "
        + "HAVING GROUPING(cyl) = 1 AND GROUPING(gear) = 0", rs -> {
          try {
            while (rs.next()) {
              Integer gear = (Integer) rs.getObject("gear");
              double totalHp = rs.getDouble("total_hp");
              actualGearTotals.put(gear, totalHp);
            }
          } catch (SQLException e) {
            fail(e);
          }
        });
    assertEquals(gearTotals.size(), actualGearTotals.size(), "CUBE HAVING filter should emit one total per gear");
    gearTotals.forEach((gear, total) -> {
      assertTrue(actualGearTotals.containsKey(gear), "Missing gear total for gear = " + gear);
      assertEquals(total, actualGearTotals.get(gear), 1e-9, "Gear total mismatch for gear = " + gear);
    });
  }

  private record ResultRow(Integer cyl, Integer gear, double totalHp, int groupingCyl, int groupingGear) {
  }
}
