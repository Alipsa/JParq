package jparq;

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
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests covering GROUP BY, COUNT(*), HAVING, and CASE support. */
public class GroupByTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = GroupByTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testGroupByCountStar() {
    Map<Integer, Integer> expectedCounts = new HashMap<>();
    jparqSql.query("SELECT cyl FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt(1);
          expectedCounts.merge(cyl, 1, Integer::sum);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    List<Integer> seenCyl = new ArrayList<>();
    jparqSql.query("SELECT cyl, COUNT(*) AS total FROM mtcars GROUP BY cyl ORDER BY cyl", rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt("cyl");
          long total = rs.getLong("total");
          assertEquals(expectedCounts.get(cyl).longValue(), total, "Unexpected COUNT(*) for cyl = " + cyl);
          seenCyl.add(cyl);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    List<Integer> expectedOrder = expectedCounts.keySet().stream().sorted().collect(Collectors.toList());
    assertEquals(expectedOrder, seenCyl, "GROUP BY cyl should return one row per cylinder value");
  }

  @Test
  void testHavingFiltersGroups() {
    Map<Integer, Integer> counts = new HashMap<>();
    jparqSql.query("SELECT cyl FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt(1);
          counts.merge(cyl, 1, Integer::sum);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    List<Integer> expected = counts.entrySet().stream().filter(entry -> entry.getValue() > 7 && entry.getKey() >= 6)
        .map(Map.Entry::getKey).sorted().collect(Collectors.toList());

    List<Integer> actual = new ArrayList<>();
    jparqSql.query(
        "SELECT cyl, COUNT(*) AS total FROM mtcars GROUP BY cyl HAVING COUNT(*) > 7 AND cyl >= 6 ORDER BY cyl", rs -> {
          try {
            while (rs.next()) {
              actual.add(rs.getInt("cyl"));
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });

    assertEquals(expected, actual, "HAVING clause should filter cylinder groups as expected");
  }

  @Test
  void testHavingAggregateNotInSelect() {
    Map<Integer, Integer> counts = new HashMap<>();
    jparqSql.query("SELECT cyl FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt(1);
          counts.merge(cyl, 1, Integer::sum);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    List<Integer> expected = counts.entrySet().stream().filter(entry -> entry.getValue() > 1).map(Map.Entry::getKey)
        .sorted().collect(Collectors.toList());

    List<Integer> actual = new ArrayList<>();
    jparqSql.query("SELECT cyl FROM mtcars GROUP BY cyl HAVING COUNT(*) > 1 ORDER BY cyl", rs -> {
      try {
        while (rs.next()) {
          actual.add(rs.getInt("cyl"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals(expected, actual, "HAVING clause should allow aggregates not present in the SELECT list");
  }

  @Test
  void testGroupByCaseExpression() {
    Map<String, Double> expected = new HashMap<>();
    jparqSql.query("SELECT cyl, hp FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          int cyl = rs.getInt(1);
          double hp = rs.getDouble(2);
          String label = cyl == 4 ? "four" : "other";
          expected.merge(label, hp, Double::sum);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    Map<String, Double> actual = new HashMap<>();
    jparqSql.query("SELECT CASE WHEN cyl = 4 THEN 'four' ELSE 'other' END AS category, SUM(hp) AS total_hp FROM mtcars "
        + "GROUP BY CASE WHEN cyl = 4 THEN 'four' ELSE 'other' END ORDER BY category", rs -> {
          try {
            while (rs.next()) {
              String category = rs.getString("category");
              double totalHp = rs.getDouble("total_hp");
              actual.put(category, totalHp);
            }
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals(expected.keySet(), actual.keySet(), "CASE expression should produce expected groups");
    expected
        .forEach((label, value) -> assertTrue(actual.containsKey(label) && Math.abs(actual.get(label) - value) < 1e-9,
            "Unexpected SUM(hp) for group " + label));
  }
}
