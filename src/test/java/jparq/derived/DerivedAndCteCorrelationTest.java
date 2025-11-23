package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Verifies correlated subquery resolution for derived tables and CTEs while
 * asserting row counts remain intact when aliases are used for correlation.
 */
class DerivedAndCteCorrelationTest {

  private static JParqSql jparqSql;
  private static Map<Integer, Long> expectedCylinderCounts;

  @BeforeAll
  static void setUp() throws URISyntaxException {
    URL mtcarsUrl = DerivedAndCteCorrelationTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
    expectedCylinderCounts = new HashMap<>();

    jparqSql.query("SELECT cyl, COUNT(*) AS cnt FROM mtcars GROUP BY cyl", rs -> {
      try {
        while (rs.next()) {
          expectedCylinderCounts.put(rs.getInt("cyl"), rs.getLong("cnt"));
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to compute expected cylinder counts", e);
      }
    });

    assertFalse(expectedCylinderCounts.isEmpty(),
        "Expected cylinder counts to be populated for correlation validation");
  }

  @Test
  void derivedTableCorrelatedCountPreservesRowCount() {
    Set<Integer> seenCylinders = new HashSet<>();
    int[] rows = new int[1];

    jparqSql.query("""
        SELECT d.grp,
               (SELECT COUNT(*) FROM mtcars m WHERE m.cyl = d.grp) AS correlated_cnt
        FROM (SELECT cyl AS grp, model FROM mtcars) d
        ORDER BY d.grp, d.model
        """, rs -> {
      try {
        while (rs.next()) {
          rows[0]++;
          int group = rs.getInt("grp");
          long correlated = rs.getLong("correlated_cnt");
          assertEquals(expectedCylinderCounts.get(group), correlated,
              "Derived projection alias should be visible to correlated subqueries");
          seenCylinders.add(group);
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to evaluate derived correlation", e);
      }
    });

    assertEquals(32, rows[0], "Derived table should emit the base row count when correlation is resolved");
    assertFalse(seenCylinders.isEmpty(), "Expected correlated counts for at least one cylinder group");
    assertEquals(expectedCylinderCounts.size(), seenCylinders.size(),
        "Correlation should evaluate for every distinct cylinder");
  }

  @Test
  void cteCorrelatedCountPreservesRowCount() {
    Set<Integer> seenCylinders = new HashSet<>();
    int[] rows = new int[1];

    jparqSql.query("""
        WITH base AS (
            SELECT cyl AS grp, model FROM mtcars
        )
        SELECT base.grp,
               (SELECT COUNT(*) FROM mtcars m WHERE m.cyl = base.grp) AS correlated_cnt
        FROM base
        ORDER BY base.grp, base.model
        """, rs -> {
      try {
        while (rs.next()) {
          rows[0]++;
          int group = rs.getInt("grp");
          long correlated = rs.getLong("correlated_cnt");
          assertEquals(expectedCylinderCounts.get(group), correlated,
              "CTE projection alias should be visible to correlated subqueries");
          seenCylinders.add(group);
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to evaluate CTE correlation", e);
      }
    });

    assertEquals(32, rows[0], "CTE should emit the base row count when correlation is resolved");
    assertFalse(seenCylinders.isEmpty(), "Expected correlated counts for at least one cylinder group");
    assertEquals(expectedCylinderCounts.size(), seenCylinders.size(),
        "Correlation should evaluate for every distinct cylinder");
  }
}
