package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Integration tests covering Common Table Expression (CTE) behaviour. */
class CTETest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = CTETest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  @DisplayName("Non-recursive CTE behaves as an inline view")
  void nonRecursiveCteFiltersApply() {
    List<String> expected = new ArrayList<>();
    jparqSql.query("SELECT model FROM mtcars WHERE cyl >= 6 AND cyl > 6 ORDER BY model", rs -> {
      try {
        while (rs.next()) {
          expected.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    List<String> actual = new ArrayList<>();
    String sql = """
        WITH filtered AS (
          SELECT model, cyl FROM mtcars WHERE cyl >= 6
        )
        SELECT model FROM filtered WHERE cyl > 6 ORDER BY model
        """;
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          actual.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertIterableEquals(expected, actual, "CTE should honour outer WHERE predicates");
  }

  @Test
  @DisplayName("CTE column aliases are applied to the outer query")
  void cteColumnAliasesOverrideInnerNames() {
    String model = "Camaro Z28";
    List<Integer> expectedValues = new ArrayList<>();
    jparqSql.query("SELECT cyl FROM mtcars WHERE model = '" + model + "'", rs -> {
      try {
        while (rs.next()) {
          expectedValues.add(rs.getInt(1));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    assertEquals(1, expectedValues.size(), "Expected a single cylinder value for the model");
    int expected = expectedValues.get(0);

    String escapedModel = model.replace("'", "''");
    String sql = """
        WITH summary(model_name, cylinders) AS (
          SELECT model, cyl FROM mtcars
        )
        SELECT cylinders FROM summary WHERE model_name = '%s'
        """.formatted(escapedModel);

    List<Integer> actualValues = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          actualValues.add(rs.getInt(1));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    assertEquals(1, actualValues.size(), "Expected a single row from the CTE");
    int actual = actualValues.get(0);

    assertEquals(expected, actual, "CTE column aliases should be used when projecting results");
  }

  @Test
  @DisplayName("CTEs can reference earlier definitions")
  void chainedCtesAreSupported() {
    List<Long> expectedValues = new ArrayList<>();
    jparqSql.query("SELECT SUM(cyl * 2) FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          expectedValues.add(rs.getLong(1));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    assertEquals(1, expectedValues.size(), "Expected a single aggregate row");
    final long expected = expectedValues.get(0);

    String sql = """
        WITH base AS (
          SELECT cyl FROM mtcars
        ),
        doubled AS (
          SELECT cyl * 2 AS doubled_cyl FROM base
        )
        SELECT SUM(doubled_cyl) FROM doubled
        """;

    List<Long> actualValues = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          actualValues.add(rs.getLong(1));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    assertEquals(1, actualValues.size(), "Expected a single aggregate row from the chained CTE");
    long actual = actualValues.get(0);

    assertEquals(expected, actual, "Chained CTEs should behave like nested inline views");
  }

  @Test
  @DisplayName("Recursive CTE generates a numeric series")
  void recursiveCteProducesSequence() {
    String sql = """
        WITH RECURSIVE cyl_range(c) AS (
          SELECT MIN(cyl) FROM mtcars
          UNION ALL
          SELECT c + 1 FROM cyl_range WHERE c < (SELECT MAX(cyl) FROM mtcars)
        )
        SELECT c FROM cyl_range ORDER BY c
        """;

    List<Integer> values = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          values.add(rs.getInt(1));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertFalse(values.isEmpty(), "Recursive CTE should emit at least one value");
    int min = values.get(0);
    int max = values.get(values.size() - 1);
    assertEquals(values.size(), max - min + 1, "Sequence should be contiguous");

    // Ensure the sequence covers the min/max cylinder values from the source table.
    List<Integer> extrema = new ArrayList<>();
    jparqSql.query("SELECT MIN(cyl), MAX(cyl) FROM mtcars", rs -> {
      try {
        assertTrue(rs.next(), "Expected extrema row");
        extrema.add(rs.getInt(1));
        extrema.add(rs.getInt(2));
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals(extrema.get(0).intValue(), min, "Recursive CTE should start at the minimum cylinder count");
    assertEquals(extrema.get(extrema.size() - 1).intValue(), max,
        "Recursive CTE should end at the maximum cylinder count");

    // Check a few representative values appear in the generated sequence.
    assertTrue(values.contains(4), "Sequence should contain four-cylinder engines");
    assertTrue(values.contains(8), "Sequence should contain eight-cylinder engines");
    assertTrue(values.contains(6), "Sequence should contain six-cylinder engines");
    assertTrue(values.stream().mapToInt(Integer::intValue).distinct().count() == values.size(),
        "Recursive CTE should not produce duplicate rows");
  }
}
