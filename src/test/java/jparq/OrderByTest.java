package jparq;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeIntegerFixture;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqSql;

/** Tests for ORDER BY operations. */
public class OrderByTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = WhereTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Checks quoted lowercase output aliases, exact-name precedence, and ambiguity.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @param base
   *          temporary directory for the Parquet fixture
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void quotedOrderingAliasesRespectExactNamesAndAmbiguity(boolean prepared, @TempDir Path base) throws Exception {
    writeIntegerFixture(base);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + base)) {
      for (String source : List.of("o", "o JOIN (VALUES (1)) v(x) ON TRUE")) {
        assertEquals(List.of(List.of(1), List.of(1), List.of(2), List.of(3), Arrays.asList((Object) null)),
            rows(connection, "SELECT n AS renamed FROM " + source + " ORDER BY \"renamed\" NULLS LAST", prepared));
      }
      assertEquals(
          List.of(List.of(3, -3L), List.of(2, -2L), List.of(1, -1L), List.of(1, -1L), Arrays.asList(null, null)),
          rows(connection, "SELECT n AS c, -n AS \"c\" FROM o ORDER BY \"c\" NULLS LAST", prepared));
      for (String predicate : List.of("", " WHERE 1 = 0")) {
        String sql = "SELECT n AS repeated, n + 1 AS repeated FROM o" + predicate + " ORDER BY \"repeated\"";
        SQLException error = assertThrows(SQLException.class, () -> rows(connection, sql, prepared), sql);
        assertEquals("42702", error.getSQLState(), sql);
        assertEquals(
            predicate.isEmpty()
                ? List.of(List.of(1, 1), List.of(1, 1), List.of(2, 2), List.of(3, 3), Arrays.asList(null, null))
                : List.of(),
            rows(connection,
                "SELECT n AS repeated, n AS repeated FROM o" + predicate + " ORDER BY \"repeated\" NULLS LAST",
                prepared));
      }
      SQLException error = assertThrows(SQLSyntaxErrorException.class,
          () -> rows(connection, "SELECT n AS renamed FROM o ORDER BY \"Renamed\"", prepared));
      assertEquals("42000", error.getSQLState());
    }
  }

  @Test
  void testOrderByMpgAsc() {
    jparqSql.query("SELECT model, mpg FROM mtcars ORDER BY mpg ASC", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;
        double prev = Double.NEGATIVE_INFINITY;

        while (rs.next()) {
          String model = rs.getString("model");
          double mpg = rs.getDouble("mpg");
          seen.add(model + " " + mpg);

          // non-decreasing
          assertTrue(mpg >= prev, "mpg must be non-decreasing: " + String.join(", ", seen));
          prev = mpg;
          rows++;
        }

        assertEquals(32, rows, "Expected 32 rows");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testOrderByMpgDesc() {
    jparqSql.query("SELECT model, mpg FROM mtcars ORDER BY mpg DESC", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;
        double prev = Double.POSITIVE_INFINITY;

        while (rs.next()) {
          String model = rs.getString("model");
          double mpg = rs.getDouble("mpg");
          seen.add(model + " " + mpg);

          // non-increasing
          assertTrue(mpg <= prev, "mpg must be non-increasing: " + String.join(", ", seen));
          prev = mpg;
          rows++;
        }

        assertEquals(32, rows, "Expected 32 rows");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testOrderByMultiKey() {
    // Sort by cyl ASC, then mpg DESC within each cyl group
    jparqSql.query("SELECT model, cyl, mpg FROM mtcars ORDER BY cyl ASC, mpg DESC", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount(), "Expected 3 columns");

        int rows = 0;
        Integer prevCyl = null;
        Double prevMpgInGroup = null;

        while (rs.next()) {
          String model = rs.getString("model");
          int cyl = rs.getInt("cyl");
          double mpg = rs.getDouble("mpg");
          seen.add(model + " cyl=" + cyl + " mpg=" + mpg);

          if (prevCyl == null) {
            prevCyl = cyl;
            prevMpgInGroup = mpg;
          } else {
            // primary key non-decreasing
            assertTrue(cyl >= prevCyl, "cyl must be non-decreasing: " + String.join(", ", seen));
            if (cyl == prevCyl) {
              // secondary key: mpg non-increasing within same cyl
              assertTrue(mpg <= prevMpgInGroup + 1e-9,
                  "mpg must be non-increasing within cyl=" + cyl + ": " + String.join(", ", seen));
            } else {
              // new group: reset secondary comparator
              prevCyl = cyl;
              prevMpgInGroup = mpg;
              rows++; // count the boundary row too
              continue;
            }
          }
          prevMpgInGroup = mpg;
          rows++;
        }

        assertEquals(32, rows, "Expected 32 rows");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testOrderByAlias() {
    jparqSql.query("""
        SELECT gear,
        AVG(mpg) AS avgMpg
        FROM mtcars
        GROUP BY gear
        order by avgMpg desc
        """, rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;
        double prev = Double.POSITIVE_INFINITY;
        List<Integer> gears = new ArrayList<>();
        List<Double> avgMpgs = new ArrayList<>();

        while (rs.next()) {
          int gear = rs.getInt("gear");
          double mpg = rs.getDouble("avgMpg");
          gears.add(gear);
          avgMpgs.add(mpg);
          seen.add(gear + " " + mpg);

          // non-increasing
          assertTrue(mpg <= prev, "avgMpg must be non-increasing: " + String.join(", ", seen));
          prev = mpg;
          rows++;
        }
        assertEquals(3, rows, "Expected 3 rows");
        double[] expectedAvgMpgs = Stream.of(24.533, 21.38, 16.107).mapToDouble(Double::doubleValue).toArray();
        double[] actualAvgMpgs = avgMpgs.stream().mapToDouble(Double::doubleValue).toArray();
        assertArrayEquals(expectedAvgMpgs, actualAvgMpgs, 0.001, "Unexpected avgMpg values");

      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testOrderByOnlyInGroupBy() {
    Map<Integer, Integer> totalsByCyl = new HashMap<>();
    jparqSql.query("SELECT cyl, hp FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          totalsByCyl.merge(rs.getInt("cyl"), rs.getInt("hp"), Integer::sum);
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    List<Integer> expectedTotals = totalsByCyl.entrySet().stream()
        .sorted(Map.Entry.<Integer, Integer>comparingByKey(Comparator.reverseOrder())).map(Map.Entry::getValue)
        .toList();

    jparqSql.query("""
        SELECT SUM(hp) FROM mtcars GROUP BY cyl ORDER BY cyl DESC
        """, rs -> {
      try {
        List<Integer> actualTotals = new ArrayList<>();
        while (rs.next()) {
          actualTotals.add(rs.getInt(1));
        }
        assertEquals(expectedTotals, actualTotals,
            "Aggregates should be ordered by grouping column even when it is not projected");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  void testOrderByExpressionOutsideSelect() {
    List<String> expectedModels = new ArrayList<>();
    jparqSql.query("SELECT model FROM mtcars ORDER BY hp DESC", rs -> {
      try {
        while (rs.next()) {
          expectedModels.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });

    List<String> actualModels = new ArrayList<>();
    jparqSql.query("SELECT model FROM mtcars ORDER BY hp + 10 DESC", rs -> {
      try {
        while (rs.next()) {
          actualModels.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });

    assertEquals(expectedModels, actualModels,
        "Ordering by expression not in SELECT should match ordering by the underlying columns");
  }

  @Test
  void testInvalidOrderByExpression() {
    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> jparqSql.query("SELECT model FROM mtcars ORDER BY nonexistent_column + 10", rs -> {
        }));
    assertTrue(ex.getCause() instanceof SQLException, "Expected SQLException cause for invalid ORDER BY expression");
  }

}
