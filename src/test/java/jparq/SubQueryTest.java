package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for alias operations. */
public class SubQueryTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = WhereTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void inClauseSubqueryFiltersRows() {
    jparqSql.query("SELECT model FROM mtcars where mpg in (select distinct mpg from mtcars)", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount(), "Expected 1 column");

        int rows = 0;

        while (rs.next()) {
          String model = rs.getString("model");
          seen.add(model);
          rows++;
        }

        assertEquals(32, rows, "Expected 32 rows");
      } catch (SQLException e) {
        System.err.println(String.join("\n", seen));
        fail(e);
      }
    });
  }

  @Test
  void scalarSubqueryInSelectClause() {
    jparqSql.query("SELECT model, (SELECT COUNT(*) FROM mtcars) AS total_cars FROM mtcars", rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;
        Long observedTotal = null;
        while (rs.next()) {
          long total = rs.getLong("total_cars");
          if (observedTotal == null) {
            observedTotal = total;
          } else {
            assertEquals(observedTotal, total, "Scalar subquery should yield a constant value");
          }
          rows++;
        }
        assertEquals(32, rows, "Expected 32 rows");
        assertEquals(32L, observedTotal.longValue(), "Expected scalar subquery to return 32");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void subqueryInFromClauseActsAsDerivedTable() {
    jparqSql.query("SELECT * FROM (SELECT model, cyl FROM mtcars WHERE cyl = 4) AS fours", rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Derived table should expose projected columns");

        int rows = 0;
        while (rs.next()) {
          assertEquals(4, rs.getInt("cyl"));
          rows++;
        }
        assertEquals(11, rows, "Expected 11 rows for 4-cylinder cars");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void existsSubqueryEvaluatesPresence() {
    jparqSql.query("SELECT model FROM mtcars WHERE NOT EXISTS (SELECT 1 FROM mtcars WHERE cyl = 4)", rs -> {
      try {
        assertTrue(!rs.next(), "NOT EXISTS should eliminate all rows when subquery returns data");
      } catch (SQLException e) {
        fail(e);
      }
    });

    jparqSql.query("SELECT model FROM mtcars WHERE EXISTS (SELECT 1 FROM mtcars WHERE cyl = 4)", rs -> {
      try {
        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(32, rows, "EXISTS should return all rows when subquery has data");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void havingClauseSupportsSubqueries() {
    jparqSql.query(
        "SELECT COUNT(*) AS total FROM mtcars HAVING COUNT(*) >= (SELECT COUNT(*) FROM mtcars)", rs -> {
          try {
            assertTrue(rs.next(), "HAVING condition should pass when comparison evaluates to true");
            assertEquals(32L, rs.getLong("total"));
            assertFalse(rs.next(), "Only a single aggregate row should be returned");
          } catch (SQLException e) {
            fail(e);
          }
        });

    jparqSql.query(
        "SELECT COUNT(*) AS total FROM mtcars HAVING COUNT(*) > (SELECT COUNT(*) FROM mtcars)", rs -> {
          try {
            assertFalse(rs.next(), "HAVING condition should filter out the aggregate row when false");
          } catch (SQLException e) {
            fail(e);
          }
        });
  }

}
