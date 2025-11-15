package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for the SQL standard FETCH clause. */
public class FetchTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = FetchTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void fetchFirstRowsOnlyLimitsResultSet() {
    jparqSql.query("SELECT model FROM mtcars FETCH FIRST 5 ROWS ONLY", rs -> {
      int rows = 0;
      try {
        while (rs.next()) {
          rows++;
        }
        assertEquals(5, rows, "FETCH FIRST should limit the number of rows returned");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void fetchFirstRowOnlyDefaultsToOne() {
    jparqSql.query("SELECT model FROM mtcars FETCH FIRST ROW ONLY", rs -> {
      int rows = 0;
      try {
        while (rs.next()) {
          rows++;
        }
        assertEquals(1, rows, "FETCH FIRST ROW ONLY should default to a single row");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void fetchFirstRowsOnlyMatchesLimitOrdering() {
    List<String> expected = new ArrayList<>();
    jparqSql.query("SELECT model, mpg FROM mtcars ORDER BY mpg DESC LIMIT 2", rs -> {
      try {
        while (rs.next()) {
          expected.add(rs.getString("model") + "|" + rs.getDouble("mpg"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    List<String> actual = new ArrayList<>();
    jparqSql.query("""
        SELECT model, mpg
        FROM mtcars
        ORDER BY mpg DESC
        FETCH FIRST 2 ROWS ONLY
        """, rs -> {
      try {
        while (rs.next()) {
          actual.add(rs.getString("model") + "|" + rs.getDouble("mpg"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals(expected, actual, "FETCH FIRST should return the same leading rows as LIMIT");
  }

  @Test
  void fetchNextRowsOnlyRespectsOffset() {
    List<String> expected = new ArrayList<>();
    jparqSql.query("""
        SELECT model, mpg
        FROM mtcars
        ORDER BY mpg DESC
        OFFSET 2 ROWS
        LIMIT 3
        """, rs -> {
      try {
        while (rs.next()) {
          expected.add(rs.getString("model") + "|" + rs.getDouble("mpg"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    List<String> actual = new ArrayList<>();
    jparqSql.query("""
        SELECT model, mpg
        FROM mtcars
        ORDER BY mpg DESC
        OFFSET 2 ROWS
        FETCH NEXT 3 ROWS ONLY
        """, rs -> {
      try {
        while (rs.next()) {
          actual.add(rs.getString("model") + "|" + rs.getDouble("mpg"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals(expected, actual, "FETCH NEXT should honor OFFSET semantics");
  }
}

