package jparq.setops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Verifies ORDER BY expressions over set operation results. */
public class SetOperationOrderByExpressionTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = SetOperationOrderByExpressionTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testOrderByExpressionAcrossUnion() {
    List<Double> expected = new ArrayList<>();
    jparqSql.query("SELECT DISTINCT hp FROM mtcars WHERE cyl IN (4,6) ORDER BY hp DESC", rs -> {
      try {
        while (rs.next()) {
          expected.add(rs.getDouble(1) + 10);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    List<Double> actual = new ArrayList<>();
    jparqSql.query("""
        SELECT hp AS power FROM mtcars WHERE cyl = 4
        UNION
        SELECT hp AS power FROM mtcars WHERE cyl = 6
        ORDER BY power + 10 DESC
        """, rs -> {
      try {
        while (rs.next()) {
          actual.add(rs.getDouble(1) + 10);
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals(expected, actual,
        "Set operation ORDER BY expressions should sort using derived expressions from the first query");
  }

  @Test
  void testInvalidOrderByExpression() {
    String sql = """
        SELECT hp AS power FROM mtcars WHERE cyl = 4
        UNION
        SELECT hp AS power FROM mtcars WHERE cyl = 6
        ORDER BY nonexistent_column + 10 DESC
        """;
    RuntimeException ex = assertThrows(RuntimeException.class, () -> jparqSql.query(sql, rs -> {
    }));
    assertTrue(ex.getCause() instanceof SQLException, "Expected SQLException cause for invalid ORDER BY expression");
  }
}
