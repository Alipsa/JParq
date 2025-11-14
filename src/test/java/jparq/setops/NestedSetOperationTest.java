package jparq.setops;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

/**
 * Tests covering nested SQL set operation semantics, including parentheses driven
 * evaluation order and mixed ALL/DISTINCT modifiers.
 */
public class NestedSetOperationTest {

  private static JParqSql jparqSql;

  /**
   * Configure the shared {@link JParqSql} instance backed by the test Parquet dataset.
   *
   * @throws URISyntaxException
   *           if the dataset path cannot be resolved
   */
  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = NestedSetOperationTest.class.getResource("/mtcars.parquet");
    if (mtcarsUrl == null) {
      throw new IllegalStateException("mtcars.parquet must be available on the test classpath");
    }
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that parentheses can override the default INTERSECT precedence by forcing a UNION
   * operation to execute before applying the INTERSECT.
   */
  @Test
  void parenthesesOverridePrecedence() {
    String sql = """
        (
          SELECT cyl FROM mtcars WHERE cyl = 4
          UNION
          SELECT cyl FROM mtcars WHERE cyl IN (6, 8)
        )
        INTERSECT
        SELECT cyl FROM mtcars WHERE cyl = 8
        ORDER BY cyl
        """;
    jparqSql.query(sql, rs -> {
      try {
        List<Integer> values = new ArrayList<>();
        while (rs.next()) {
          values.add(rs.getInt(1));
        }
        assertEquals(List.of(8), values,
            "Parentheses should force the UNION to complete before INTERSECT is applied");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Ensure nested parentheses across multiple set operations are evaluated from the innermost
   * expression outward.
   */
  @Test
  void multiLevelNestingIsHonored() {
    String sql = """
        (
          SELECT cyl FROM mtcars WHERE gear = 5
          EXCEPT
          (
            SELECT cyl FROM mtcars WHERE gear = 3
            INTERSECT
            SELECT cyl FROM mtcars WHERE cyl = 8
          )
        )
        UNION
        (
          SELECT cyl FROM mtcars WHERE gear = 4
          INTERSECT
          SELECT cyl FROM mtcars WHERE am = 1
        )
        ORDER BY cyl
        """;
    jparqSql.query(sql, rs -> {
      try {
        List<Integer> values = new ArrayList<>();
        while (rs.next()) {
          values.add(rs.getInt(1));
        }
        assertEquals(List.of(4, 6), values,
            "Nested set operations should evaluate inner parentheses before combining results");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Validate that nested set operations can mix ALL and DISTINCT semantics by combining UNION ALL
   * and EXCEPT while respecting parenthesized evaluation order.
   */
  @Test
  void mixedAllAndDistinctOperationsAreSupported() {
    String sql = """
        (
          SELECT cyl FROM mtcars WHERE gear = 4
          UNION ALL
          SELECT cyl FROM mtcars WHERE gear = 5
        )
        EXCEPT
        (
          SELECT cyl FROM mtcars WHERE gear = 5
          INTERSECT
          SELECT cyl FROM mtcars WHERE cyl = 4
        )
        ORDER BY cyl
        """;
    jparqSql.query(sql, rs -> {
      try {
        List<Integer> values = new ArrayList<>();
        while (rs.next()) {
          values.add(rs.getInt(1));
        }
        assertEquals(List.of(6, 8), values,
            "Mixing UNION ALL with EXCEPT should yield distinct rows after the DISTINCT operation");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }
}

