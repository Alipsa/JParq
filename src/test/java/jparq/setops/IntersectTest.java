package jparq.setops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests covering INTERSECT query semantics. */
public class IntersectTest {

  private static JParqSql jparqSql;

  /**
   * Configure the shared {@link JParqSql} instance backed by the test Parquet
   * dataset.
   *
   * @throws URISyntaxException
   *           if the dataset path cannot be resolved
   */
  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = IntersectTest.class.getResource("/mtcars.parquet");
    if (mtcarsUrl == null) {
      throw new IllegalStateException("mtcars.parquet must be available on the test classpath");
    }
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that INTERSECT returns only the rows that exist in both result sets
   * and removes duplicates.
   */
  @Test
  void intersectReturnsDistinctCommonRows() {
    String sql = """
        SELECT cyl FROM mtcars WHERE cyl IN (4, 6, 8)
        INTERSECT
        SELECT cyl FROM mtcars WHERE cyl IN (6, 8)
        """;
    jparqSql.query(sql, rs -> {
      try {
        int rows = 0;
        Set<Integer> values = new LinkedHashSet<>();
        while (rs.next()) {
          rows++;
          values.add(rs.getInt(1));
        }
        assertEquals(2, rows, "INTERSECT should only emit rows present in both inputs");
        assertEquals(Set.of(6, 8), values, "INTERSECT must deduplicate common rows");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Verify that an ORDER BY clause applied to the INTERSECT result respects the
   * column aliases defined in the first SELECT statement.
   */
  @Test
  void intersectSupportsFinalOrderBy() {
    String sql = """
        SELECT cyl AS cylinders FROM mtcars WHERE cyl IN (4, 6, 8)
        INTERSECT
        SELECT cyl FROM mtcars WHERE cyl IN (6, 8)
        ORDER BY cylinders DESC
        """;
    jparqSql.query(sql, rs -> {
      try {
        List<Integer> ordered = new ArrayList<>();
        while (rs.next()) {
          ordered.add(rs.getInt(1));
        }
        assertEquals(List.of(8, 6), ordered,
            "ORDER BY should operate on the final INTERSECT result using first-select aliases");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Ensure INTERSECT is evaluated before UNION when operators are mixed without
   * parentheses.
   */
  @Test
  void intersectHasHigherPrecedenceThanUnion() {
    String sql = """
        SELECT cyl FROM mtcars WHERE cyl = 4
        UNION
        SELECT cyl FROM mtcars WHERE cyl IN (6, 8)
        INTERSECT
        SELECT cyl FROM mtcars WHERE cyl = 6
        ORDER BY cyl
        """;
    jparqSql.query(sql, rs -> {
      try {
        List<Integer> values = new ArrayList<>();
        while (rs.next()) {
          values.add(rs.getInt(1));
        }
        assertEquals(List.of(4, 6), values, "INTERSECT must be applied before UNION when operators are mixed");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

}
