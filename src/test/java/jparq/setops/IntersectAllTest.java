package jparq.setops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests covering INTERSECT ALL query semantics. */
public class IntersectAllTest {

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
    URL mtcarsUrl = IntersectAllTest.class.getResource("/mtcars.parquet");
    if (mtcarsUrl == null) {
      throw new IllegalStateException("mtcars.parquet must be available on the test classpath");
    }
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that INTERSECT ALL preserves duplicate rows according to multiset
   * semantics.
   */
  @Test
  void intersectAllPreservesMultiplicity() {
    String sql = """
        WITH left_data(val) AS (
          VALUES (1), (1), (1), (2)
        ),
        right_data(val) AS (
          VALUES (1), (1), (3)
        )
        SELECT val FROM left_data
        INTERSECT ALL
        SELECT val FROM right_data
        ORDER BY val
        """;
    List<Integer> values = collectInts(sql);
    assertEquals(List.of(1, 1), values,
        "INTERSECT ALL should emit each common row the minimum number of times it appears in the inputs");
  }

  /**
   * Ensure INTERSECT ALL retains duplicates that the standard INTERSECT removes.
   */
  @Test
  void intersectAllDiffersFromDistinctIntersect() {
    String base = """
        WITH left_data(val) AS (
          VALUES (5), (5), (7)
        ),
        right_data(val) AS (
          VALUES (5), (5), (8)
        )
        SELECT val FROM left_data
        %s
        SELECT val FROM right_data
        ORDER BY val
        """;
    List<Integer> allValues = collectInts(String.format(Locale.ROOT, base, "INTERSECT ALL"));
    List<Integer> distinctValues = collectInts(String.format(Locale.ROOT, base, "INTERSECT"));
    assertEquals(List.of(5, 5), allValues,
        "INTERSECT ALL should preserve duplicate intersections present in both inputs");
    assertEquals(List.of(5), distinctValues,
        "INTERSECT should deduplicate the intersection result");
  }

  /**
   * Ensure INTERSECT ALL enforces union compatibility for the participating
   * queries.
   */
  @Test
  void intersectAllRequiresUnionCompatibleInputs() {
    String sql = """
        SELECT cyl FROM mtcars
        INTERSECT ALL
        SELECT cyl, gear FROM mtcars
        """;
    RuntimeException ex = assertThrows(RuntimeException.class, () -> jparqSql.query(sql, rs -> {
    }));
    assertTrue(ex.getCause() instanceof SQLException,
        "The engine should surface union compatibility errors via SQLException");
    assertTrue(ex.getCause().getMessage().contains("same number of columns"),
        "The error message should describe the union compatibility violation");
  }

  /**
   * Execute the provided SQL and collect the integer values from the first
   * column of the result set.
   *
   * @param sql
   *          the SQL to execute
   * @return a list containing the integer values in encounter order
   */
  private List<Integer> collectInts(String sql) {
    List<Integer> values = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          values.add(rs.getInt(1));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    return values;
  }
}
