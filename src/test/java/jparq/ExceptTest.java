package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

/** Tests covering EXCEPT query semantics. */
public class ExceptTest {

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
    URL mtcarsUrl = ExceptTest.class.getResource("/mtcars.parquet");
    if (mtcarsUrl == null) {
      throw new IllegalStateException("mtcars.parquet must be available on the test classpath");
    }
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that EXCEPT returns only the rows from the left input that are not
   * present in the right input and removes duplicates.
   */
  @Test
  void exceptReturnsDistinctDirectionalRows() {
    String sql = """
        SELECT cyl FROM mtcars WHERE cyl IN (4, 6, 8)
        EXCEPT
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
        assertEquals(1, rows, "EXCEPT should only emit rows exclusive to the left input");
        assertEquals(Set.of(4), values, "EXCEPT must deduplicate rows and preserve directionality");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Verify that an ORDER BY clause applied to the EXCEPT result respects column
   * aliases defined in the first SELECT statement.
   */
  @Test
  void exceptSupportsFinalOrderBy() {
    String sql = """
        SELECT cyl AS cylinders FROM mtcars WHERE cyl IN (4, 6, 8)
        EXCEPT
        SELECT cyl FROM mtcars WHERE cyl = 8
        ORDER BY cylinders DESC
        """;
    jparqSql.query(sql, rs -> {
      try {
        List<Integer> ordered = new ArrayList<>();
        while (rs.next()) {
          ordered.add(rs.getInt(1));
        }
        assertEquals(List.of(6, 4), ordered,
            "ORDER BY should operate on the final EXCEPT result using first-select aliases");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Ensure that EXCEPT treats NULL values as equal when determining whether a row
   * should be excluded by the right input.
   */
  @Test
  void exceptTreatsNullsAsEqual() {
    String sql = """
        SELECT CASE WHEN cyl = 4 THEN NULL ELSE cyl END AS val FROM mtcars WHERE cyl IN (4, 6)
        EXCEPT
        SELECT CASE WHEN cyl = 6 THEN NULL ELSE cyl END FROM mtcars WHERE cyl = 6
        """;
    jparqSql.query(sql, rs -> {
      try {
        List<Integer> values = new ArrayList<>();
        while (rs.next()) {
          values.add(rs.getInt(1));
        }
        assertEquals(List.of(6), values,
            "NULL rows in the left input should be removed when matched by the right input");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Ensure EXCEPT has the same precedence as UNION and evaluates left-to-right
   * when operators are mixed without parentheses.
   */
  @Test
  void exceptEvaluatesLeftToRightWithUnion() {
    String sql = """
        SELECT cyl FROM mtcars WHERE cyl IN (4, 6)
        EXCEPT
        SELECT cyl FROM mtcars WHERE cyl = 4
        UNION
        SELECT cyl FROM mtcars WHERE cyl = 8
        ORDER BY cyl
        """;
    jparqSql.query(sql, rs -> {
      try {
        List<Integer> values = new ArrayList<>();
        while (rs.next()) {
          values.add(rs.getInt(1));
        }
        assertEquals(List.of(6, 8), values,
            "EXCEPT should evaluate with the same precedence as UNION using left-to-right association");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Ensure that EXCEPT ALL is rejected as it is not supported by the engine.
   */
  @Test
  void exceptAllIsRejected() {
    String sql = """
        SELECT cyl FROM mtcars
        EXCEPT ALL
        SELECT cyl FROM mtcars
        """;
    RuntimeException ex = assertThrows(RuntimeException.class, () -> jparqSql.query(sql, rs -> {
    }));
    assertTrue(ex.getCause() instanceof SQLException, "Expected SQLException for EXCEPT ALL rejection");
    Throwable root = ex.getCause().getCause();
    assertTrue(root instanceof IllegalArgumentException, "Underlying cause should describe unsupported EXCEPT ALL");
    assertTrue(root.getMessage().contains("EXCEPT ALL"), "Error message should mention EXCEPT ALL");
  }
}
