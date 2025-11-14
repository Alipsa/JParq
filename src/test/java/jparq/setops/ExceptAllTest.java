package jparq.setops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests covering EXCEPT ALL query semantics. */
public class ExceptAllTest {

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
    URL mtcarsUrl = ExceptAllTest.class.getResource("/mtcars.parquet");
    if (mtcarsUrl == null) {
      throw new IllegalStateException("mtcars.parquet must be available on the test classpath");
    }
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Verify that EXCEPT ALL subtracts row multiplicities according to the SQL
   * standard.
   */
  @Test
  void exceptAllSubtractsMultiplicity() {
    String subtractCte = """
        WITH subtract AS (
            SELECT cyl, gear FROM (
                SELECT cyl, gear FROM mtcars
                WHERE cyl = 4 AND gear = 4
                ORDER BY model
                LIMIT 3
            )
            UNION ALL
            SELECT cyl, gear FROM (
                SELECT cyl, gear FROM mtcars
                WHERE cyl = 6 AND gear = 3
                ORDER BY model
                LIMIT 2
            )
        )
        """;
    String left = "SELECT cyl, gear FROM mtcars";
    String right = subtractCte + "\nSELECT cyl, gear FROM subtract";
    String differenceSql = subtractCte
        + "\nSELECT cyl, gear FROM mtcars"
        + "\nEXCEPT ALL\nSELECT cyl, gear FROM subtract";
    List<List<Integer>> leftRows = queryIntRows(left);
    List<List<Integer>> rightRows = queryIntRows(right);
    Map<List<Integer>, Long> leftCounts = countOccurrences(leftRows);
    Map<List<Integer>, Long> rightCounts = countOccurrences(rightRows);

    List<List<Integer>> resultRows = queryIntRows(differenceSql);
    Map<List<Integer>, Long> resultCounts = countOccurrences(resultRows);

    for (Map.Entry<List<Integer>, Long> entry : leftCounts.entrySet()) {
      List<Integer> key = entry.getKey();
      long expectedCount = Math.max(0, entry.getValue() - rightCounts.getOrDefault(key, 0L));
      assertEquals(expectedCount, resultCounts.getOrDefault(key, 0L),
          () -> "Unexpected multiplicity for row " + key);
    }
  }

  /**
   * Ensure that EXCEPT ALL never produces negative multiplicities when the right
   * input has more duplicates than the left input.
   */
  @Test
  void exceptAllDoesNotProduceNegativeCounts() {
    String sql = """
        SELECT val FROM (VALUES (1), (1)) AS lhs(val)
        EXCEPT ALL
        SELECT val FROM (VALUES (1), (1), (1)) AS rhs(val)
        """;
    List<List<Integer>> rows = queryIntRows(sql);
    assertTrue(rows.isEmpty(), "All rows should be removed when the right input has more duplicates");
  }

  /**
   * Demonstrate that EXCEPT ALL preserves duplicates compared to EXCEPT when the
   * left input contains repeated rows.
   */
  @Test
  void exceptAllPreservesDuplicatesComparedToDistinctExcept() {
    String left = "SELECT val FROM (VALUES (1), (1), (1)) AS lhs(val)";
    String right = "SELECT val FROM (VALUES (1)) AS rhs(val)";

    List<List<Integer>> leftRows = queryIntRows(left);
    List<List<Integer>> rightRows = queryIntRows(right);
    Map<List<Integer>, Long> leftCounts = countOccurrences(leftRows);
    Map<List<Integer>, Long> rightCounts = countOccurrences(rightRows);

    List<List<Integer>> distinctResult = queryIntRows(left + "\nEXCEPT\n" + right);
    List<List<Integer>> allResult = queryIntRows(left + "\nEXCEPT ALL\n" + right);

    Map<List<Integer>, Long> expectedAllCounts = new HashMap<>();
    for (Map.Entry<List<Integer>, Long> entry : leftCounts.entrySet()) {
      long expectedCount = Math.max(0, entry.getValue() - rightCounts.getOrDefault(entry.getKey(), 0L));
      if (expectedCount > 0) {
        expectedAllCounts.put(entry.getKey(), expectedCount);
      }
    }
    Map<List<Integer>, Long> actualAllCounts = countOccurrences(allResult);

    assertTrue(distinctResult.isEmpty(), "EXCEPT should remove duplicates by default");
    assertEquals(expectedAllCounts, actualAllCounts,
        "EXCEPT ALL should preserve remaining duplicates after subtracting the right input");
  }

  private static List<List<Integer>> queryIntRows(String sql) {
    List<List<Integer>> rows = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
          List<Integer> row = new ArrayList<>(columnCount);
          for (int col = 1; col <= columnCount; col++) {
            Object value = rs.getObject(col);
            row.add(value == null ? null : ((Number) value).intValue());
          }
          rows.add(List.copyOf(row));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    return rows;
  }

  private static Map<List<Integer>, Long> countOccurrences(List<List<Integer>> rows) {
    Map<List<Integer>, Long> counts = new HashMap<>();
    for (List<Integer> row : rows) {
      counts.merge(row, 1L, Long::sum);
    }
    return counts;
  }
}
