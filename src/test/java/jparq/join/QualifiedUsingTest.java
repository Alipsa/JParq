package jparq.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies that qualified USING keys retain each input's NULL extension. */
class QualifiedUsingTest {

  /**
   * Checks matched rows and both unmatched sides for every outer join type.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void outerJoinKeys(boolean pushdown) throws Exception {
    String inputs = " FROM (VALUES (1), (2)) a(x) %s JOIN (VALUES (2), (3)) b(x) USING (x)";
    List<List<Object>> full = Arrays.asList(Arrays.asList(1, 1, null), Arrays.asList(2, 2, 2),
        Arrays.asList(3, null, 3));
    for (String type : List.of("LEFT", "RIGHT", "FULL", "INNER")) {
      List<List<Object>> expected = switch (type) {
        case "LEFT" -> full.subList(0, 2);
        case "RIGHT" -> full.subList(1, 3);
        case "INNER" -> full.subList(1, 2);
        default -> full;
      };
      String from = inputs.formatted(type);
      assertEquals(expected, rows("SELECT x AS merged, a.x, b.x" + from + " ORDER BY merged", pushdown), type);
      assertEquals(expected.stream().map(row -> row.subList(1, 3)).toList(),
          rows("SELECT a.*, b.*" + from + " ORDER BY COALESCE(a.x, b.x)", pushdown), type + " wildcard");
      assertEquals(expected.stream().map(row -> row.subList(0, 1)).toList(),
          rows("SELECT *" + from + " ORDER BY x", pushdown), type + " merged wildcard");
    }
  }

  /**
   * Reproduces the physical-table example and checks qualified NULL predicates.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void physicalLeftJoin(boolean pushdown) throws Exception {
    String from = " FROM mtcars m LEFT JOIN (SELECT 'zz' AS model) n USING (model)";
    assertEquals(Arrays.asList(Arrays.asList("Mazda RX4", null)),
        rows("SELECT m.model, n.model" + from + " LIMIT 1", pushdown));
    assertEquals(List.of(List.of(32L)), rows("SELECT COUNT(*)" + from + " WHERE n.model IS NULL", pushdown));
    assertEquals(List.of(List.of(0L)), rows("SELECT COUNT(*)" + from + " WHERE n.model IS NOT NULL", pushdown));
  }

  /**
   * Checks that later joins bind each qualified key and the accumulated merged
   * key.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void chainedJoins(boolean pushdown) throws Exception {
    String from = " FROM (VALUES (1)) a(x) FULL JOIN (VALUES (2)) b(x) USING (x)";
    assertEquals(Arrays.asList(Arrays.asList(1, 1, null, 1), Arrays.asList(2, null, 2, 2)),
        rows("SELECT x AS merged, a.x, b.x, c.x" + from + " JOIN (VALUES (1), (2)) c(x) USING (x) ORDER BY merged",
            pushdown));
    assertEquals(Arrays.asList(Arrays.asList(1, 1, null, null), Arrays.asList(2, null, 2, 2)), rows(
        "SELECT x AS merged, a.x, b.x, c.y" + from + " LEFT JOIN (VALUES (1), (2)) c(y) ON b.x = c.y ORDER BY merged",
        pushdown));
  }

  /**
   * Checks that parenthesized joins retain their input values and wildcard shape.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void nestedJoins(boolean pushdown) throws Exception {
    String from = " FROM ((VALUES (1)) a(x) FULL JOIN (VALUES (2)) b(x) USING (x))";
    assertEquals(Arrays.asList(Arrays.asList(1, 1, null), Arrays.asList(2, null, 2)),
        rows("SELECT x AS merged, a.x, b.x" + from + " ORDER BY merged", pushdown));
    assertEquals(List.of(List.of(1), List.of(2)), rows("SELECT *" + from + " ORDER BY x", pushdown));
  }

  /**
   * Checks empty metadata, alias lists, and materialized wildcard projections.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void materializedAndEmptyResults(boolean pushdown) throws Exception {
    String joined = "(VALUES (1)) a(x) FULL JOIN (VALUES (2)) b(x) USING (x)";
    assertEquals(List.of(List.of(1), List.of(2)),
        rows("SELECT * FROM (SELECT * FROM " + joined + ") t ORDER BY x", pushdown));
    assertEquals(List.of(List.of(1), List.of(2)), rows("SELECT t.* FROM (" + joined + ") t(y) ORDER BY y", pushdown));
    assertEquals(Arrays.asList(Arrays.asList(null, 1L), Arrays.asList(2, 1L)),
        rows("SELECT b.x, COUNT(*) FROM " + joined + " GROUP BY b.x ORDER BY b.x NULLS FIRST", pushdown));
    assertEquals(List.of(), rows("SELECT * FROM " + joined + " WHERE 1 = 0", pushdown, 1));
  }

  /**
   * Ensures actual NULL join keys remain distinct unmatched rows.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void nullKeys(boolean pushdown) throws Exception {
    assertEquals(Arrays.asList(Arrays.asList(null, null, null), Arrays.asList(null, null, null)),
        rows("SELECT x AS merged, a.x, b.x FROM (VALUES (CAST(NULL AS INTEGER))) a(x)"
            + " FULL JOIN (VALUES (CAST(NULL AS INTEGER))) b(x) USING (x)", pushdown));
    assertEquals(Arrays.asList(Arrays.asList(1, 2, 1, 2, null, null), Arrays.asList(1, 3, null, null, 1, 3)),
        rows("SELECT x AS merged, y AS second_key, a.*, b.* FROM (VALUES (1, 2)) a(x, y)"
            + " FULL JOIN (VALUES (1, 3)) b(x, y) USING (x, y) ORDER BY second_key", pushdown));
  }

  /**
   * Rejects internal input fields as keys in a later USING clause.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void hiddenUsingKeyIsRejected(boolean pushdown) throws Exception {
    SQLException failure = assertThrows(SQLException.class,
        () -> rows("SELECT * FROM ((VALUES (1)) a(x) FULL JOIN (VALUES (2)) b(x) USING (x))"
            + " JOIN (VALUES (1)) c(a__x) USING (a__x)", pushdown));
    assertEquals("42000", failure.getSQLState());
    assertTrue(failure.getMessage().contains("USING column 'a__x' not found"), failure.getMessage());
  }

  /**
   * Distinguishes each qualified grouping key from the merged key.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void mergedGroupingDoesNotGroupInputKeys(boolean pushdown) throws Exception {
    String from = " FROM (VALUES (1)) a(x) FULL JOIN (VALUES (2)) b(x) USING (x)";
    for (String projection : List.of("b.x", "a.x, x", "b.x + 1", "COALESCE(a.x, 0)")) {
      SQLException failure = assertThrows(SQLException.class,
          () -> rows("SELECT " + projection + from + " GROUP BY x", pushdown));
      assertTrue(failure.getMessage().contains("GROUP BY"), failure.getMessage());
    }
    assertThrows(SQLException.class, () -> rows("SELECT b.x" + from + " WHERE 1 = 0 GROUP BY x", pushdown));
    assertThrows(SQLException.class, () -> rows("SELECT x" + from + " GROUP BY b.x", pushdown));
    assertThrows(SQLException.class, () -> rows("SELECT x" + from + " GROUP BY x HAVING b.x IS NULL", pushdown));
    assertEquals(List.of(List.of(1), List.of(2)), rows("SELECT x" + from + " GROUP BY x ORDER BY x", pushdown));
    assertEquals(Arrays.asList(Arrays.asList((Object) null), List.of(2)),
        rows("SELECT b.x" + from + " GROUP BY b.x ORDER BY b.x NULLS FIRST", pushdown));
    assertEquals(List.of(List.of(1), List.of(2)),
        rows("SELECT a.x FROM (VALUES (1), (2)) a(x) GROUP BY x ORDER BY a.x", pushdown));
  }

  /**
   * Rejects ordering by a repeated label for distinct bound expressions.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void mergedAndQualifiedOrderingLabelsAreAmbiguous(boolean pushdown) throws Exception {
    String from = " FROM (VALUES (1)) a(x) FULL JOIN (VALUES (2)) b(x) USING (x)";
    SQLException failure = assertThrows(SQLException.class,
        () -> rows("SELECT x, a.x" + from + " ORDER BY x", pushdown));
    assertEquals("42702", failure.getSQLState());
    assertTrue(failure.getMessage().contains("Ambiguous column reference: x"), failure.getMessage());
    assertEquals(Arrays.asList(Arrays.asList(1, 1), Arrays.asList(2, null)),
        rows("SELECT x, a.x" + from + " ORDER BY 1", pushdown));
  }

  /**
   * Sorts grouped output by its selected position or bound grouping column.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if setup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void groupedOrderingKeepsProjectionIdentity(boolean pushdown) throws Exception {
    String from = " FROM (VALUES (1), (3)) a(x) FULL JOIN (VALUES (2), (3)) b(x) USING (x)";
    List<List<Object>> mergedOrder = Arrays.asList(Arrays.asList(null, 1), List.of(2, 2), List.of(3, 3));
    List<List<Object>> rightOrder = Arrays.asList(List.of(2, 2), List.of(3, 3), Arrays.asList(null, 1));
    for (String grouping : List.of("", " GROUP BY x, b.x")) {
      for (String ordering : List.of("m", "2")) {
        assertEquals(mergedOrder, rows("SELECT b.x, x AS m" + from + grouping + " ORDER BY " + ordering, pushdown));
      }
      for (String ordering : List.of("b.x", "1")) {
        assertEquals(rightOrder,
            rows("SELECT b.x, x AS m" + from + grouping + " ORDER BY " + ordering + " NULLS LAST", pushdown));
      }
    }
    assertEquals(Arrays.asList(Arrays.asList((Object) null), List.of(2), List.of(3)),
        rows("SELECT b.x AS right_key" + from + " GROUP BY x, b.x ORDER BY x", pushdown));
    assertEquals(Arrays.asList(List.of(3, 3), List.of(2, 2), Arrays.asList(null, 1)),
        rows("SELECT b.x, x AS m" + from + " GROUP BY x, b.x ORDER BY m DESC", pushdown));
    assertEquals(List.of(List.of(3, 3), List.of(2, 2)),
        rows("SELECT b.x, x AS m" + from + " GROUP BY x, b.x ORDER BY 2 DESC LIMIT 2", pushdown));
  }

  /**
   * Executes SQL with a fixed pushdown setting and collects nullable column
   * values.
   *
   * @param sql
   *          query to execute
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @return result rows in query order
   * @throws Exception
   *           if setup or query execution fails
   */
  private List<List<Object>> rows(String sql, boolean pushdown) throws Exception {
    return rows(sql, pushdown, -1);
  }

  /**
   * Collects query rows and optionally checks the metadata column count.
   *
   * @param sql
   *          query to execute
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @param expectedColumns
   *          expected column count, or -1 to skip the check
   * @return result rows in query order
   * @throws Exception
   *           if setup or query execution fails
   */
  private List<List<Object>> rows(String sql, boolean pushdown, int expectedColumns) throws Exception {
    Path directory = Path.of(QualifiedUsingTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      List<List<Object>> rows = new ArrayList<>();
      int columnCount = result.getMetaData().getColumnCount();
      if (expectedColumns >= 0) {
        assertEquals(expectedColumns, columnCount, "Wildcard metadata must omit qualified-only fields");
      }
      while (result.next()) {
        List<Object> row = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          Object value = result.getObject(i);
          row.add(value instanceof Number number && !(value instanceof Long) ? number.intValue() : value);
        }
        rows.add(row);
      }
      return rows;
    }
  }
}
