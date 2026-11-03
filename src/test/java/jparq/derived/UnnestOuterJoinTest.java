package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies independent and correlated UNNEST arguments in outer joins. */
class UnnestOuterJoinTest {
  /**
   * Preserves right-only rows for independent array and scalar-subquery
   * arguments.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void independentArgumentsPreserveOuterJoinRows(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      List<String> inputs = List.of("(VALUES (1)) v(k) RIGHT JOIN UNNEST(ARRAY[1, 2]) t(x) ON v.k = t.x",
          "(VALUES (1), (3)) v(k) FULL JOIN UNNEST(ARRAY[1, 2]) t(x) ON v.k = t.x",
          "(VALUES (1)) v(k) FULL JOIN UNNEST(ARRAY[(SELECT 1), 2]) t(x) ON v.k = t.x");
      List<List<String>> expected = List.of(List.of("1:1", "null:2"), List.of("1:1", "3:null", "null:2"),
          List.of("1:1", "null:2"));
      for (int i = 0; i < inputs.size(); i++) {
        String input = inputs.get(i);
        assertEquals(expected.get(i),
            rows(connection, "SELECT v.k, t.x FROM " + input + " ORDER BY v.k NULLS LAST, t.x NULLS LAST"), input);
        try (var statement = connection.prepareStatement("SELECT COUNT(*) FROM " + input);
            var result = statement.executeQuery()) {
          assertTrue(result.next(), input);
          assertEquals(expected.get(i).size(), result.getInt(1), input);
          assertFalse(result.next(), input);
        }
      }
    }
  }

  /**
   * Handles empty left inputs, inner-scope names, and UNNEST as the first input.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void independentArgumentsDoNotNeedALeftRow(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String join : List.of("RIGHT", "FULL")) {
        String sql = "SELECT v.k, t.x FROM (SELECT 1 AS k WHERE FALSE) v " + join
            + " JOIN UNNEST(ARRAY[1, 2]) t(x) ON v.k = t.x ORDER BY t.x";
        assertEquals(List.of("null:1", "null:2"), rows(connection, sql), sql);
        String shadowed = "SELECT v.k, t.x FROM (VALUES (1)) v(k) " + join
            + " JOIN UNNEST(ARRAY[(SELECT k FROM (VALUES (2)) z(k)), 1]) t(x) ON v.k = t.x ORDER BY t.x";
        assertEquals(List.of("1:1", "null:2"), rows(connection, shadowed), shadowed);
      }
      for (String join : List.of("RIGHT", "FULL")) {
        String first = "SELECT v.k, t.x FROM UNNEST(ARRAY[1, 2]) t(x) " + join
            + " JOIN (VALUES (1), (3)) v(k) ON v.k = t.x ORDER BY v.k NULLS LAST, t.x";
        assertEquals("RIGHT".equals(join) ? List.of("1:1", "3:null") : List.of("1:1", "3:null", "null:2"),
            rows(connection, first), first);
      }
    }
  }

  /**
   * Rejects correlations to the nonpreserved input, including when it is empty.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void rightAndFullRejectCorrelatedArguments(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String join : List.of("RIGHT", "FULL")) {
        String column = "SELECT COUNT(*) FROM (VALUES (ARRAY[1, 2])) v(a) " + join + " JOIN UNNEST(v.a) t(x) ON TRUE";
        var columnFailure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var statement = connection.prepareStatement(column); var result = statement.executeQuery()) {
            result.next();
          }
        }, column);
        assertEquals("42000", columnFailure.getSQLState(), column);
        for (String source : List.of("(VALUES (1)) v(k)", "(SELECT 1 AS k WHERE FALSE) v")) {
          for (String argument : List.of("ARRAY[v.k, 2]", "ARRAY[k, 2]", "ARRAY[(SELECT v.k), 2]")) {
            String sql = "SELECT COUNT(*) FROM " + source + " " + join + " JOIN UNNEST(" + argument
                + ") t(x) ON v.k = t.x";
            var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
              try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
                result.next();
              }
            }, sql);
            assertEquals("42000", failure.getSQLState(), sql);
          }
        }
      }
    }
  }

  /**
   * Reads the outer join's visible values, including its null-extended rows.
   *
   * @param connection
   *          the fixture connection
   * @param sql
   *          the query projecting k and x
   * @return values in result order
   * @throws Exception
   *           if execution fails
   */
  private static List<String> rows(Connection connection, String sql) throws Exception {
    List<String> values = new ArrayList<>();
    try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
      while (result.next()) {
        values.add(result.getObject(1) + ":" + result.getObject(2));
      }
    }
    return values;
  }

  /**
   * Builds a fixture URL with the requested pushdown options.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @return the JDBC URL
   */
  private static String url(boolean pushdown) {
    return "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown=" + pushdown
        + "&projectionPushdown=" + pushdown;
  }
}
