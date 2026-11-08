package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies qualified NULL bindings in non-aggregate scalar subqueries. */
class CorrelatedNullBindingTest {
  /**
   * Retains qualified NULLs in direct and arithmetic projection subqueries.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void projectionsKeepQualifiedNulls(boolean predicate, boolean projection) throws Exception {
    try (
        var connection = DriverManager
            .getConnection("jdbc:jparq:.?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
        var statement = connection.createStatement()) {
      for (String scalar : List.of("(SELECT o.v)", "(SELECT o.v + x.v)")) {
        String sql = "SELECT o.g, " + scalar + input() + " ORDER BY o.g";
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertNull(result.getObject(2), sql);
          assertTrue(result.next(), sql);
          assertEquals(2, result.getInt(1), sql);
          assertEquals(scalar.contains("+") ? 15 : 5, result.getInt(2), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Preserves UNKNOWN in WHERE and distinguishes NULL from the joined value.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void whereKeepsQualifiedNulls(boolean predicate, boolean projection) throws Exception {
    try (
        var connection = DriverManager
            .getConnection("jdbc:jparq:.?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
        var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT o.g" + input() + " WHERE (SELECT o.v) = 10")) {
        assertFalse(result.next());
      }
      for (String condition : List.of("(SELECT o.v) = 5", "(SELECT o.v + x.v) = 15", "(SELECT o.v) IS NULL")) {
        String sql = "SELECT o.g" + input() + " WHERE " + condition;
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(condition.endsWith("IS NULL") ? 1 : 2, result.getInt(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Supplies two identically named columns, including a typed outer NULL.
   *
   * @return the joined VALUES input shared by projection and predicate queries
   */
  private static String input() {
    return " FROM (VALUES (1, CAST(NULL AS INTEGER)), (2, 5)) o(g, v) CROSS JOIN (VALUES (10)) x(v)";
  }
}
