package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies alias binding when nested statements inherit their CTE inputs. */
class InheritedCteAliasTest {

  /**
   * Checks aliases in scalar, predicate, derived, LATERAL, and subsequent CTE
   * queries.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void nestedReferencesRetainAliases(boolean predicate, boolean projection) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown=" + predicate
        + "&projectionPushdown=" + projection;
    String cte = "WITH c AS (SELECT id FROM optimized WHERE id <= 2) ";
    try (var connection = DriverManager.getConnection(url)) {
      for (String source : List.of("c x(a)", "c AS x(a)", "(c) x(a)", "((c)) x(a)")) {
        String scalar = cte + "SELECT (SELECT MAX(a) FROM " + source + ") m";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(scalar)) {
          assertTrue(result.next(), scalar);
          assertEquals(2, result.getInt(1), scalar);
          assertFalse(result.next(), scalar);
        }
        for (String sql : List.of(
            cte + "SELECT o.id FROM optimized o WHERE o.id <= 2 AND EXISTS (SELECT 1 FROM " + source
                + " WHERE x.a = o.id) ORDER BY o.id",
            cte + "SELECT o.id FROM optimized o WHERE o.id IN (SELECT a FROM " + source + ") ORDER BY o.id",
            cte + "SELECT * FROM (SELECT a FROM " + source + ") d ORDER BY a",
            cte + "SELECT * FROM (SELECT x.a FROM " + source + " JOIN (VALUES (1)) s(z) ON x.a >= s.z) d ORDER BY a",
            cte + "SELECT l.a FROM optimized o CROSS JOIN LATERAL (SELECT a FROM " + source
                + " WHERE a = o.id) l WHERE o.id <= 2 ORDER BY l.a",
            "WITH c AS (SELECT id FROM optimized WHERE id <= 2), d AS (SELECT a FROM " + source
                + ") SELECT a FROM d ORDER BY a")) {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            for (int id = 1; id <= 2; id++) {
              assertTrue(result.next(), sql);
              assertEquals(id, result.getInt(1), sql);
            }
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Rejects physical-table alias lists explicitly rather than silently ignoring
   * them.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @throws Exception
   *           if JDBC execution fails unexpectedly
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rejectsUnsupportedPhysicalAliases(boolean predicate, boolean projection) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown=" + predicate
        + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String source : List.of("optimized x(a)", "optimized AS x(a)", "(optimized) x(a)")) {
        for (String sql : List.of("SELECT * FROM " + source,
            "SELECT * FROM " + source + " JOIN (VALUES (1)) s(z) ON TRUE",
            "SELECT * FROM (SELECT * FROM " + source + ") d")) {
          SQLException failure = assertThrows(SQLException.class, () -> {
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("Column alias lists on physical tables are not supported"),
              sql + ": " + failure);
        }
      }
    }
  }
}
