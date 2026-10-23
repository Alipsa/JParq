package jparq.aggregate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Prevents aggregate FILTER predicates from being silently discarded. */
class AggregateFilterTest {
  private static String jdbcUrl;

  /**
   * Locates the mtcars fixture.
   *
   * @throws Exception
   *           if the fixture cannot be located
   */
  @BeforeAll
  static void locateFixtures() throws Exception {
    jdbcUrl = "jdbc:jparq:" + Path.of(AggregateFilterTest.class.getResource("/mtcars.parquet").toURI()).getParent();
  }

  /**
   * Rejects FILTER during preparation across aggregate and query shapes.
   *
   * @param sql
   *          query containing an unsupported FILTER clause
   * @throws Exception
   *           if a connection cannot be opened
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT COUNT(*) FILTER (WHERE cyl IN (4,6)) FROM mtcars",
      "SELECT COUNT(DISTINCT cyl) FILTER (WHERE hp > 100) FROM mtcars",
      "SELECT SUM(hp) FILTER (WHERE cyl = 4), AVG(mpg) FILTER (WHERE cyl = 6) FROM mtcars",
      "SELECT MIN(hp) FILTER (WHERE cyl = 4), MAX(hp) FILTER (WHERE cyl = 6) FROM mtcars",
      "SELECT cyl, COUNT(*) FILTER (WHERE hp > 100) FROM mtcars GROUP BY cyl",
      "SELECT cyl FROM mtcars GROUP BY cyl HAVING COUNT(*) FILTER (WHERE hp > 100) > 0",
      "SELECT COUNT(*) FILTER (WHERE cyl = 4) OVER () FROM mtcars",
      "SELECT SUM(hp) FILTER (WHERE cyl = 4) OVER (ORDER BY mpg) FROM mtcars",
      "SELECT COUNT(*) FILTER (WHERE cyl = ?) FROM mtcars",
      "SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars WHERE 1 = 0",
      "SELECT CASE WHEN 1 = 0 THEN COUNT(*) FILTER (WHERE cyl = 4) ELSE 0 END FROM mtcars",
      "WITH c AS (SELECT COUNT(*) FILTER (WHERE cyl = 4) AS n FROM mtcars) SELECT n FROM c",
      "WITH c AS (SELECT COUNT(*) FILTER (WHERE cyl = 4) AS n FROM mtcars) SELECT 1",
      "SELECT d.n FROM (SELECT COUNT(*) FILTER (WHERE cyl = 4) AS n FROM mtcars) d",
      "SELECT (SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars)",
      "SELECT 1 FROM mtcars WHERE 1 < ANY (SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars)",
      "SELECT COUNT(*) FROM mtcars UNION ALL SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars",
      "SELECT cyl FROM mtcars ORDER BY COUNT(*) FILTER (WHERE cyl = 4)",
      "SELECT 1 FROM mtcars WHERE EXISTS (SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars)",
      "SELECT 1 FROM mtcars m JOIN mtcars n ON m.cyl = COUNT(*) FILTER (WHERE n.cyl = 4)",
      "SELECT 1 FROM mtcars GROUP BY COUNT(*) FILTER (WHERE cyl = 4)", "VALUES (COUNT(*) FILTER (WHERE 1 = 1))",
      "WITH c AS (SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars) SELECT 1 UNION ALL SELECT 2",
      "SELECT 1 FROM (mtcars m JOIN mtcars n ON m.cyl = COUNT(*) FILTER (WHERE n.cyl = 4))",
      "SELECT mpg FROM mtcars LIMIT (SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars)",
      "SELECT 1 UNION ALL SELECT 2 LIMIT (SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars)",
      "SELECT * FROM UNNEST(ARRAY[(SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars)]) AS t(x)",
      "SELECT 1 FROM mtcars WHERE 1 < ALL (SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars)",
      "SELECT 1 FROM mtcars WHERE 1 < SOME (SELECT COUNT(*) FILTER (WHERE cyl = 4) FROM mtcars)"
  })
  void rejectsFilterBeforeExecution(String sql) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      try (var connection = DriverManager.getConnection(jdbcUrl + "?predicatePushdown=" + pushdown);
          var statement = connection.createStatement()) {
        var preparationFailure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(sql));
        assertEquals("42000", preparationFailure.getSQLState(), sql);
        assertTrue(preparationFailure.getMessage().contains("FILTER (WHERE ...) clauses are not supported"), sql);
        var executionFailure = assertThrows(SQLSyntaxErrorException.class, () -> statement.executeQuery(sql));
        assertEquals("42000", executionFailure.getSQLState(), sql);
      }
    }
  }

  /**
   * Verifies the documented alternatives for the issue's filtered count.
   *
   * @param sql
   *          supported alternative query
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT COUNT(*) FROM mtcars WHERE cyl IN (4,6)", "SELECT COUNT(CASE WHEN cyl IN (4,6) THEN 1 END) FROM mtcars"
  })
  void supportsFilteredCountAlternatives(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertTrue(result.next());
      assertEquals(18, result.getLong(1));
      assertFalse(result.next());
    }
  }

  /**
   * Keeps ordinary aggregates, windows, and text mentioning FILTER usable.
   *
   * @param sql
   *          supported query with a known first value
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT COUNT(*) FROM mtcars", "SELECT COUNT(*) OVER () FROM mtcars LIMIT 1",
      "SELECT COUNT(*) AS filter FROM mtcars",
      "SELECT COUNT(*) FROM mtcars WHERE 'FILTER (WHERE cyl = 4)' = 'FILTER (WHERE cyl = 4)'",
      "SELECT COUNT(*) /* FILTER (WHERE cyl = 4) */ FROM mtcars"
  })
  void preservesSupportedQueries(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertTrue(result.next());
      assertEquals(32, result.getLong(1));
      assertFalse(result.next());
    }
  }
}
