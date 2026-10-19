package jparq.cte;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Preserves decimal values and metadata across materialization boundaries. */
class DecimalMaterializationTest {

  private static String jdbcUrl;

  /**
   * Locates the committed decimal fixtures.
   *
   * @throws URISyntaxException
   *           if the fixture URL cannot be converted to a path
   */
  @BeforeAll
  static void locateFixtures() throws URISyntaxException {
    jdbcUrl = "jdbc:jparq:" + Path.of(DecimalMaterializationTest.class.getResource("/hf").toURI());
  }

  /**
   * Checks the same physical decimal column directly, through CTEs, and through
   * derived tables.
   *
   * @param sql
   *          the query selecting the fixture's decimal amount
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT amount FROM optimized WHERE id <= 2 ORDER BY id",
      "WITH t AS (SELECT id, amount FROM optimized WHERE id <= 2) SELECT amount FROM t ORDER BY id",
      "SELECT d.amount FROM (SELECT id, amount FROM optimized WHERE id <= 2) d ORDER BY d.id",
      "WITH t AS (SELECT id, amount FROM optimized WHERE id <= 2) "
          + "SELECT d.amount FROM (SELECT id, amount FROM t) d ORDER BY d.id"
  })
  void physicalDecimalsRetainValuesAndLogicalMetadata(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      var metadata = result.getMetaData();
      assertEquals(Types.DECIMAL, metadata.getColumnType(1));
      assertEquals(12, metadata.getPrecision(1));
      assertEquals(2, metadata.getScale(1));
      for (String expected : List.of("0.01", "0.02")) {
        assertTrue(result.next());
        assertEquals(new BigDecimal(expected), result.getBigDecimal(1));
      }
      assertFalse(result.next());
      assertEquals(12, result.getMetaData().getPrecision(1));
      assertEquals(2, result.getMetaData().getScale(1));
    }
  }

  /**
   * Checks aggregate decimals directly and after CTE or derived-table
   * materialization.
   *
   * @param sql
   *          the aggregate query
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT SUM(amount) AS s FROM optimized", "WITH t AS (SELECT SUM(amount) AS s FROM optimized) SELECT s FROM t",
      "SELECT d.s FROM (SELECT SUM(amount) AS s FROM optimized) d"
  })
  void aggregateDecimalsRetainTheirFractionalPart(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
      assertEquals(9, result.getMetaData().getPrecision(1));
      assertEquals(2, result.getMetaData().getScale(1));
      assertTrue(result.next());
      assertEquals(new BigDecimal("1342259.20"), result.getBigDecimal(1));
      assertFalse(result.next());
      assertEquals(2, result.getMetaData().getScale(1));
    }
  }

  /**
   * Preserves decimals produced by grouped aggregates of explicit casts.
   *
   * @param sql
   *          the grouped aggregate query
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "WITH t AS (SELECT id, SUM(CAST(1.25 AS DECIMAL(5,2))) AS s "
          + "FROM optimized WHERE id <= 2 GROUP BY id) SELECT s FROM t ORDER BY id",
      "SELECT d.s FROM (SELECT id, SUM(CAST(1.25 AS DECIMAL(5,2))) AS s "
          + "FROM optimized WHERE id <= 2 GROUP BY id) d ORDER BY d.id"
  })
  void groupedComputedDecimalsRemainExact(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals(3, result.getMetaData().getPrecision(1));
      assertEquals(2, result.getMetaData().getScale(1));
      for (int row = 0; row < 2; row++) {
        assertTrue(result.next());
        assertEquals(new BigDecimal("1.25"), result.getBigDecimal(1));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Keeps decimal metadata available for an empty physical input.
   *
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @Test
  void emptyDecimalInputsRetainTheirSchema() throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement
            .executeQuery("WITH t AS (SELECT amount FROM optimized WHERE id < 0) SELECT amount FROM t")) {
      assertEquals(12, result.getMetaData().getPrecision(1));
      assertEquals(2, result.getMetaData().getScale(1));
      assertFalse(result.next());
    }
  }

  /**
   * Verifies the explicitly typed anchor recommended for fractional recursion.
   *
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @Test
  void explicitDecimalAnchorAcceptsFractionalSteps() throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            WITH RECURSIVE r(n) AS (
              SELECT CAST(1 AS DECIMAL(10,1)) FROM optimized WHERE id = 1
              UNION ALL SELECT n + 0.5 FROM r WHERE n < 3
            )
            SELECT n FROM r ORDER BY n
            """)) {
      for (String expected : List.of("1.0", "1.5", "2.0", "2.5", "3.0")) {
        assertTrue(result.next());
        assertEquals(new BigDecimal(expected), result.getBigDecimal(1));
      }
      assertFalse(result.next());
    }
  }
}
