package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies incompatible ARRAY elements report SQL errors at JDBC boundaries.
 */
class MixedArrayElementsTest {
  private static String jdbcUrl;

  /**
   * Locates the mtcars fixture.
   *
   * @throws Exception
   *           if fixture lookup fails
   */
  @BeforeAll
  static void locateFixture() throws Exception {
    jdbcUrl = "jdbc:jparq:" + Path.of(MixedArrayElementsTest.class.getResource("/mtcars.parquet").toURI()).getParent();
  }

  /**
   * Rejects mixed arrays in projections, materializations, WHERE, and HAVING.
   *
   * @param array
   *          incompatible ARRAY expression
   * @throws Exception
   *           if a connection cannot be opened
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "ARRAY[4,[6]]", "ARRAY[[6],4]", "ARRAY[NULL,4,[6],NULL]", "ARRAY[4,ARRAY[6]]", "ARRAY[4,'6']", "ARRAY['4',6]",
      "ARRAY[4,TRUE]", "ARRAY[[4,[6]]]", "ARRAY[[4],['6']]", "ARRAY[[4],[[6]]]", "ARRAY[[NULL,4],NULL,['6',NULL]]",
      "ARRAY[[[4]],[[TRUE]]]"
  })
  void rejectsMixedElements(String array) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      try (var connection = DriverManager.getConnection(jdbcUrl + "?predicatePushdown=" + pushdown)) {
        for (String sql : List.of("SELECT " + array + " AS a", "SELECT a FROM (SELECT " + array + " AS a) d",
            "WITH c AS (SELECT " + array + " AS a) SELECT a FROM c",
            "SELECT model FROM mtcars WHERE 4 = ANY(" + array + ")",
            "SELECT model FROM mtcars WHERE 4 = SOME(" + array + ")",
            "SELECT model FROM mtcars WHERE 4 = ALL(" + array + ")",
            "SELECT COUNT(*) FROM mtcars HAVING COUNT(*) > ANY(" + array + ")")) {
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> readAll(connection, sql), sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("ARRAY elements must share a common type"), failure.getMessage());
          for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            assertFalse(cause instanceof ClassCastException, sql);
          }
        }
      }
    }
  }

  /**
   * Rejects incompatible elements supplied through JDBC parameters.
   *
   * @param sql
   *          parameterized projection or predicate
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT ARRAY[?,[?]]", "SELECT model FROM mtcars WHERE 4 = ANY(ARRAY[?,[?]])"
  })
  void rejectsParameterizedMixedElements(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl); var statement = connection.prepareStatement(sql)) {
      statement.setInt(1, 4);
      statement.setInt(2, 6);
      var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
        try (var result = statement.executeQuery()) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      });
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains("ARRAY elements must share a common type"), failure.getMessage());
    }
  }

  /**
   * Preserves valid numeric and nested arrays and skips unevaluated CASE arms.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void preservesValidArraysAndSkippedExpressions() throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT ARRAY[NULL,4,6.5,NULL] AS a, ARRAY[[4],[6]] AS b,
                   CASE WHEN TRUE THEN 7 ELSE ARRAY[4,[6]] END AS c, ARRAY[[1],[2.5]] AS d,
                   ARRAY[[[1]],[[2.5]]] AS e
            """)) {
      assertTrue(result.next());
      assertEquals(Arrays.asList(null, new BigDecimal("4"), new BigDecimal("6.5"), null), result.getObject(1));
      assertEquals(List.of(List.of(4L), List.of(6L)), result.getObject(2));
      assertEquals(7L, result.getObject(3));
      assertEquals(List.of(List.of(new BigDecimal("1")), List.of(new BigDecimal("2.5"))), result.getObject(4));
      assertEquals(List.of(List.of(List.of(new BigDecimal("1"))), List.of(List.of(new BigDecimal("2.5")))),
          result.getObject(5));
      assertFalse(result.next());
    }
  }

  /**
   * Forces execution, iteration, and value access so deferred errors are
   * observed.
   *
   * @param connection
   *          JDBC connection
   * @param sql
   *          query to read
   * @throws SQLException
   *           if execution or value access fails
   */
  private static void readAll(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
          result.getObject(column);
        }
      }
    }
  }
}
