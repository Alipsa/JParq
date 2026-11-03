package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies lexical binding of unqualified columns in LATERAL derived tables
 * (#286).
 */
class LateralColumnBindingTest {
  /**
   * Reads a physical preceding input when the inner input does not declare the
   * name.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void unqualifiedOuterColumnFiltersInnerRows(boolean pushdown) throws Exception {
    String sql = "SELECT m.cyl, q.y FROM mtcars m CROSS JOIN LATERAL "
        + "(SELECT x AS y FROM (VALUES (5)) v(x) WHERE x > cyl) q";
    try (var connection = DriverManager.getConnection(url(pushdown));
        var statement = connection.prepareStatement(sql);
        var result = statement.executeQuery()) {
      int count = 0;
      while (result.next()) {
        assertEquals(4, result.getInt(1));
        assertEquals(5, result.getInt(2));
        count++;
      }
      assertEquals(11, count);
    }
  }

  /**
   * Gives CTE and derived-table columns precedence over preceding outer columns.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void innerNamesTakePrecedenceAcrossInputKinds(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String input : List.of("c", "(SELECT k, x FROM c) d")) {
        String sql = "WITH c(k, x) AS (VALUES (7, 5)) "
            + "SELECT q.y FROM (VALUES (99, 4), (98, 6)) a(k, cyl) CROSS JOIN LATERAL " + "(SELECT k AS y FROM " + input
            + " WHERE x > cyl) q";
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          assertTrue(result.next(), sql);
          assertEquals(7, result.getInt(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Rejects an outer name supplied by two preceding inputs instead of choosing
   * one.
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
  void ambiguousOuterNamesAreSyntaxErrors(boolean pushdown) throws Exception {
    String sql = "SELECT q.y FROM (VALUES (1)) a(k), (VALUES (2)) b(k) CROSS JOIN LATERAL "
        + "(SELECT k + z.x AS y FROM (VALUES (0)) z(x)) q";
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      var error = assertThrows(SQLSyntaxErrorException.class, () -> {
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          result.next();
        }
      });
      assertEquals("42702", error.getSQLState());
      assertTrue(error.getMessage().contains("Ambiguous column reference: k"), error.getMessage());
    }
  }

  /**
   * Builds a fixture connection with matching pushdown options.
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
