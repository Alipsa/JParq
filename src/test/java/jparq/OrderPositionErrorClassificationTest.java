package jparq;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeIntegerFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Classifies invalid positional ORDER BY references with SQLState 42000 without
 * leaking raw parse or arithmetic failures.
 */
class OrderPositionErrorClassificationTest {

  /**
   * Rejects out-of-range and unparsable ordinals with the JDBC syntax
   * classification, naming the invalid position in the message.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void classifiesInvalidOrderPositions(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String literal : List.of("0", "99", "2147483648", "99999999999", "99999999999999999999")) {
        for (String projection : List.of("n", "*")) {
          String sql = "SELECT " + projection + " FROM o ORDER BY " + literal;
          SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared),
              sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("outside the select list: " + literal), failure.getMessage());
          assertFalse(isNumericFailure(failure), failure.getMessage());
        }
      }
    }
  }

  /**
   * Classifies invalid positional references on the deferred execution path
   * regardless of result size: zero rows, a single row, and multiple rows without
   * ties on the earlier keys.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void classifiesDeferredOrderPositionsForDegenerateResults(boolean prepared, @TempDir Path directory)
      throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      String[] queries = {
          "SELECT * FROM o WHERE 1 = 0 ORDER BY 99", "SELECT * FROM o WHERE n = 2 ORDER BY 99",
          "SELECT * FROM o WHERE n > 1 ORDER BY 1, 99", "WITH c AS (SELECT n FROM o) SELECT * FROM c ORDER BY 99",
          "WITH c AS (SELECT n FROM o) SELECT * FROM c WHERE 1 = 0 ORDER BY 99",
          "SELECT * FROM o WHERE n >= 1 ORDER BY 1, 99",
      };
      for (String sql : queries) {
        SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("outside the select list: 99"), failure.getMessage());
        assertFalse(isNumericFailure(failure), failure.getMessage());
      }
    }
  }

  /**
   * Keeps valid ordinal references working for projections of several widths.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void keepsValidOrderPositions(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(List.of(List.of(1), List.of(1), List.of(2), List.of(3), Arrays.asList((Object) null)),
          rows(connection, "SELECT n FROM o ORDER BY 1", prepared));
      assertEquals(
          Arrays.asList(List.of(1, 2L), List.of(1, 2L), List.of(2, 3L), List.of(3, 4L),
              Arrays.asList((Object) null, (Object) null)),
          rows(connection, "SELECT n, n + 1 FROM o ORDER BY 2 NULLS LAST", prepared));
      assertEquals(List.of(List.of(1), List.of(1), List.of(2), List.of(3), Arrays.asList((Object) null)),
          rows(connection, "SELECT * FROM o ORDER BY 1 NULLS LAST", prepared));
      assertEquals(List.of(), rows(connection, "SELECT * FROM o WHERE 1 = 0 ORDER BY 1", prepared));
    }
  }

  /**
   * Keeps ordinal references usable when the output width is only known at
   * execution, rejecting invalid ones with the syntax classification.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void classifiesJoinOrderPositions(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String literal : List.of("0", "99", "99999999999")) {
        String sql = "SELECT o.n FROM o JOIN (VALUES (1)) v(x) ON TRUE ORDER BY " + literal;
        SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("outside the select list: " + literal), failure.getMessage());
      }
    }
  }

  /**
   * Classifies invalid positional references on set operations with SQLState
   * 42000 without silently truncating large literals.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void classifiesSetOperationOrderPositions(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String literal : List.of("0", "99", "99999999999", "99999999999999999999")) {
        String sql = "SELECT n FROM o UNION ALL SELECT n FROM o ORDER BY " + literal;
        SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("outside the select list: " + literal), failure.getMessage());
        assertFalse(isNumericFailure(failure), failure.getMessage());
      }
      // The range check runs even when the combined result is empty.
      for (String literal : List.of("0", "99")) {
        String sql = "SELECT n FROM o WHERE 1 = 0 UNION ALL SELECT n FROM o WHERE 1 = 0 ORDER BY " + literal;
        SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("outside the select list: " + literal), failure.getMessage());
        String derived = "SELECT * FROM (SELECT n FROM o WHERE 1 = 0 UNION ALL SELECT n FROM o WHERE 1 = 0 ORDER BY "
            + literal + ") t";
        failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, derived, prepared), derived);
        assertEquals("42000", failure.getSQLState(), derived);
        assertTrue(failure.getMessage().contains("outside the select list: " + literal), failure.getMessage());
      }
      String derived = "SELECT * FROM (SELECT n FROM o UNION ALL SELECT n FROM o ORDER BY 99) t";
      SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, derived, prepared),
          derived);
      assertEquals("42000", failure.getSQLState(), derived);
      assertTrue(failure.getMessage().contains("outside the select list: 99"), failure.getMessage());
      // Unknown labels fail on an empty result too.
      String unknown = "SELECT n FROM o WHERE 1 = 0 UNION ALL SELECT n FROM o WHERE 1 = 0 ORDER BY nosuch";
      failure = assertThrows(SQLException.class, () -> rows(connection, unknown, prepared), unknown);
      assertTrue(failure.getMessage().contains("Unknown set operation ORDER BY column: nosuch"), failure.getMessage());
      assertEquals(
          Arrays.asList(List.of(1), List.of(1), List.of(1), List.of(1), List.of(2), List.of(2), List.of(3), List.of(3),
              Arrays.asList((Object) null), Arrays.asList((Object) null)),
          rows(connection, "SELECT n FROM o UNION ALL SELECT n FROM o ORDER BY 1", prepared));
    }
  }

  /**
   * Names the invalid position exactly as written in the query, including
   * non-canonical integer literals.
   *
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @Test
  void reportsOrderPositionsAsWritten(@TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      SQLException failure = assertThrows(SQLSyntaxErrorException.class,
          () -> rows(connection, "SELECT n FROM o ORDER BY 099", false));
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains("outside the select list: 099"), failure.getMessage());
      failure = assertThrows(SQLSyntaxErrorException.class,
          () -> rows(connection, "SELECT * FROM o WHERE n > 1 ORDER BY 1, 099", false));
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains("outside the select list: 099"), failure.getMessage());
    }
  }

  /**
   * Reports whether the issue's raw numeric parse or arithmetic leak still
   * appears anywhere in the cause chain.
   *
   * @param failure
   *          the failure to inspect
   * @return true when a NumberFormatException or ArithmeticException is a
   *         cause-chain link
   */
  private static boolean isNumericFailure(Throwable failure) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current instanceof NumberFormatException || current instanceof ArithmeticException) {
        return true;
      }
    }
    return false;
  }
}
