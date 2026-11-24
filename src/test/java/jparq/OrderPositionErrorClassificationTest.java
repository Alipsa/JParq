package jparq;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeEmptyIntegerFixture;
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
import org.junit.jupiter.api.Disabled;
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
    writeEmptyIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      String[] queries = {
          "SELECT * FROM o WHERE 1 = 0 ORDER BY 99", "SELECT * FROM o WHERE n = 2 ORDER BY 99",
          "SELECT * FROM o WHERE n > 1 ORDER BY 1, 99", "WITH c AS (SELECT n FROM o) SELECT * FROM c ORDER BY 99",
          "WITH c AS (SELECT n FROM o) SELECT * FROM c WHERE 1 = 0 ORDER BY 99",
          "SELECT * FROM o WHERE n >= 1 ORDER BY 1, 99",
          // A zero-row file has no record to take the schema from.
          "SELECT * FROM e ORDER BY 99", "SELECT * FROM e WHERE 1 = 0 ORDER BY 99",
      };
      for (String sql : queries) {
        SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("outside the select list: 99"), failure.getMessage());
        assertFalse(isNumericFailure(failure), failure.getMessage());
      }
      // A valid ordinal is accepted on a zero-row file, and the out-of-range
      // successor pins the deferred width to the single fixture column.
      assertEquals(List.of(), rows(connection, "SELECT * FROM e ORDER BY 1", prepared));
      String outOfRange = "SELECT * FROM e ORDER BY 2";
      SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, outOfRange, prepared),
          outOfRange);
      assertEquals("42000", failure.getSQLState(), outOfRange);
      assertTrue(failure.getMessage().contains("outside the select list: 2"), failure.getMessage());
    }
  }

  /**
   * Classifies invalid positional references on {@code SELECT DISTINCT *} with
   * SQLState 42000 for multi-row, single-row, and empty inputs: the ordinal
   * literal must not enter the DISTINCT key list before the position is
   * validated.
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
  void classifiesDistinctWildcardOrderPositions(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    writeEmptyIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      String[] queries = {
          "SELECT DISTINCT * FROM o ORDER BY 99", "SELECT DISTINCT * FROM o WHERE n = 2 ORDER BY 99",
          "SELECT DISTINCT * FROM o WHERE 1 = 0 ORDER BY 99", "SELECT DISTINCT * FROM e ORDER BY 99",
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
   * Keeps positional references on {@code SELECT DISTINCT *} sorting as well as
   * deduplicating (issue #610: the ordinal used to be treated as a DISTINCT key
   * column and failed with an unclassified error).
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
  void sortsDistinctWildcardByPosition(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(List.of(List.of(1), List.of(2), List.of(3), Arrays.asList((Object) null)),
          rows(connection, "SELECT DISTINCT * FROM o ORDER BY 1", prepared));
      assertEquals(List.of(List.of(3), List.of(2), List.of(1), Arrays.asList((Object) null)),
          rows(connection, "SELECT DISTINCT * FROM o ORDER BY 1 DESC NULLS LAST", prepared));
    }
  }

  /**
   * Keeps ORDER BY references out of the {@code SELECT DISTINCT *} key list in
   * every textual form (#611): a case-variant, quoted, or derived-alias reference
   * must not be treated as a DISTINCT key column, and each sorts as written.
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
  void keepsOrderNamesOutOfDistinctWildcardKeys(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      List<List<Object>> expected = List.of(Arrays.asList((Object) null), List.of(3), List.of(2), List.of(1));
      assertEquals(expected, rows(connection, "SELECT DISTINCT * FROM o ORDER BY N DESC", prepared));
      assertEquals(expected, rows(connection, "SELECT DISTINCT * FROM o ORDER BY \"n\" DESC", prepared));
      assertEquals(expected,
          rows(connection, "SELECT DISTINCT * FROM (SELECT n AS \"x2\" FROM o) d ORDER BY \"x2\" DESC", prepared));
    }
  }

  /**
   * Until #609 is fixed, the top-level {@code SELECT *, expr} projection drops
   * the expression column, so the runtime sort width is the input width and the
   * valid output position is rejected as out of range instead of sorting. The
   * window-function variant fails the same way, so both shapes are pinned here
   * for the #609 fix. Several rows with a descending position keep the check
   * honest once the drop is fixed: the rows must come back sorted by column 2,
   * not merely accepted.
   *
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @Disabled("#609: SELECT *, expr drops the extra column; the runtime bound must be the projection width")
  @Test
  void sortsWildcardPlusExpressionByOutputPosition(@TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(
          List.of(List.of(3, 4L), List.of(2, 3L), List.of(1, 2L), List.of(1, 2L),
              Arrays.asList((Object) null, (Object) null)),
          rows(connection, "SELECT *, n + 1 AS m FROM o ORDER BY 2 DESC NULLS LAST", false));
      assertEquals(List.of(List.of(1, 4L), List.of(1, 3L), List.of(2, 2L), List.of(3, 1L)),
          rows(connection,
              "SELECT *, ROW_NUMBER() OVER (ORDER BY n DESC) AS rn FROM o WHERE n IS NOT NULL " + "ORDER BY 2 DESC",
              false));
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
   * Validates positional references on a {@code USING} join against the visible
   * select list, not the record: the peer copy of the join column that the join
   * keeps in the record but hides from {@code SELECT *} takes no ordinal
   * position.
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
  void classifiesUsingJoinOrderPositions(boolean prepared, @TempDir Path directory) throws Exception {
    String from = " FROM (VALUES (1, 30), (2, 10), (3, 20)) a(id, v) "
        + "JOIN (VALUES (1, 5), (2, 6), (3, 4)) b(id, v) USING (id)";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(List.of(List.of(2, 10, 6), List.of(3, 20, 4), List.of(1, 30, 5)),
          rows(connection, "SELECT *" + from + " ORDER BY 2", prepared));
      assertEquals(List.of(List.of(3, 20, 4), List.of(1, 30, 5), List.of(2, 10, 6)),
          rows(connection, "SELECT *" + from + " ORDER BY 3", prepared));
      for (String sql : List.of("SELECT *" + from + " ORDER BY 4", "SELECT *" + from + " WHERE 1 = 0 ORDER BY 4")) {
        SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("outside the select list: 4"), failure.getMessage());
        assertFalse(isNumericFailure(failure), failure.getMessage());
      }
    }
  }

  /**
   * Keeps unknown ORDER BY references failing with SQLState 42000 instead of
   * degrading to a no-op sort that compares every key equal: an unresolvable name
   * fails for non-empty and empty inputs alike, across base, distinct, join, and
   * derived shapes, naming the reference exactly as written.
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
  void classifiesUnknownOrderColumns(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      String[] queries = {
          "SELECT * FROM o ORDER BY nosuch", "SELECT * FROM o ORDER BY \"nosuch\"", "SELECT * FROM o ORDER BY \"N\"",
          "SELECT * FROM o WHERE 1 = 0 ORDER BY nosuch", "SELECT DISTINCT * FROM o ORDER BY \"nosuch\"",
          "SELECT * FROM o JOIN (VALUES (1)) v(x) ON TRUE ORDER BY nosuch",
          "SELECT * FROM o JOIN (VALUES (1)) v(x) ON TRUE WHERE 1 = 0 ORDER BY nosuch",
          "SELECT * FROM (SELECT n FROM o) d ORDER BY \"nosuch\"",
      };
      String[] references = {
          "nosuch", "\"nosuch\"", "\"N\"", "nosuch", "\"nosuch\"", "nosuch", "nosuch", "\"nosuch\"",
      };
      for (int query = 0; query < queries.length; query++) {
        String sql = queries[query];
        SQLException failure = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql, prepared), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains(references[query]), failure.getMessage());
        assertFalse(isNumericFailure(failure), failure.getMessage());
      }
    }
  }

  /**
   * Classifies invalid positional references on set operations with SQLState
   * 42000 without silently truncating large literals, naming each literal exactly
   * as written.
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
      // 099 pins the literal-as-written diagnostic: the parsed value is 99.
      for (String literal : List.of("0", "99", "099", "99999999999", "99999999999999999999")) {
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
      // Unknown labels fail on an empty result too. #385: the failure is still
      // an unclassified SQLException; when that issue is fixed, update this
      // assertion to SQLSyntaxErrorException with SQLState 42000.
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
