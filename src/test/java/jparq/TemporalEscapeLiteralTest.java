package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that JDBC temporal escape literals {d}, {t}, and {ts} reject invalid
 * calendar and clock fields like typed literals instead of normalizing them.
 */
class TemporalEscapeLiteralTest {

  /**
   * Supplies escape literals whose text fails the JDBC escape shape or strict
   * conversion, and their expected state.
   *
   * @return invalid escape literal cases
   */
  static Stream<Arguments> invalidEscapeLiterals() {
    return Stream.of(Arguments.of("SELECT {d '2024-02-30'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {d '2024-13-01'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {t '25:00:00'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {t '12:60:00'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {t '23:59:60'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {ts '2024-02-30 10:00:00'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {ts '2024-01-01 12:60:00'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {d 'bad'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {t 'bad'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {ts 'bad'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {d '2024-02-29 10:00:00'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {d '2024-02-29T10:00:00'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {d ' 2024-02-29'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {t '10:00'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {t '10:00:00.5'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {ts '2024-02-29'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {ts '2024-01-01T10:00:00'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT [{d '2024-02-30'}] FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT CASE WHEN mpg=1 THEN mpg=2 ELSE mpg=3 END, {d '2024-13-01'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {d '2024-13-01'}, CASE WHEN mpg=1 THEN mpg=2 ELSE mpg=3 END FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT CASE WHEN mpg=1 THEN mpg=2 ELSE mpg=3 END, {d '2024-02-29 10:00:00'} FROM mtcars LIMIT 1",
            "22007"),
        Arguments.of("SELECT CASE WHEN mpg=1 THEN mpg=2 ELSE mpg=3 END, {t '10:00:00.5'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT CASE WHEN mpg=1 THEN mpg=2 ELSE mpg=3 END, {d '2024-02-30'} FROM mtcars LIMIT 1",
            "22008"));
  }

  /**
   * Rejects escape literals whose fields fail strict temporal conversion.
   *
   * @param sql
   *          query with an invalid escape literal
   * @param state
   *          expected data SQLState
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @MethodSource("invalidEscapeLiterals")
  void escapesRejectInvalidFields(String sql, String state) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      SQLDataException failure = assertThrows(SQLDataException.class, () -> statement.executeQuery(sql));
      assertEquals(state, failure.getSQLState());
    }
  }

  /**
   * Retains valid escape literals, including unpadded SQL fields, uppercase tags,
   * tags without surrounding whitespace, and valid escapes nested in function
   * escapes.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void escapesRetainValidFields() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT {d '2024-02-29'} AS d, {t '10:20:30'} AS t, "
          + "{ts '2024-02-29 10:20:30'} AS ts FROM mtcars LIMIT 1")) {
        assertTrue(result.next());
        assertEquals(Date.valueOf("2024-02-29"), result.getDate(1));
        assertEquals(Time.valueOf("10:20:30"), result.getTime(2));
        assertEquals(Timestamp.valueOf("2024-02-29 10:20:30"), result.getTimestamp(3));
      }
      try (var result = statement.executeQuery(
          "SELECT {d '2024-2-9'} AS a, {D '2024-02-29'} AS b, {d'2024-02-29'} AS c FROM mtcars LIMIT 1")) {
        assertTrue(result.next());
        assertEquals(Date.valueOf("2024-02-09"), result.getDate(1));
        assertEquals(Date.valueOf("2024-02-29"), result.getDate(2));
        assertEquals(Date.valueOf("2024-02-29"), result.getDate(3));
      }
      try (var result = statement.executeQuery("SELECT {fn YEAR({d '2024-02-29'})} AS y FROM mtcars LIMIT 1")) {
        assertTrue(result.next());
        assertEquals(2024, result.getInt(1));
      }
    }
  }

  /**
   * Supplies statements whose escape-shaped text sits in a quoted region or a
   * comment, along with the value the statement returns. The {@code //} case pins
   * current behavior only: {@code //} is not a standard SQL comment and
   * JSqlParser's non-standard support for it is tracked in issue #580, so this
   * case must be dropped when #580 is fixed.
   *
   * @return quoted-region and comment cases
   */
  static Stream<Arguments> ignoredRegions() {
    return Stream.of(Arguments.of("SELECT '{d ''2024-02-30''}' AS x FROM mtcars LIMIT 1", "{d '2024-02-30'}"),
        Arguments.of("SELECT 1 AS x -- {d '2024-02-30'}\nFROM mtcars LIMIT 1", "1"),
        Arguments.of("SELECT 1 AS x /* {d '2024-02-30'} */ FROM mtcars LIMIT 1", "1"),
        Arguments.of("SELECT 1 AS x // {d '2024-02-30'}\nFROM mtcars LIMIT 1", "1"),
        Arguments.of("SELECT $$ {d '2024-02-30'} $$ AS x FROM mtcars LIMIT 1", " {d '2024-02-30'} "));
  }

  /**
   * Ignores escape-shaped text inside quoted regions and comments so valid SQL is
   * not rejected.
   *
   * @param sql
   *          query whose escape-shaped text is quoted or commented out
   * @param expected
   *          value returned by the first column
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @MethodSource("ignoredRegions")
  void escapesIgnoreQuotedRegionsAndComments(String sql, String expected) throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertTrue(result.next());
      assertEquals(expected, result.getString(1));
    }
  }

  /**
   * Treats escape-shaped text inside a backtick identifier as a column name
   * rather than an escape literal, so the query fails on the missing column
   * instead of with a temporal data error.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void backtickIdentifierIsNotAnEscape() throws Exception {
    SQLException backtick = assertThrows(SQLException.class,
        () -> tryQuery("SELECT `a {d '2024-02-30'}` FROM mtcars LIMIT 1"));
    assertEquals("42000", backtick.getSQLState(),
        () -> "expected a column-resolution failure for the backtick identifier: " + backtick.getMessage());
    assertTrue(backtick.getMessage().contains("a {d '2024-02-30'}"),
        () -> "backtick content should be treated as a column, not an escape: " + backtick.getMessage());
  }

  /**
   * Supplies invalid escape literals in nested scopes, predicates, LATERAL
   * inputs, function escapes, CTEs, and derived tables with their expected state.
   *
   * @return nested-scope escape cases
   */
  static Stream<Arguments> nestedScopeEscapes() {
    return Stream.of(Arguments.of("SELECT (SELECT {d '2024-02-30'}) AS x FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT * FROM mtcars WHERE mpg > {d 'bad'}", "22007"),
        Arguments.of("SELECT * FROM mtcars CROSS JOIN LATERAL (SELECT {t '25:00:00'} AS x) LIMIT 1", "22008"),
        Arguments.of("SELECT {fn YEAR({d '2024-02-30'})} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("WITH c AS (SELECT {d '2024-02-30'} AS x) SELECT x FROM c LIMIT 1", "22008"),
        Arguments.of("SELECT x FROM (SELECT {d '2024-02-30'} AS x) AS sub LIMIT 1", "22008"));
  }

  /**
   * Rejects invalid escape literals in nested scopes, predicates, LATERAL inputs,
   * function escapes, CTEs, and derived tables.
   *
   * @param sql
   *          query with an invalid escape literal in a nested scope
   * @param state
   *          expected data SQLState
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @MethodSource("nestedScopeEscapes")
  void escapesRejectInvalidFieldsInNestedScopes(String sql, String state) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      SQLDataException failure = assertThrows(SQLDataException.class, () -> statement.executeQuery(sql));
      assertEquals(state, failure.getSQLState());
    }
  }

  /**
   * Rejects invalid escape literals when preparing a statement.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void preparedStatementsRejectInvalidEscapes() throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      SQLDataException failure = assertThrows(SQLDataException.class,
          () -> connection.prepareStatement("SELECT {d '2024-02-30'} FROM mtcars LIMIT 1"));
      assertEquals("22008", failure.getSQLState());
    }
  }

  /**
   * Supplies statements that are both syntactically invalid and carry an escape
   * whose text the lenient parser rejects, along with a case where the lenient
   * parser rolls the value over instead and a case that only parses in complex
   * mode.
   *
   * @return syntax-plus-invalid-escape cases
   */
  static Stream<String> syntaxErrorWithInvalidEscape() {
    return Stream.of("SELECT {d '2024-13-01'} FROM FROM mtcars", "SELECT {d '2024-02-30'} FROM FROM mtcars",
        "SELECT {d 'bad'} FROM FROM mtcars",
        "SELECT CASE WHEN mpg=1 THEN mpg=2 ELSE mpg=3 END, {d '2024-13-01'} FROM FROM mtcars");
  }

  /**
   * Reports the syntax error before the temporal data error when a statement is
   * both syntactically invalid and carries an invalid escape, regardless of
   * whether the lenient parser rejects or rolls the value over and whether the
   * statement only parses in complex mode.
   *
   * @param sql
   *          query with a syntax error and an invalid escape literal
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @MethodSource("syntaxErrorWithInvalidEscape")
  void syntaxErrorPrecedesInvalidEscape(String sql) throws Exception {
    SQLException failure = assertThrows(SQLException.class, () -> tryQuery(sql));
    assertEquals("42000", failure.getSQLState(), () -> "expected a syntax error: " + failure.getMessage());
  }

  /**
   * Runs a query and rethrows its failure for assertThrows.
   *
   * @param sql
   *          query text
   * @throws SQLException
   *           if the query fails
   */
  private static void tryQuery(String sql) throws SQLException {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      result.next();
    } catch (Exception failure) {
      if (failure instanceof SQLException sqlFailure) {
        throw sqlFailure;
      }
      throw new IllegalStateException(failure);
    }
  }

  /**
   * Resolves the JDBC URL for the mtcars fixture.
   *
   * @return the fixture JDBC URL
   */
  private static String url() {
    try {
      return "jdbc:jparq:"
          + Path.of(TemporalEscapeLiteralTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    } catch (URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }
}
