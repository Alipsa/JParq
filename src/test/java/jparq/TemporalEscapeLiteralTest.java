package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
   * Supplies escape literals with invalid fields and their expected state.
   *
   * @return invalid escape literal cases
   */
  static Stream<Arguments> invalidEscapeLiterals() {
    return Stream.of(Arguments.of("SELECT {d '2024-02-30'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {d '2024-13-01'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {t '25:00:00'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {t '12:60:00'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {ts '2024-02-30 10:00:00'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {ts '2024-01-01 12:60:00'} FROM mtcars LIMIT 1", "22008"),
        Arguments.of("SELECT {d 'bad'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {t 'bad'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT {ts 'bad'} FROM mtcars LIMIT 1", "22007"),
        Arguments.of("SELECT [{d '2024-02-30'}] FROM mtcars LIMIT 1", "22008"));
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
   * and tags without surrounding whitespace.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void escapesRetainValidFields() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT {d '2024-02-29'} AS d, {t '10:20:30'} AS t, "
            + "{ts '2024-02-29 10:20:30'} AS ts FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals(Date.valueOf("2024-02-29"), result.getDate(1));
      assertEquals(Time.valueOf("10:20:30"), result.getTime(2));
      assertEquals(Timestamp.valueOf("2024-02-29 10:20:30"), result.getTimestamp(3));
    }
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery(
            "SELECT {d '2024-2-9'} AS a, {D '2024-02-29'} AS b, {d'2024-02-29'} AS c FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals(Date.valueOf("2024-02-09"), result.getDate(1));
      assertEquals(Date.valueOf("2024-02-29"), result.getDate(2));
      assertEquals(Date.valueOf("2024-02-29"), result.getDate(3));
    }
  }

  /**
   * Ignores escape-shaped text inside quoted regions and comments so valid SQL is
   * not rejected.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void escapesIgnoreQuotedRegionsAndComments() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT '{d ''2024-02-30''}' AS x FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals("{d '2024-02-30'}", result.getString(1));
    }
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 1 AS x -- {d '2024-02-30'}\nFROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals(1, result.getInt(1));
    }
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 1 AS x /* {d '2024-02-30'} */ FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals(1, result.getInt(1));
    }
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 1 AS x // {d '2024-02-30'}\nFROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals(1, result.getInt(1));
    }
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT $$ {d '2024-02-30'} $$ AS x FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals(" {d '2024-02-30'} ", result.getString(1));
    }
    SQLException backtick = assertThrows(SQLException.class,
        () -> tryQuery("SELECT `a {d '2024-02-30'}` FROM mtcars LIMIT 1"));
    assertFalse(isDataFailure(backtick),
        () -> "unexpected data failure for backtick identifier: " + backtick.getMessage());
  }

  /**
   * Rejects invalid escape literals in nested scopes and predicates.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void escapesRejectInvalidFieldsInNestedScopes() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      SQLDataException scalar = assertThrows(SQLDataException.class,
          () -> statement.executeQuery("SELECT (SELECT {d '2024-02-30'}) AS x FROM mtcars LIMIT 1"));
      assertEquals("22008", scalar.getSQLState());
      SQLDataException predicate = assertThrows(SQLDataException.class,
          () -> statement.executeQuery("SELECT * FROM mtcars WHERE mpg > {d 'bad'}"));
      assertEquals("22007", predicate.getSQLState());
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
   * Reports the data SQLStates that identify temporal conversion failures.
   *
   * @param failure
   *          the failure to classify
   * @return whether the failure carries a temporal data SQLState
   */
  private static boolean isDataFailure(SQLException failure) {
    String state = failure.getSQLState();
    return "22007".equals(state) || "22008".equals(state);
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
    } catch (SQLException failure) {
      throw failure;
    } catch (Exception failure) {
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
