package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that temporal getters convert VARCHAR values strictly and report
 * classified SQL failures instead of leaking parser exceptions.
 */
class TemporalGetStringConversionTest {

  /**
   * Supplies invalid VARCHAR literals with their accessor and expected state.
   *
   * @return invalid temporal string cases
   */
  static Stream<Arguments> invalidTemporalStrings() {
    return Stream.of(Arguments.of("'bad'", "date", "22007"), Arguments.of("'bad'", "time", "22007"),
        Arguments.of("'bad'", "timestamp", "22007"), Arguments.of("'2024-02-30'", "date", "22008"),
        Arguments.of("'2024-02-30'", "timestamp", "22008"), Arguments.of("'2024-13-01'", "date", "22008"),
        Arguments.of("'2024-13-01'", "timestamp", "22008"), Arguments.of("'25:00:00'", "time", "22008"),
        Arguments.of("'25:00:00'", "timestamp", "22007"), Arguments.of("'2024-01-01 12:60:00'", "timestamp", "22008"));
  }

  /**
   * Rejects VARCHAR values that no supported temporal format accepts.
   *
   * @param value
   *          the invalid VARCHAR literal
   * @param getter
   *          the temporal accessor
   * @param state
   *          expected data SQLState
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @MethodSource("invalidTemporalStrings")
  void gettersRejectInvalidTemporalStrings(String value, String getter, String state) throws Exception {
    String url = "jdbc:jparq:"
        + Path.of(TemporalGetStringConversionTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection(url);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + value + " AS x FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      SQLDataException failure = assertThrows(SQLDataException.class, () -> {
        switch (getter) {
          case "date" -> result.getDate(1);
          case "time" -> result.getTime(1);
          default -> result.getTimestamp(1);
        }
      });
      assertEquals(state, failure.getSQLState());
      assertTrue(failure.getMessage().contains(getter.toUpperCase()), failure::getMessage);
    }
  }

  /**
   * Retains valid VARCHAR temporal values, including unpadded SQL fields, and
   * keeps numeric epoch values on the millisecond path.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void gettersRetainValidTemporalStrings() throws Exception {
    String url = "jdbc:jparq:"
        + Path.of(TemporalGetStringConversionTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection(url);
        var statement = connection.createStatement();
        var result = statement.executeQuery(
            "SELECT '2024-2-29' AS d, '10:20:30' AS t, '2024-02-29T10:20:30' AS ts FROM mtcars " + "LIMIT 1")) {
      assertTrue(result.next());
      assertEquals(Date.valueOf("2024-02-29"), result.getDate(1));
      assertEquals(Time.valueOf("10:20:30"), result.getTime(2));
      assertEquals(Timestamp.valueOf("2024-02-29 10:20:30"), result.getTimestamp(3));
    }
    try (var connection = DriverManager.getConnection(url);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 1700000000000 AS x FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals(new Date(1700000000000L), result.getDate(1));
      assertEquals(new Timestamp(1700000000000L), result.getTimestamp(1));
    }
  }
}
