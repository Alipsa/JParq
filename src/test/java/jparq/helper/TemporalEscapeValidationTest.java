package jparq.helper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.helper.TemporalEscapeValidation;

/**
 * Unit tests for {@link TemporalEscapeValidation} that exercise the raw-text
 * scanner directly.
 */
class TemporalEscapeValidationTest {

  /**
   * Supplies escape literals with invalid fields and their expected state.
   *
   * @return invalid escape literal cases
   */
  static Stream<Arguments> invalidEscapes() {
    return Stream.of(Arguments.of("SELECT {d '2024-02-30'} FROM t", "22008"),
        Arguments.of("SELECT {d '2024-13-01'} FROM t", "22008"), Arguments.of("SELECT {t '25:00:00'} FROM t", "22008"),
        Arguments.of("SELECT {t '12:60:00'} FROM t", "22008"), Arguments.of("SELECT {t '23:59:60'} FROM t", "22008"),
        Arguments.of("SELECT {ts '2024-02-30 10:00:00'} FROM t", "22008"),
        Arguments.of("SELECT {ts '2024-01-01 12:60:00'} FROM t", "22008"),
        Arguments.of("SELECT {d 'bad'} FROM t", "22007"), Arguments.of("SELECT {t 'bad'} FROM t", "22007"),
        Arguments.of("SELECT {ts 'bad'} FROM t", "22007"), Arguments.of("SELECT [{d '2024-02-30'}] FROM t", "22008"),
        Arguments.of("SELECT {d '2024-01-01'''} FROM t", "22007"));
  }

  /**
   * Rejects escape literals whose fields fail strict conversion, surfacing the
   * classified data SQLState through the exception cause.
   *
   * @param sql
   *          statement text containing an invalid escape literal
   * @param state
   *          expected data SQLState
   */
  @ParameterizedTest
  @MethodSource("invalidEscapes")
  void rejectsInvalidEscapes(String sql, String state) {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> TemporalEscapeValidation.validate(sql));
    SQLException classified = SqlExceptions.findSqlStateFailure(failure);
    assertNotNull(classified);
    assertEquals(state, classified.getSQLState());
  }

  /**
   * Supplies statement text whose temporal or escape-shaped content must be
   * accepted: valid literals, non-temporal braces, and quoted regions.
   *
   * @return valid and ignored statement text cases
   */
  static Stream<String> validOrIgnored() {
    return Stream.of("SELECT {d '2024-02-29'} FROM t", "SELECT {t '10:20:30'} FROM t",
        "SELECT {ts '2024-02-29 10:20:30'} FROM t", "SELECT {d '2024-2-9'} FROM t", "SELECT {D '2024-02-29'} FROM t",
        "SELECT {d'2024-02-29'} FROM t", "SELECT 1 FROM t", "SELECT {fn YEAR(x)} FROM t",
        "SELECT '{d ''2024-02-30''}' FROM t", "SELECT \"a {d '2024-02-30'}\" FROM t",
        "SELECT `a {d '2024-02-30'}` FROM t", "SELECT $$ {d '2024-02-30'} $$ FROM t");
  }

  /**
   * Accepts valid escape literals and ignores escape-shaped text inside quoted
   * regions or in non-temporal braces.
   *
   * @param sql
   *          statement text to validate
   */
  @ParameterizedTest
  @MethodSource("validOrIgnored")
  void retainsValidAndIgnoresQuoted(String sql) {
    assertDoesNotThrow(() -> TemporalEscapeValidation.validate(sql));
  }

  /**
   * Supplies escape literals with invalid fields separated from the tag or
   * closing brace by the whitespace the parser accepts.
   *
   * @return whitespace variant cases
   */
  static Stream<String> whitespaceVariants() {
    return Stream.of("SELECT {d   '2024-02-30'   } FROM t", "SELECT {d\t'2024-02-30'} FROM t",
        "SELECT {d '2024-02-30'\t} FROM t");
  }

  /**
   * Recognizes escape literals across the whitespace the parser accepts between
   * the tag, the literal, and the closing brace.
   *
   * @param sql
   *          statement text with varied spacing around an invalid escape
   */
  @ParameterizedTest
  @MethodSource("whitespaceVariants")
  void recognizesWhitespaceVariants(String sql) {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> TemporalEscapeValidation.validate(sql));
    assertEquals("22008", SqlExceptions.findSqlStateFailure(failure).getSQLState());
  }

  /** Accepts null and brace-free input without scanning. */
  @Test
  void acceptsNullAndBraceFreeInput() {
    assertDoesNotThrow(() -> TemporalEscapeValidation.validate(null));
    assertDoesNotThrow(() -> TemporalEscapeValidation.validate(""));
    assertDoesNotThrow(() -> TemporalEscapeValidation.validate("SELECT 1 FROM t"));
  }
}
