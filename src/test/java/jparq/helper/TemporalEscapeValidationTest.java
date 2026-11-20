package jparq.helper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        Arguments.of("SELECT {d '2024-01-01'''} FROM t", "22007"),
        Arguments.of("SELECT {d '2024-02-29 10:00:00'} FROM t", "22007"),
        Arguments.of("SELECT {d '2024-02-29T10:00:00'} FROM t", "22007"),
        Arguments.of("SELECT {d ' 2024-02-29'} FROM t", "22007"), Arguments.of("SELECT {d '24-02-29'} FROM t", "22007"),
        Arguments.of("SELECT {t '10:00'} FROM t", "22007"), Arguments.of("SELECT {t '10:00:00.5'} FROM t", "22007"),
        Arguments.of("SELECT {t '1:00:00'} FROM t", "22007"), Arguments.of("SELECT {ts '2024-02-29'} FROM t", "22007"),
        Arguments.of("SELECT {ts '2024-01-01T10:00:00'} FROM t", "22007"),
        Arguments.of("SELECT {ts '2024-02-29 1:00:00'} FROM t", "22007"),
        Arguments.of("SELECT {oj 'SELECT 1'}, {d '2024-02-30'} FROM t", "22008"));
  }

  /**
   * Rejects escape literals whose text fails the JDBC escape shape or strict
   * conversion, surfacing the classified data SQLState through the exception
   * cause.
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
        "SELECT {ts '2024-02-29 10:20:30'} FROM t", "SELECT {ts '2024-02-29 10:20:30.123'} FROM t",
        "SELECT {d '2024-2-9'} FROM t", "SELECT {D '2024-02-29'} FROM t", "SELECT {d'2024-02-29'} FROM t",
        "SELECT 1 FROM t", "SELECT {fn YEAR(x)} FROM t", "SELECT '{d ''2024-02-30''}' FROM t",
        "SELECT \"a {d '2024-02-30'}\" FROM t", "SELECT `a {d '2024-02-30'}` FROM t",
        "SELECT $$ {d '2024-02-30'} $$ FROM t", "SELECT {oj 'SELECT {d ''2024-02-30''}'} FROM t",
        "SELECT {escape '{d ''2024-02-30''}'} FROM t");
  }

  /**
   * Accepts valid escape literals and ignores escape-shaped text inside quoted
   * regions or in non-temporal braces, including opaque JDBC escapes.
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

  /**
   * Replaces every temporal escape value with the valid placeholder of the same
   * shape.
   */
  @Test
  void placeholderValuesReplaceEveryEscape() {
    assertEquals("SELECT {d '1970-01-01'}, {t '00:00:00'}, {ts '1970-01-01 00:00:00'} FROM t", TemporalEscapeValidation
        .withPlaceholderValues("SELECT {d '2024-02-30'}, {t '25:00:00'}, {ts '2024-01-01 00:00:00'} FROM t"));
    assertEquals("SELECT {d '1970-01-01'} FROM t",
        TemporalEscapeValidation.withPlaceholderValues("SELECT {d '20''24-02-30'} FROM t"));
  }

  /**
   * Returns text without a temporal escape, including quoted escape-shaped text,
   * unchanged.
   */
  @Test
  void placeholderValuesRetainOtherText() {
    String quoted = "SELECT '{d ''bad''}', 1 FROM t";
    assertSame(quoted, TemporalEscapeValidation.withPlaceholderValues(quoted));
    String none = "SELECT 1 FROM t";
    assertSame(none, TemporalEscapeValidation.withPlaceholderValues(none));
  }

  /**
   * Reports whether the statement text carries a temporal escape outside quoted
   * regions.
   */
  @Test
  void detectsTemporalEscapes() {
    assertTrue(TemporalEscapeValidation.hasTemporalEscape("SELECT {d 'bad'} FROM t"));
    assertTrue(TemporalEscapeValidation.hasTemporalEscape("SELECT {TS '2024-01-01 00:00:00'} FROM t"));
    assertFalse(TemporalEscapeValidation.hasTemporalEscape("SELECT '{d ''bad''}' FROM t"));
    assertFalse(TemporalEscapeValidation.hasTemporalEscape("SELECT {oj 'SELECT {d ''bad''}'} FROM t"));
    assertFalse(TemporalEscapeValidation.hasTemporalEscape("SELECT 1 FROM t"));
    assertFalse(TemporalEscapeValidation.hasTemporalEscape(null));
  }
}
