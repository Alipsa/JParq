package jparq.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for character length and position functions. */
class CharacterLengthAndPositionTest {

  private static JParqSql sql;

  @BeforeAll
  static void setup() {
    sql = StringFunctionTestSupport.createSql();
  }

  @Test
  void testLengthsAndPosition() {
    AtomicReference<Integer> charLength = new AtomicReference<>();
    AtomicReference<Integer> charLength2 = new AtomicReference<>();
    AtomicReference<Integer> octetLength = new AtomicReference<>();
    AtomicReference<Integer> position = new AtomicReference<>();

    sql.query(
        "SELECT CHAR_LENGTH('héllo') AS char_len, CHARACTER_LENGTH('héllo') AS char_len2, "
            + "OCTET_LENGTH('Å') AS oct_len, POSITION('c' IN 'abca') AS pos FROM mtcars LIMIT 1",
        rs -> {
          try {
            assertTrue(rs.next(), "Expected a result row");
            charLength.set(rs.getInt("char_len"));
            charLength2.set(rs.getInt("char_len2"));
            octetLength.set(rs.getInt("oct_len"));
            position.set(rs.getInt("pos"));
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals(5, charLength.get().intValue(), "CHAR_LENGTH should count Unicode characters");
    assertEquals(charLength.get(), charLength2.get(), "CHARACTER_LENGTH should equal CHAR_LENGTH");
    assertEquals(2, octetLength.get().intValue(), "UTF-8 Å should be two bytes");
    assertEquals(3, position.get().intValue(), "POSITION should be 1-based and find first match");
  }
}
