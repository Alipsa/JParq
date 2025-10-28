package jparq.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for CHAR and UNICODE helpers. */
class UnicodeAndCodepointsTest {

  private static JParqSql sql;

  @BeforeAll
  static void setup() {
    sql = StringFunctionTestSupport.createSql();
  }

  @Test
  void testCharAndUnicode() {
    AtomicReference<String> charValue = new AtomicReference<>();
    AtomicInteger unicodeSnowman = new AtomicInteger();
    AtomicInteger unicodeA = new AtomicInteger();

    sql.query("SELECT CONCAT(CHAR(65), CHAR(9731)) AS char_val, UNICODE('☃') AS unicode_snowman, "
        + "UNICODE('A') AS unicode_a FROM mtcars LIMIT 1", rs -> {
          try {
            assertTrue(rs.next(), "Expected a row");
            charValue.set(rs.getString("char_val"));
            unicodeSnowman.set(rs.getInt("unicode_snowman"));
            unicodeA.set(rs.getInt("unicode_a"));
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals("A☃", charValue.get(), "CHAR should assemble code points");
    assertEquals(9731, unicodeSnowman.get(), "UNICODE should return the first code point");
    assertEquals(65, unicodeA.get(), "UNICODE should handle ASCII letters");
  }
}
