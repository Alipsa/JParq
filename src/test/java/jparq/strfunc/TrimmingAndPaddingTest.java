package jparq.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for trimming and padding helpers. */
class TrimmingAndPaddingTest {

  private static JParqSql sql;

  @BeforeAll
  static void setup() {
    sql = StringFunctionTestSupport.createSql();
  }

  @Test
  void testTrimAndPad() {
    AtomicReference<String> trimDefault = new AtomicReference<>();
    AtomicReference<String> trimCustom = new AtomicReference<>();
    AtomicReference<String> ltrim = new AtomicReference<>();
    AtomicReference<String> rtrim = new AtomicReference<>();
    AtomicReference<String> lpad = new AtomicReference<>();
    AtomicReference<String> rpad = new AtomicReference<>();

    sql.query("SELECT TRIM('  hi  ') AS trim_default, TRIM(BOTH 'x' FROM 'xabcx') AS trim_custom, "
        + "LTRIM('  hi') AS ltrim_val, RTRIM('hi  ') AS rtrim_val, LPAD('42', 5, '0') AS lpad_val, "
        + "RPAD('42', 5, '0') AS rpad_val FROM mtcars LIMIT 1", rs -> {
          try {
            assertTrue(rs.next(), "Expected a row");
            trimDefault.set(rs.getString("trim_default"));
            trimCustom.set(rs.getString("trim_custom"));
            ltrim.set(rs.getString("ltrim_val"));
            rtrim.set(rs.getString("rtrim_val"));
            lpad.set(rs.getString("lpad_val"));
            rpad.set(rs.getString("rpad_val"));
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals("hi", trimDefault.get(), "TRIM without specification should remove spaces");
    assertEquals("abc", trimCustom.get(), "TRIM with custom characters should work");
    assertEquals("hi", ltrim.get(), "LTRIM should remove leading spaces");
    assertEquals("hi", rtrim.get(), "RTRIM should remove trailing spaces");
    assertEquals("00042", lpad.get(), "LPAD should pad on the left");
    assertEquals("42000", rpad.get(), "RPAD should pad on the right");
  }
}
