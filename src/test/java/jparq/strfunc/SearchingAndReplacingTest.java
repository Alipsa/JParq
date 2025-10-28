package jparq.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for OVERLAY and REPLACE. */
class SearchingAndReplacingTest {

  private static JParqSql sql;

  @BeforeAll
  static void setup() {
    sql = StringFunctionTestSupport.createSql();
  }

  @Test
  void testOverlayAndReplace() {
    AtomicReference<String> overlay = new AtomicReference<>();
    AtomicReference<String> replace = new AtomicReference<>();

    sql.query("SELECT OVERLAY('abcdef' PLACING 'xyz' FROM 3 FOR 2) AS overlay_val, "
        + "REPLACE('banana', 'na', 'xy') AS replace_val FROM mtcars LIMIT 1", rs -> {
          try {
            assertTrue(rs.next(), "Expected a row");
            overlay.set(rs.getString("overlay_val"));
            replace.set(rs.getString("replace_val"));
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals("abxyzef", overlay.get(), "OVERLAY should splice replacement text");
    assertEquals("baxyxy", replace.get(), "REPLACE should replace all occurrences");
  }
}
