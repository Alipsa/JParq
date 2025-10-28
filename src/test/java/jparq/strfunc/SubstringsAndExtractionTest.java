package jparq.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for substring-related helpers. */
class SubstringsAndExtractionTest {

  private static JParqSql sql;

  @BeforeAll
  static void setup() {
    sql = StringFunctionTestSupport.createSql();
  }

  @Test
  void testSubstringLeftRight() {
    AtomicReference<String> substringNamed = new AtomicReference<>();
    AtomicReference<String> substringPositional = new AtomicReference<>();
    AtomicReference<String> leftVal = new AtomicReference<>();
    AtomicReference<String> rightVal = new AtomicReference<>();

    sql.query(
        "SELECT SUBSTRING('abcdef' FROM 2 FOR 3) AS sub_named, "
            + "SUBSTRING('abcdef', 3, 2) AS sub_pos, LEFT('abcdef', 3) AS left_val, "
            + "RIGHT('abcdef', 2) AS right_val FROM mtcars LIMIT 1",
        rs -> {
          try {
            assertTrue(rs.next(), "Expected a row");
            substringNamed.set(rs.getString("sub_named"));
            substringPositional.set(rs.getString("sub_pos"));
            leftVal.set(rs.getString("left_val"));
            rightVal.set(rs.getString("right_val"));
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals("bcd", substringNamed.get(), "Named SUBSTRING should honour FROM/FOR");
    assertEquals("cd", substringPositional.get(), "Positional SUBSTRING should work");
    assertEquals("abc", leftVal.get(), "LEFT should extract leading characters");
    assertEquals("ef", rightVal.get(), "RIGHT should extract trailing characters");
  }
}
