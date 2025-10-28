package jparq.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for UPPER and LOWER. */
class CaseConversionTest {

  private static JParqSql sql;

  @BeforeAll
  static void setup() {
    sql = StringFunctionTestSupport.createSql();
  }

  @Test
  void testUpperLower() {
    AtomicReference<String> upper = new AtomicReference<>();
    AtomicReference<String> lower = new AtomicReference<>();

    sql.query("SELECT UPPER('sql') AS upper_val, LOWER('SQL') AS lower_val FROM mtcars LIMIT 1", rs -> {
      try {
        assertTrue(rs.next(), "Expected a row");
        upper.set(rs.getString("upper_val"));
        lower.set(rs.getString("lower_val"));
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals("SQL", upper.get(), "UPPER should convert to uppercase");
    assertEquals("sql", lower.get(), "LOWER should convert to lowercase");
  }
}
