package jparq.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for CONCAT and related behaviour. */
class ConcatenationTest {

  private static JParqSql sql;

  @BeforeAll
  static void setup() {
    sql = StringFunctionTestSupport.createSql();
  }

  @Test
  void testConcat() {
    AtomicReference<String> concatVal = new AtomicReference<>();
    AtomicReference<String> concatNull = new AtomicReference<>();

    sql.query(
        "SELECT CONCAT('a','b',NULL,'c') AS concat_val, CONCAT(NULL,NULL) AS concat_null " + "FROM mtcars LIMIT 1",
        rs -> {
          try {
            assertTrue(rs.next(), "Expected a row");
            concatVal.set(rs.getString("concat_val"));
            concatNull.set(rs.getString("concat_null"));
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals("abc", concatVal.get(), "NULL arguments should be skipped when concatenating");
    assertNull(concatNull.get(), "All-null CONCAT should return NULL");
  }
}
