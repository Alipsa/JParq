package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class JParqSqlTest {

  @Test
  public void testQuery() {
    String jdbcUrl = "jdbc:jparq:src/test/resources/datasets";
    JParqSql jparqSql = new JParqSql(jdbcUrl);
    AtomicInteger count = new AtomicInteger(0);
    jparqSql.query("SELECT * FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          count.incrementAndGet();
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    assertEquals(32, count.get());
  }

  @Test
  public void testQueryWithException() {
    String jdbcUrl = "jdbc:jparq:src/test/resources/datasets";
    JParqSql jparqSql = new JParqSql(jdbcUrl);
    assertThrows(RuntimeException.class, () -> {
      jparqSql.query("SELECT * FROM non_existing_table", rs -> {
      });
    });
  }
}
