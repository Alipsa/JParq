package jparq.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Integration tests for JDBC canonical string functions executed via SQL. */
class JdbcCanonicalStringFunctionsTest {

  private final JParqSql sql = StringFunctionTestSupport.createSql();

  @Test
  void asciiReturnsCodePoint() {
    AtomicReference<Object> result = new AtomicReference<>();
    sql.query("SELECT ASCII('A') AS val FROM mtcars LIMIT 1", rs -> {
      try {
        rs.next();
        result.set(rs.getObject("val"));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    assertEquals(65, ((Number) result.get()).intValue());
  }

  @Test
  void locateFindsSubstringWithOffset() {
    AtomicReference<Object> result = new AtomicReference<>();
    sql.query("SELECT LOCATE('c', 'abcabc', 3) AS pos FROM mtcars LIMIT 1", rs -> {
      try {
        rs.next();
        result.set(rs.getObject("pos"));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    assertEquals(3, ((Number) result.get()).intValue());
  }

  @Test
  void repeatRepeatsString() {
    AtomicReference<Object> result = new AtomicReference<>();
    sql.query("SELECT REPEAT('a', 4) AS rep FROM mtcars LIMIT 1", rs -> {
      try {
        rs.next();
        result.set(rs.getObject("rep"));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    assertEquals("aaaa", result.get());
  }

  @Test
  void spaceGeneratesSpaces() {
    AtomicReference<Object> result = new AtomicReference<>();
    sql.query("SELECT SPACE(5) AS sp FROM mtcars LIMIT 1", rs -> {
      try {
        rs.next();
        result.set(rs.getObject("sp"));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    assertEquals("     ", result.get());
  }

  @Test
  void insertReplacesPortion() {
    AtomicReference<Object> result = new AtomicReference<>();
    sql.query("SELECT INSERT('abcdef', 3, 2, 'XYZ') AS ins FROM mtcars LIMIT 1", rs -> {
      try {
        rs.next();
        result.set(rs.getObject("ins"));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    assertEquals("abXYZef", result.get());
  }

  @Test
  void soundexAndDifferenceWork() {
    AtomicReference<Object> sx = new AtomicReference<>();
    AtomicReference<Object> diff = new AtomicReference<>();
    sql.query("SELECT SOUNDEX('Robert') AS sx, DIFFERENCE('Smith', 'Smyth') AS diff FROM mtcars LIMIT 1", rs -> {
      try {
        rs.next();
        sx.set(rs.getObject("sx"));
        diff.set(rs.getObject("diff"));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    assertEquals("R163", sx.get());
    assertEquals(4, ((Number) diff.get()).intValue());
  }

  @Test
  void handlesNullAndEmptyInputsGracefully() {
    AtomicReference<Object> ascii = new AtomicReference<>();
    AtomicReference<Object> loc = new AtomicReference<>();
    sql.query("SELECT ASCII(NULL) AS a, LOCATE(NULL, 'abc') AS l FROM mtcars LIMIT 1", rs -> {
      try {
        rs.next();
        ascii.set(rs.getObject("a"));
        loc.set(rs.getObject("l"));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    assertNull(ascii.get());
    assertNull(loc.get());
  }
}
