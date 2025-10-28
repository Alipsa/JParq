package jparq.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for COLLATE, SIMILAR TO and REGEXP_LIKE. */
class CollationAndComparisonTest {

  private static JParqSql sql;

  @BeforeAll
  static void setup() {
    sql = StringFunctionTestSupport.createSql();
  }

  @Test
  void testCollationSimilarAndRegexp() {
    AtomicReference<String> collated = new AtomicReference<>();
    AtomicBoolean similarTrue = new AtomicBoolean();
    AtomicBoolean similarFalse = new AtomicBoolean();
    AtomicBoolean similarNested = new AtomicBoolean();
    AtomicBoolean similarEscaped = new AtomicBoolean();
    AtomicBoolean regexpTrue = new AtomicBoolean();
    AtomicBoolean regexpCase = new AtomicBoolean();
    AtomicBoolean regexpFalse = new AtomicBoolean();

    sql.query("SELECT 'abc' COLLATE latin1_general_ci AS collated, " + "'cat' SIMILAR TO '(cat|dog)' AS similar_true, "
        + "'cab' SIMILAR TO 'c(a|o)c' AS similar_false, "
        + "'caterpillar' SIMILAR TO 'c(ater|a|at)pillar' AS similar_nested, "
        + "'under_score' SIMILAR TO 'under\\_score' ESCAPE '\\' AS similar_escaped, "
        + "REGEXP_LIKE('abc123', '^[a-z]+[0-9]+$') AS regexp_true, " + "REGEXP_LIKE('AbC', 'abc', 'i') AS regexp_case, "
        + "REGEXP_LIKE('cat', 'dog') AS regexp_false FROM mtcars LIMIT 1", rs -> {
          try {
            if (!rs.next()) {
              fail("Expected a result row");
              return;
            }
            collated.set(rs.getString("collated"));
            similarTrue.set(asBoolean(rs.getObject("similar_true")));
            similarFalse.set(asBoolean(rs.getObject("similar_false")));
            similarNested.set(asBoolean(rs.getObject("similar_nested")));
            similarEscaped.set(asBoolean(rs.getObject("similar_escaped")));
            regexpTrue.set(asBoolean(rs.getObject("regexp_true")));
            regexpCase.set(asBoolean(rs.getObject("regexp_case")));
            regexpFalse.set(asBoolean(rs.getObject("regexp_false")));
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals("abc", collated.get(), "COLLATE should return the original text");
    assertTrue(similarTrue.get(), "Pattern should match");
    assertFalse(similarFalse.get(), "Pattern should not match");
    assertTrue(similarNested.get(), "Alternation and grouping should match");
    assertTrue(similarEscaped.get(), "Escaped wildcard should match literally");
    assertTrue(regexpTrue.get(), "Regex should match digits suffix");
    assertTrue(regexpCase.get(), "Case-insensitive regex should match");
    assertFalse(regexpFalse.get(), "Regex should not match");
  }

  @Test
  void testRegexpLikeFindsSubstrings() {
    AtomicBoolean substringMatch = new AtomicBoolean();

    sql.query("SELECT REGEXP_LIKE('abc', 'b') AS regexp_substring FROM mtcars LIMIT 1", rs -> {
      try {
        if (!rs.next()) {
          fail("Expected a result row");
          return;
        }
        substringMatch.set(asBoolean(rs.getObject("regexp_substring")));
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertTrue(substringMatch.get(), "REGEXP_LIKE should match unanchored substrings");
  }

  private boolean asBoolean(Object value) {
    if (value == null) {
      return false;
    }
    if (value instanceof Boolean bool) {
      return bool;
    }
    return Boolean.parseBoolean(value.toString());
  }
}
