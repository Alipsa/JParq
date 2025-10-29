package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.SqlParser;

/** Tests for comment handling in {@link SqlParser}. */
class SqlParserCommentTest {

  @Test
  void parseSelectIgnoresLineComments() {
    String sql = """
        -- leading comment
        SELECT model, mpg FROM mtcars -- inline comment
        WHERE cyl = 6 -- trailing comment
        ORDER BY mpg DESC
        """;

    SqlParser.Select select = SqlParser.parseSelect(sql);

    assertEquals("mtcars", select.table(), "Table name should be parsed despite comments");
    assertEquals(2, select.labels().size(), "SELECT list should retain both columns");
    assertEquals("mpg", select.orderBy().get(0).column(), "ORDER BY column should resolve to mpg");
  }

  @Test
  void parseSelectPreservesLiteralsWhileRemovingBlockComments() {
    String sql = """
        SELECT '--value -- text' AS literal_label
        FROM /* block comment */ mtcars
        WHERE model = 'Ferrari /* comment marker */'
        /* final block comment */
        """;

    SqlParser.Select select = assertDoesNotThrow(() -> SqlParser.parseSelect(sql));

    assertEquals("literal_label", select.labels().get(0), "Alias should be preserved");
    assertEquals("'--value -- text'", select.expressions().get(0).toString(),
        "String literal should remain untouched");
    assertEquals("model = 'Ferrari /* comment marker */'", select.where().toString(),
        "WHERE clause literal content must be retained");
  }
}
