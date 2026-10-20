package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
    assertEquals("'--value -- text'", select.expressions().get(0).toString(), "String literal should remain untouched");
    assertEquals("model = 'Ferrari /* comment marker */'", select.where().toString(),
        "WHERE clause literal content must be retained");
  }
  /** Verifies that apostrophes inside backtick aliases are identifier content. */
  @Test
  void parseSelectPreservesBacktickAliasContainingApostrophe() {
    var select = SqlParser.parseSelect("SELECT model AS `it's` FROM mtcars");
    assertEquals("`it's`", select.labels().getFirst());
  }

  /**
   * Keeps quotes and comment markers inside alternative identifier delimiters.
   *
   * @param identifier
   *          the delimited identifier
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "`it's`", "[it's]", "`a``'--/*b`", "[a]]'--/*b]", "`a\"b`", "[a\"b]"
  })
  void stripCommentsPreservesDelimitedIdentifiers(String identifier) {
    String sql = "SELECT model AS " + identifier + " FROM mtcars";
    assertEquals(sql + " ", SqlParser.stripSqlComments(sql + " -- trailing comment"));
  }

  /**
   * Rejects unfinished identifiers as well as unfinished string literals.
   *
   * @param quote
   *          the opening delimiter
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "`", "[", "'", "\""
  })
  void stripCommentsRejectsUnterminatedQuotes(String quote) {
    assertThrows(IllegalArgumentException.class, () -> SqlParser.stripSqlComments("SELECT " + quote + "unfinished"));
  }

}
