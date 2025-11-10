package jparq.qualified;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.QualifiedWildcard;
import se.alipsa.jparq.engine.SqlParser;

/** Tests focused on the parser behaviour for qualified wildcard projections. */
class AstTest {

  @Test
  void parsesAliasQualifiedWildcard() {
    SqlParser.Select select = SqlParser.parseSelectAllowQualifiedWildcards("SELECT t.* FROM T t");

    List<QualifiedWildcard> wildcards = select.qualifiedWildcards();

    assertEquals(1, wildcards.size(), "Expected a single qualified wildcard entry");
    assertEquals("t", wildcards.getFirst().qualifier(), "Alias should be preserved as qualifier");
  }

  @Test
  void parsesSchemaQualifiedWildcard() {
    SqlParser.Select select = SqlParser.parseSelectAllowQualifiedWildcards("SELECT schema.table.* FROM schema.table");

    List<QualifiedWildcard> wildcards = select.qualifiedWildcards();

    assertEquals(1, wildcards.size(), "Expected a single qualified wildcard entry");
    assertEquals("schema.table", wildcards.getFirst().qualifier(),
        "Schema-qualified table name should be preserved");
  }

  @Test
  void parsesWildcardAlongsideExpressions() {
    SqlParser.Select select = SqlParser
        .parseSelectAllowQualifiedWildcards("SELECT m.*, t.x FROM mtcars m JOIN t ON t.id = m.id");

    List<QualifiedWildcard> wildcards = select.qualifiedWildcards();

    assertFalse(wildcards.isEmpty(), "Qualified wildcard should be captured from the SELECT list");
    assertEquals("m", wildcards.getFirst().qualifier(), "The qualifier should reflect the table alias");
    assertTrue(select.columns().size() >= 1, "Existing column parsing should continue to function");
  }
}
