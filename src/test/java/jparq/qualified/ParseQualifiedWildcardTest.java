package jparq.qualified;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.QualifiedWildcard;
import se.alipsa.jparq.engine.SqlParser;

/** Tests focused on the parser behaviour for qualified wildcard projections. */
class ParseQualifiedWildcardTest {

  @Test
  @DisplayName("Parser retains alias-qualified wildcard projections")
  void parsesAliasQualifiedWildcard() {
    SqlParser.Select select = SqlParser.parseSelectAllowQualifiedWildcards("SELECT t.* FROM T t");

    List<QualifiedWildcard> wildcards = select.qualifiedWildcards();

    assertEquals(1, wildcards.size(), "Expected a single qualified wildcard entry");
    assertEquals("t", wildcards.getFirst().qualifier(), "Alias should be preserved as qualifier");
  }

  @Test
  @DisplayName("Parser retains quoted identifier qualified wildcards")
  void parsesQuotedQualifiedWildcard() {
    SqlParser.Select select = SqlParser.parseSelectAllowQualifiedWildcards("SELECT \"My Table\".* FROM \"My Table\"");

    List<QualifiedWildcard> wildcards = select.qualifiedWildcards();

    assertEquals(1, wildcards.size(), "Expected a single qualified wildcard entry");
    assertEquals("My Table", wildcards.getFirst().qualifier(),
        "Quoted identifier should preserve the unquoted qualifier");
  }

  @Test
  @DisplayName("Parser captures schema-qualified wildcard projections")
  void parsesSchemaQualifiedWildcard() {
    SqlParser.Select select = SqlParser.parseSelectAllowQualifiedWildcards("SELECT schema.table.* FROM schema.table");

    List<QualifiedWildcard> wildcards = select.qualifiedWildcards();

    assertEquals(1, wildcards.size(), "Expected a single qualified wildcard entry");
    assertEquals("schema.table", wildcards.getFirst().qualifier(), "Schema-qualified table name should be preserved");
  }

  @Test
  @DisplayName("Parser retains qualified wildcards alongside other expressions")
  void parsesWildcardAlongsideExpressions() {
    SqlParser.Select select = SqlParser
        .parseSelectAllowQualifiedWildcards("SELECT m.*, t.x FROM mtcars m JOIN t ON t.id = m.id");

    List<QualifiedWildcard> wildcards = select.qualifiedWildcards();

    assertFalse(wildcards.isEmpty(), "Qualified wildcard should be captured from the SELECT list");
    assertEquals("m", wildcards.getFirst().qualifier(), "The qualifier should reflect the table alias");
    assertTrue(select.columns().size() >= 1, "Existing column parsing should continue to function");
  }

  @Test
  @DisplayName("Parser preserves qualified wildcards when an unqualified star is present")
  void parsesQualifiedWildcardAlongsideUnqualifiedStar() {
    SqlParser.Select select = SqlParser.parseSelectAllowQualifiedWildcards("SELECT t.*, * FROM table t");

    List<QualifiedWildcard> wildcards = select.qualifiedWildcards();

    assertEquals(1, wildcards.size(), "The qualified wildcard should not be discarded");
    assertEquals("t", wildcards.getFirst().qualifier(), "Alias qualifier should be preserved");
  }
}
