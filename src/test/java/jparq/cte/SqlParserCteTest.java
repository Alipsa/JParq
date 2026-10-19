package jparq.cte;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.SqlParser;

/** Tests for CTE parsing support in {@link SqlParser}. */
class SqlParserCteTest {

  /**
   * Verifies that a table-free recursive anchor retains its projection and
   * filter.
   */
  @Test
  void recursiveCteAcceptsSelectAnchorWithoutFrom() {
    var query = SqlParser.parseSelect("""
        WITH RECURSIVE r(n) AS (
          SELECT 1 + 0 WHERE TRUE
          UNION ALL
          SELECT n + 1 FROM r WHERE n < 3
        )
        SELECT n FROM r ORDER BY n
        """);
    var cte = query.commonTableExpressions().getFirst();
    assertEquals(List.of("n"), cte.columnAliases());
    var body = assertInstanceOf(SqlParser.SetQuery.class, cte.query());
    var anchor = assertInstanceOf(SqlParser.Select.class, body.components().getFirst().query());
    assertEquals("1 + 0", anchor.expressions().getFirst().toString());
    assertNotNull(anchor.where());
    var input = anchor.tableReferences().getFirst().valueTable();
    assertNotNull(input);
    assertEquals(List.of(List.of()), input.rows());
    assertTrue(input.columnNames().isEmpty());
    var recursive = assertInstanceOf(SqlParser.Select.class, body.components().get(1).query());
    assertEquals("r", recursive.table());
  }

  @Test
  void derivedPlanUsesItsOwnCteDeclaration() {
    var query = SqlParser.parseSelect("WITH a AS (SELECT id FROM small) "
        + "SELECT COUNT(*) FROM (WITH a AS (SELECT id FROM big) SELECT id FROM a) t");
    var derived = query.tableReferences().getFirst().subquery();
    assertTrue(query.tableReferences().getFirst().isDerivedTable());
    assertFalse(derived.tableReferences().getFirst().isDerivedTable());
    var local = derived.tableReferences().getFirst().commonTableExpression();
    assertEquals("big", ((SqlParser.Select) local.query()).table());
    assertEquals("small", ((SqlParser.Select) query.commonTableExpressions().getFirst().query()).table());
  }

  /** Verifies that materialized scopes never produce legacy pre-stage options. */
  @Test
  void materializedOperationsStayInsideTheirOwnScope() {
    for (String sql : List.of(
        "WITH x AS (SELECT DISTINCT id FROM o ORDER BY id LIMIT 2 OFFSET 1 ROWS) "
            + "SELECT id FROM x ORDER BY id DESC LIMIT 1 OFFSET 1 ROWS",
        "SELECT id FROM (SELECT DISTINCT id FROM o ORDER BY id LIMIT 2 OFFSET 1 ROWS) x "
            + "ORDER BY id DESC LIMIT 1 OFFSET 1 ROWS")) {
      var select = SqlParser.parseSelect(sql);
      assertEquals(1, select.limit());
      assertEquals(1, select.offset());
      assertFalse(select.distinct());
      assertFalse(select.innerDistinct());
      assertTrue(select.innerDistinctColumns().isEmpty());
      assertEquals(-1, select.preLimit());
      assertEquals(0, select.preOffset());
      assertTrue(select.preOrderBy().isEmpty());
      assertEquals(1, select.orderBy().size());
      assertFalse(select.orderBy().getFirst().asc());
    }
  }

  @Test
  void parseSelectCapturesCommonTableExpressions() {
    String sql = """
        WITH summary AS (
          SELECT model, cyl FROM mtcars
        )
        SELECT model FROM summary
        """;

    SqlParser.Select select = SqlParser.parseSelect(sql);

    assertEquals(1, select.commonTableExpressions().size(), "Expected a single CTE definition");
    SqlParser.CommonTableExpression cte = select.commonTableExpressions().get(0);
    assertEquals("summary", cte.name().text());
    assertInstanceOf(SqlParser.Select.class, cte.query(), "CTE body should be parsed as a SELECT");

    SqlParser.TableReference reference = select.tableReferences().get(0);
    assertNotNull(reference.commonTableExpression(), "Table reference should link to the CTE definition");
    assertEquals("summary", reference.commonTableExpression().name().text());
  }

  @Test
  void parseSelectCapturesCteColumnAliases() {
    String sql = """
        WITH summary(model_name, cylinder_count) AS (
          SELECT model, cyl FROM mtcars
        )
        SELECT cylinder_count FROM summary
        """;

    SqlParser.Select select = SqlParser.parseSelect(sql);

    SqlParser.CommonTableExpression cte = select.commonTableExpressions().get(0);
    assertEquals(List.of("model_name", "cylinder_count"), cte.columnAliases(),
        "CTE column aliases should be preserved");

    SqlParser.TableReference reference = select.tableReferences().get(0);
    assertNotNull(reference.commonTableExpression());
    assertEquals(cte, reference.commonTableExpression());
  }

  @Test
  void parseSetQueryPropagatesCommonTableExpressions() {
    String sql = """
        WITH base AS (
          SELECT cyl FROM mtcars
        )
        SELECT cyl FROM base
        UNION
        SELECT cyl FROM base
        """;

    SqlParser.Query query = SqlParser.parseQuery(sql);

    SqlParser.SetQuery setQuery = assertInstanceOf(SqlParser.SetQuery.class, query);
    assertEquals(1, setQuery.commonTableExpressions().size(), "Set query should expose CTE definitions");
    SqlParser.CommonTableExpression cte = setQuery.commonTableExpressions().get(0);
    assertEquals("base", cte.name().text());
    assertTrue(setQuery.components().stream().allMatch(component -> {
      SqlParser.Query componentQuery = component.query();
      if (componentQuery instanceof SqlParser.Select componentSelect) {
        return componentSelect.commonTableExpressions().contains(cte);
      }
      if (componentQuery instanceof SqlParser.SetQuery nested) {
        return nested.commonTableExpressions().contains(cte);
      }
      return false;
    }), "Each set component should be able to reference the CTE");
  }
}
