package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.SqlParser;

/** Tests for CTE parsing support in {@link SqlParser}. */
class SqlParserCteTest {

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
    assertEquals("summary", cte.name());
    assertInstanceOf(SqlParser.Select.class, cte.query(), "CTE body should be parsed as a SELECT");

    SqlParser.TableReference reference = select.tableReferences().get(0);
    assertNotNull(reference.commonTableExpression(), "Table reference should link to the CTE definition");
    assertEquals("summary", reference.commonTableExpression().name());
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
    assertEquals("base", cte.name());
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
