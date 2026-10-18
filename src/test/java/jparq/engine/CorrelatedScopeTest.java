package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.CorrelatedSubqueryRewriter;

/** Checks query-local alias shadowing during correlated-subquery rewriting. */
class CorrelatedScopeTest {
  /**
   * Local aliases must never invoke the outer value resolver.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void localAliasShadowsOuterQualifier() throws Exception {
    for (String source : List.of("o", "o o", "o O")) {
      var select = (Select) CCJSqlParserUtil.parse("SELECT COUNT(*) FROM " + source + " WHERE o.label < 'c'");
      var rewritten = CorrelatedSubqueryRewriter.rewrite(select, List.of("o"), Set.of("label"),
          (qualifier, column) -> fail("Local column was treated as correlated: " + qualifier + "." + column));
      assertFalse(rewritten.correlated());
      assertTrue(rewritten.correlatedReferences().isEmpty());
      assertTrue(rewritten.sql().contains("o.label"));
    }
  }

  /**
   * Shadowing one outer qualifier must preserve other correlated qualifiers.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void shadowingPreservesOtherOuterReferences() throws Exception {
    var select = (Select) CCJSqlParserUtil.parse("SELECT COUNT(*) FROM o o WHERE o.id > t.id");
    var rewritten = CorrelatedSubqueryRewriter.rewrite(select, List.of("o", "t"), (qualifier, column) -> {
      assertEquals("U:t", qualifier);
      return 3;
    });
    assertTrue(rewritten.correlated());
    assertEquals(Set.of(new CorrelatedSubqueryRewriter.QualifiedColumn("U:t", "id")), rewritten.correlatedReferences());
    assertTrue(rewritten.sql().contains("o.id > 3"));
  }

  /**
   * Bindings in nested SELECTs must not leak into their parent SELECT.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void nestedBindingsAreScoped() throws Exception {
    var select = (Select) CCJSqlParserUtil
        .parse("SELECT (SELECT MAX(o.id) FROM o o) FROM lookup i WHERE i.other = o.label");
    var rewritten = CorrelatedSubqueryRewriter.rewrite(select, List.of("o"), (qualifier, column) -> {
      assertEquals("label", column);
      return "d";
    });
    assertTrue(rewritten.correlated());
    assertEquals(Set.of("label"), rewritten.correlatedColumns());
    assertTrue(rewritten.sql().contains("MAX(o.id)"));
    assertTrue(rewritten.sql().contains("i.other = 'd'"));
  }
}
