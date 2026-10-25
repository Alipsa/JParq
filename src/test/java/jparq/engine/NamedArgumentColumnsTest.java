package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.ColumnsUsed;
import se.alipsa.jparq.engine.SqlParser;

/** Tests column discovery in named function arguments. */
class NamedArgumentColumnsTest {
  /**
   * Collects both named operands, preserving qualifiers and removing duplicates.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void collectsNamedOperands() throws Exception {
    var expression = CCJSqlParserUtil.parseExpression("POSITION(n.needle IN LOWER(n.name)) + POSITION('x' IN n.name)");
    assertEquals(Set.of("needle", "name"), ColumnsUsed.inWhere(expression));
    assertEquals(Set.of("needle", "name"), ColumnsUsed.inScope(expression));
    assertEquals(List.of("n.needle", "n.name", "n.name"),
        ColumnsUsed.referencesInScope(expression).stream().map(Object::toString).toList());
    assertEquals(Set.of("needle", "name"), SqlParser.collectQualifiedColumns(expression, List.of("n"), false));
    assertEquals(Set.of(), SqlParser.collectQualifiedColumns(expression, List.of("other")));
  }

  /**
   * Includes unqualified operands only when the caller requests them.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void collectsUnqualifiedOperands() throws Exception {
    var expression = CCJSqlParserUtil.parseExpression("POSITION(needle IN name)");
    assertEquals(Set.of("needle", "name"), SqlParser.collectQualifiedColumns(expression, List.of("n")));
    assertEquals(Set.of(), SqlParser.collectQualifiedColumns(expression, List.of("n"), false));
  }

  /**
   * Preserves the caller's query-scope boundary inside named arguments.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void keepsSubqueryScope() throws Exception {
    var expression = CCJSqlParserUtil.parseExpression("POSITION((SELECT needle FROM needles) IN n.name)");
    assertEquals(Set.of("name"), ColumnsUsed.inScope(expression));
    assertEquals(List.of("n.name"), ColumnsUsed.referencesInScope(expression).stream().map(Object::toString).toList());
  }
}
