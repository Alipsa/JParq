package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.JSqlParserSupport;

/**
 * Verifies shared traversal without requiring callers to handle query scopes.
 */
class StatementExpressionTraversalTest {

  /**
   * Visits each CAST exactly once, including inside quantified subqueries.
   *
   * @param sql
   *          statement with one CAST in a nested scope or clause
   * @throws Exception
   *           if parsing fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT CAST(1 AS INTEGER)", "SELECT 1 WHERE 1 < ANY (SELECT CAST(1 AS INTEGER))",
      "SELECT 1 WHERE 1 < ALL (SELECT CAST(1 AS INTEGER))", "SELECT 1 WHERE 1 < SOME (SELECT CAST(1 AS INTEGER))",
      "SELECT 1 WHERE 1 < ANY (SELECT 2 WHERE 2 < ALL (SELECT CAST(1 AS INTEGER)))",
      "SELECT 1 LIMIT (SELECT CAST(1 AS INTEGER))", "SELECT 1 UNION ALL SELECT 2 LIMIT (SELECT CAST(1 AS INTEGER))",
      "SELECT 1 OFFSET (SELECT CAST(1 AS INTEGER)) ROWS", "SELECT 1 FETCH FIRST (SELECT CAST(1 AS INTEGER)) ROWS ONLY",
      "SELECT 1 UNION ALL SELECT 2 OFFSET (SELECT CAST(1 AS INTEGER)) ROWS",
      "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n OFFSET (SELECT CAST(1 AS INTEGER)) ROWS",
      "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n LIMIT 1 OFFSET (SELECT CAST(1 AS INTEGER))",
      "SELECT 1 UNION ALL SELECT 2 FETCH FIRST (SELECT CAST(1 AS INTEGER)) ROWS ONLY",
      "SELECT * FROM UNNEST(ARRAY[(SELECT CAST(1 AS INTEGER))]) AS t(x)",
      "WITH c AS (SELECT CAST(1 AS INTEGER)) SELECT 1 UNION ALL SELECT 2", "VALUES (CAST(1 AS INTEGER))",
      "SELECT 1 FROM (a JOIN b ON a.id = CAST(b.id AS INTEGER))", "SELECT d.n FROM (SELECT CAST(1 AS INTEGER) AS n) d"
  })
  void visitsNestedCastOnce(String sql) throws Exception {
    var visits = new AtomicInteger();
    var statement = (Select) JSqlParserSupport.parseStatement(sql);
    JSqlParserSupport.visitExpressions(statement, new JSqlParserSupport.StatementExpressionVisitor() {
      /** {@inheritDoc} */
      @Override
      public <S> Void visit(CastExpression cast, S context) {
        visits.incrementAndGet();
        return super.visit(cast, context);
      }
    });
    assertEquals(1, visits.get(), sql);
  }
}
