package jparq.parser;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.SqlParser;

/**
 * Regression coverage for native parsing of CTEs containing window functions
 * and joins.
 */
class JsqlParserCteWindowBugTest {

  private static final String WINDOW_CTE_SQL = """
      WITH latest_salary AS (
        SELECT employee,
               salary,
               ROW_NUMBER() OVER (PARTITION BY employee ORDER BY change_date DESC, id DESC) AS rn
        FROM salary
      )
      SELECT e.id, e.first_name, e.last_name, l.salary
      FROM employees e
      JOIN latest_salary l ON l.employee = e.id
      WHERE l.rn = 1
      """;

  private static final String WINDOW_CTE_WITH_COMMENT_SQL = """
      WITH LatestSalary AS (
          SELECT
              employee,
              salary,
              /* Comment after salary keeps repro close to real query */
              ROW_NUMBER() OVER (
                  PARTITION BY employee
                  ORDER BY change_date DESC, id DESC
              ) as rn
          FROM
              salary
      )
      SELECT
          e.*,
          ls.salary
      FROM
          employees e
      JOIN
          LatestSalary ls ON e.id = ls.employee
      WHERE
          ls.rn = 1;
      """;

  private static final String WINDOW_CTE_STRIPPED_SQL = """
      WITH LatestSalary AS (
          SELECT
              employee,
              salary,
              ROW_NUMBER() OVER (
                  PARTITION BY employee
                  ORDER BY change_date DESC, id DESC
              ) as rn
          FROM
              salary
      )
      SELECT
          e.*,
          ls.salary
      FROM
          employees e
      JOIN
          LatestSalary ls ON e.id = ls.employee
      WHERE
          ls.rn = 1;
      """;

  /**
   * Verifies that the library parses the comment-free window/CTE/join shape
   * directly.
   */
  @Test
  void jsqlParserParsesStrippedCteWindowJoin() {
    assertDoesNotThrow(() -> CCJSqlParserUtil.parse(WINDOW_CTE_STRIPPED_SQL),
        "JSqlParser should parse the comment-free variant after comment stripping");
  }

  /** Keeps native CTE declarations when parsing window functions and joins. */
  @Test
  void sqlParserParsesWindowCteDirectly() {
    SqlParser.Query parsed = assertDoesNotThrow(() -> SqlParser.parseQuery(WINDOW_CTE_SQL));
    assertNotNull(parsed);
    assertEquals(1, parsed.commonTableExpressions().size());
  }

  /**
   * Parses the original comment-heavy window/CTE/join shape without rewriting its
   * WITH clause.
   */
  @Test
  void sqlParserHandlesCommentHeavyWindowCte() {
    SqlParser.Query parsed = assertDoesNotThrow(() -> SqlParser.parseQuery(WINDOW_CTE_WITH_COMMENT_SQL));
    assertNotNull(parsed);
    assertEquals(1, parsed.commonTableExpressions().size());
  }

  /**
   * Retains lexical parameter indexes when window CTE queries use row-count
   * parameters.
   */
  @Test
  void windowCteParametersRetainOriginalPositions() {
    String sql = WINDOW_CTE_SQL.replace("WHERE l.rn = 1", "WHERE l.rn = ?") + " LIMIT ?";
    assertEquals(Map.of(2, "LIMIT"), SqlParser.rowCountParameters(sql));
    assertEquals(1, SqlParser.parseQuery(sql).commonTableExpressions().size());
  }
}
