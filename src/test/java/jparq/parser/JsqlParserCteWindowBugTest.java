package jparq.parser;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.SqlParser;

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

  @Test
  void jsqlParserParsesStrippedCteWindowJoin() {
    assertDoesNotThrow(() -> CCJSqlParserUtil.parse(WINDOW_CTE_STRIPPED_SQL),
        "JSqlParser should parse the comment-free variant after comment stripping");
  }

  @Test
  void sqlParserFallsBackToDerivedTableRewriting() {
    SqlParser.Query parsed = assertDoesNotThrow(() -> SqlParser.parseQuery(WINDOW_CTE_SQL));
    assertNotNull(parsed, "Query parsing should succeed after applying the derived-table fallback");
  }

  @Test
  void sqlParserHandlesCommentHeavyWindowCte() {
    SqlParser.Query parsed = assertDoesNotThrow(() -> SqlParser.parseQuery(WINDOW_CTE_WITH_COMMENT_SQL));
    assertNotNull(parsed, "Queries that rely on the CTE workaround should parse even when comments are present");
  }
}
