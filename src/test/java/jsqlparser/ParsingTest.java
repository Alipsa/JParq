package jsqlparser;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.junit.jupiter.api.Test;

public class ParsingTest {

  @Test
  public void testParseStatementWithCommentBlock() throws Exception {
    Statement stm = CCJSqlParserUtil.parse("""
        WITH LatestSalary AS (
            SELECT
                employee,
                salary,
                /* Partition sets the 'window' per employee.
                   Order By puts the latest date first.
                   Use salary.id as a tie-breaker to handle same-date collisions.
                */
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
        """);
    assertNotNull(stm);
  }

  @Test
  public void testParseStatementIssue2352WithoutComment() {
    assertDoesNotThrow(() -> CCJSqlParserUtil.parse("""
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
        """));
  }

  /**
   * This works on the main branch of jsqlparser but fails on 5.3. This test will
   * need to be inverted when upgrading to 5.4. At that point the fallback code in
   * JParq can be removed.
   */
  @Test
  public void testParseStatementWithBlankLines() throws Exception {
    assertThrows(JSQLParserException.class, () -> CCJSqlParserUtil.parse("""
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
        """));
  }
}
