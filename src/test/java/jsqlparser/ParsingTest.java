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
   * This works on the main branch of jsqlparser but fails on 5.3.
   * THis test will need to be inverted when upgrading to 5.4.
   * At that point the fallback code in JParq can be removed.
   */
  @Test
  public void testParseStatementWithBlankLines() throws Exception {
    StringBuilder sb = new StringBuilder()
        .append("\tWITH LatestSalary AS (\n")
        .append("\t  SELECT\n")
        .append("\t    employee,\n")
        .append("\t    salary,\n")
        .append("\n")
        .append("\n")
        .append("\n")
        .append("\n")
        .append("\t    ROW_NUMBER() OVER (\n")
        .append("\t     PARTITION BY employee\n")
        .append("\t      ORDER BY change_date DESC, id DESC\n")
        .append("\t    ) as rn\n")
        .append("\t  FROM\n")
        .append("\t    salary\n")
        .append("\t)\n")
        .append("\tSELECT e.*, ls.salary\n")
        .append("\tFROM employees e\n")
        .append("\tJOIN LatestSalary ls ON e.id = ls.employee\n")
        .append("\tWHERE ls.rn = 1;");
    //System.out.println(sb);
    assertThrows(JSQLParserException.class, () -> CCJSqlParserUtil.parse(sb.toString()));
  }
}
