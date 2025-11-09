package jparq.join;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering {@code JOIN ... USING (...)} support, ensuring the
 * coalesced columns appear once and in the correct order regardless of join
 * type.
 */
class JoinUsingTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = JoinUsingTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Verify an {@code INNER JOIN ... USING} emits a single instance of the join
   * column and that the merged column precedes the remaining columns.
   */
  @Test
  void innerJoinUsingEmitsSingleCoalescedColumn() {
    String sql = """
        SELECT *
        FROM employee_department ed
        INNER JOIN (
          SELECT id AS department, department AS department_name
          FROM departments
        ) d USING (department)
        ORDER BY ed.employee
        """;

    jparqSql.query(sql, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        Assertions.assertEquals(4, meta.getColumnCount(), "USING should collapse duplicate join columns");
        Assertions.assertEquals("department", meta.getColumnLabel(1), "Joined column should appear first");
        Assertions.assertEquals("id", meta.getColumnLabel(2),
            "Left table columns should include id after USING column");
        Assertions.assertEquals("employee", meta.getColumnLabel(3),
            "Employee column should follow id for the left table");
        Assertions.assertEquals("department_name", meta.getColumnLabel(4),
            "Right table columns should appear after left table columns");

        List<Integer> departments = new ArrayList<>();
        List<String> names = new ArrayList<>();
        while (rs.next()) {
          departments.add(rs.getInt("department"));
          names.add(rs.getString("department_name"));
        }
        Assertions.assertFalse(departments.isEmpty(), "Joined rows should be returned");
        Assertions.assertEquals(names.size(), departments.size(), "Each row should expose the department name");
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }

  /**
   * Ensure outer joins honour {@code USING} semantics when the right-hand side
   * does not contain a matching row, keeping the merged column while returning
   * {@code NULL} for unmatched columns.
   */
  @Test
  void leftJoinUsingRetainsOuterRows() {
    String sql = """
        SELECT *
        FROM (
          SELECT id AS department, department AS department_name
          FROM departments
        ) d
        LEFT JOIN (
          SELECT employee, department
          FROM employee_department
          WHERE department <> 2
        ) ed USING (department)
        ORDER BY department, employee
        """;

    jparqSql.query(sql, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        Assertions.assertEquals(3, meta.getColumnCount(), "USING should keep a single department column");
        Assertions.assertEquals("department", meta.getColumnLabel(1), "Merged USING column should lead the projection");
        Assertions.assertEquals("department_name", meta.getColumnLabel(2),
            "Left table columns should follow the USING column");
        Assertions.assertEquals("employee", meta.getColumnLabel(3), "Right table column should trail left columns");

        boolean foundHr = false;
        boolean foundNull = false;
        while (rs.next()) {
          int department = rs.getInt("department");
          if (department == 2) {
            foundHr = true;
            Object employee = rs.getObject("employee");
            if (employee == null) {
              foundNull = true;
            }
          }
        }
        Assertions.assertTrue(foundHr, "HR department should be present even without matching employees");
        Assertions.assertTrue(foundNull, "Left join should preserve unmatched department rows");
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }

  /**
   * Confirm that {@code NULL} values never satisfy {@code USING} equality,
   * preventing joins from matching rows that only share {@code NULL}
   * placeholders.
   */
  @Test
  void nullValuesDoNotMatchUsingJoin() {
    String sql = """
        SELECT *
        FROM (
          SELECT CASE WHEN id = 1 THEN NULL END AS department
          FROM departments
          WHERE id = 1
        ) d1
        INNER JOIN (
          SELECT CASE WHEN id = 1 THEN NULL END AS department
          FROM departments
          WHERE id = 1
        ) d2 USING (department)
        """;

    jparqSql.query(sql, rs -> {
      try {
        Assertions.assertFalse(rs.next(), "Rows with NULL join keys must not match under USING semantics");
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }

  /**
   * Ensure {@code RIGHT JOIN ... USING} coalesces the merged column from the
   * preserved right-hand row when the owner table does not contribute a matching
   * record.
   */
  @Test
  void rightJoinUsingCoalescesRightKey() {
    String sql = """
        SELECT *
        FROM (
          SELECT employee, department
          FROM employee_department
          WHERE department <> 2
        ) ed
        RIGHT JOIN (
          SELECT id AS department, department AS department_name
          FROM departments
        ) d USING (department)
        ORDER BY department, employee
        """;

    jparqSql.query(sql, rs -> {
      try {
        boolean preservedRightRow = false;
        while (rs.next()) {
          Number key = (Number) rs.getObject("department");
          Object employee = rs.getObject("employee");
          if (employee == null) {
            preservedRightRow = true;
            Assertions.assertNotNull(key,
                "RIGHT JOIN should retain the USING column value when the owner table is absent");
          }
        }
        Assertions.assertTrue(preservedRightRow,
            "RIGHT JOIN must surface rows from the preserved side even without a matching owner");
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }
}
