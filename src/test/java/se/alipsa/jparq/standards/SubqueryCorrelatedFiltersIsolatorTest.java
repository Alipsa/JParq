package se.alipsa.jparq.standards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import jparq.WhereTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class SubqueryCorrelatedFiltersIsolatorTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL url = WhereTest.class.getResource("/acme");
    Assertions.assertNotNull(url, "acme must be on the test classpath (src/test/resources)");

    Path dir = Paths.get(url.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Expected output
   * 
   * <pre>
   * employee   department
   *        2            1
   *        4            3
   *        5            3
   * </pre>
   */
  @Test
  void testCteAndJoin() {
    jparqSql.query("""
          WITH high_salary AS (
            SELECT DISTINCT employee
            FROM salary
            WHERE salary >= 180000.0
          )
          SELECT ed.employee AS employee_id, ed.department AS department_id
          FROM employee_department ed
          JOIN high_salary hs ON hs.employee = ed.employee
          ORDER BY employee_id;
        """, rs -> {
      try {
        List<String> rows = new ArrayList<>();
        while (rs.next()) {
          int employeeId = rs.getInt("employee_id");
          int departmentId = rs.getInt("department_id");
          rows.add(employeeId + ":" + departmentId);
        }
        assertEquals(List.of("2:1", "4:3", "5:3"), rows, "Expected rows ordered by employee_id alias, got " + rows);
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  /**
   * Expected result:
   * <pre>
   *   DEPARTMENT_NAME
   *   IT
   *   Sales
   *   Sales
   * </pre>
   */
  @Test
  void testDepartmentNameSubquery() {
    jparqSql.query("""
          WITH high_salary AS (
            SELECT DISTINCT employee
            FROM salary
            WHERE salary >= 180000.0
          )
          SELECT (SELECT d.department FROM departments d WHERE d.id = derived.department_id) AS department_name
          FROM (
            SELECT ed.employee AS employee_id, ed.department AS department_id
            FROM employee_department ed
            JOIN high_salary hs ON hs.employee = ed.employee
          ) AS derived
          ORDER BY department_name;
        """, rs -> {
      try {
        List<String> departments = new ArrayList<>();
        while (rs.next()) {
          departments.add(rs.getString("department_name"));
        }
        assertEquals(List.of("IT", "Sales", "Sales"), departments,
            "Expected department names ordered by derived inline view correlation");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Disabled
  @Test
  void testSalaryChangeCountSubquery() {
    jparqSql.query("""
          WITH high_salary AS (
            SELECT DISTINCT employee
            FROM salary
            WHERE salary >= 180000.0
          )
          SELECT (SELECT COUNT(*) FROM salary s WHERE s.employee = derived.employee_id) AS salary_change_count
          FROM (
            SELECT ed.employee AS employee_id, ed.department AS department_id
            FROM employee_department ed
            JOIN high_salary hs ON hs.employee = ed.employee
          ) AS derived
          ORDER BY salary_change_count;
        """, rs -> {
      try {
        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(3, rows, "Expected 3 rows from the salary change count subquery, got " + rows);
      } catch (SQLException e) {
        fail(e);
      }
    });
  }
}
