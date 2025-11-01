package jparq.join;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering explicit {@code FULL [OUTER] JOIN} syntax using
 * {@code JOIN ... ON ...} in the {@code FROM} clause.
 */
class FullJoinTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = FullJoinTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Verify that a {@code FULL JOIN} retains every row from both tables when the
   * join condition fails to match.
   */
  @Test
  void fullJoinIncludesRowsFromBothTables() {
    Set<Integer> employeeIds = fetchEmployeeIds();
    Set<Integer> salaryEmployees = fetchSalaryEmployees();

    String sql = """
        SELECT e.id AS emp_id, s.employee AS salary_id
        FROM employees e
        FULL JOIN salary s ON e.id = s.employee AND 1 = 0
        """;

    Set<Integer> observedEmployees = new LinkedHashSet<>();
    Set<Integer> observedSalary = new LinkedHashSet<>();
    AtomicBoolean sawNullEmployee = new AtomicBoolean(false);
    AtomicBoolean sawNullSalary = new AtomicBoolean(false);
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          Integer employeeId = (Integer) rs.getObject("emp_id");
          if (employeeId == null) {
            sawNullEmployee.set(true);
          } else {
            observedEmployees.add(employeeId);
          }
          Integer salaryId = (Integer) rs.getObject("salary_id");
          if (salaryId == null) {
            sawNullSalary.set(true);
          } else {
            observedSalary.add(salaryId);
          }
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(employeeIds, observedEmployees,
        "Full join should include every identifier from the left table");
    Assertions.assertEquals(salaryEmployees, observedSalary,
        "Full join should include every identifier from the right table");
    Assertions.assertTrue(sawNullEmployee.get(), "Rows without salary matches should yield NULL employee identifiers");
    Assertions.assertTrue(sawNullSalary.get(), "Rows without employee matches should yield NULL salary identifiers");
  }

  /**
   * Ensure the optional {@code OUTER} keyword is accepted for a
   * {@code FULL OUTER JOIN} and that matching rows remain available.
   */
  @Test
  void fullOuterJoinKeywordIsOptional() {
    Set<Integer> employeeIds = fetchEmployeeIds();
    Set<Integer> salaryEmployees = fetchSalaryEmployees();

    String sql = """
        SELECT e.id AS emp_id, s.employee AS salary_id
        FROM employees e
        FULL OUTER JOIN salary s ON e.id = s.employee
        """;

    AtomicBoolean sawMatchingRow = new AtomicBoolean(false);
    Set<Integer> observedEmployees = new LinkedHashSet<>();
    Set<Integer> observedSalary = new LinkedHashSet<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          Integer employeeId = (Integer) rs.getObject("emp_id");
          Integer salaryId = (Integer) rs.getObject("salary_id");
          if (employeeId != null) {
            observedEmployees.add(employeeId);
          }
          if (salaryId != null) {
            observedSalary.add(salaryId);
          }
          if (employeeId != null && salaryId != null) {
            sawMatchingRow.set(true);
          }
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(employeeIds, observedEmployees,
        "Full outer join should preserve employee identifiers");
    Assertions.assertEquals(salaryEmployees, observedSalary,
        "Full outer join should preserve salary identifiers");
    Assertions.assertTrue(sawMatchingRow.get(), "At least one row should contain matching values from both tables");
  }

  /**
   * Retrieve the employee identifiers present in the employees table.
   *
   * @return ordered set of employee identifiers
   */
  private static Set<Integer> fetchEmployeeIds() {
    Set<Integer> ids = new LinkedHashSet<>();
    jparqSql.query("SELECT id FROM employees ORDER BY id", rs -> {
      try {
        while (rs.next()) {
          ids.add(rs.getInt(1));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
    Assertions.assertFalse(ids.isEmpty(), "Employees table should not be empty");
    return ids;
  }

  /**
   * Retrieve the employee identifiers referenced by the salary table.
   *
   * @return ordered set of employee identifiers present in the salary table
   */
  private static Set<Integer> fetchSalaryEmployees() {
    Set<Integer> employees = new LinkedHashSet<>();
    jparqSql.query("SELECT employee FROM salary ORDER BY employee", rs -> {
      try {
        while (rs.next()) {
          employees.add(rs.getInt(1));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
    Assertions.assertFalse(employees.isEmpty(), "Salary table should not be empty");
    return employees;
  }
}
