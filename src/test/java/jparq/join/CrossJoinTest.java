package jparq.join;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests verifying explicit and implicit {@code CROSS JOIN} support.
 */
class CrossJoinTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = CrossJoinTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Ensure explicit {@code CROSS JOIN} syntax produces the Cartesian product of
   * the joined tables.
   */
  @Test
  void explicitCrossJoinProducesCartesianProduct() {
    String sql = """
        SELECT e.id AS employee_id, d.department AS department_name
        FROM employees e
        CROSS JOIN departments d
        ORDER BY e.id, d.id
        """;

    int employeeCount = countRows("employees");
    Set<Integer> employees = new HashSet<>();
    Set<String> departments = new HashSet<>();
    int rowCount = executeAndCollect(sql, employees, departments);

    Set<String> expectedDepartments = Set.of("IT", "HR", "Sales");

    Assertions.assertEquals(employeeCount, employees.size(),
        "The CROSS JOIN should expose each employee exactly once per department");
    Assertions.assertEquals(expectedDepartments, departments,
        "The CROSS JOIN should expose all departments from the lookup table");
    Assertions.assertEquals(employeeCount * departments.size(), rowCount,
        "Explicit CROSS JOIN should produce the cartesian product of both tables");
  }

  /**
   * Verify that comma-separated table references behave as an implicit
   * {@code CROSS JOIN}.
   */
  @Test
  void implicitCrossJoinViaCommaSeparatedTables() {
    String sql = """
        SELECT e.id AS employee_id, d.department AS department_name
        FROM employees e, departments d
        ORDER BY e.id, d.id
        """;

    int employeeCount = countRows("employees");
    Set<Integer> employees = new HashSet<>();
    Set<String> departments = new HashSet<>();
    int rowCount = executeAndCollect(sql, employees, departments);

    Set<String> expectedDepartments = Set.of("IT", "HR", "Sales");

    Assertions.assertEquals(employeeCount, employees.size(), "Implicit CROSS JOIN should still include all employees");
    Assertions.assertEquals(expectedDepartments, departments,
        "Implicit CROSS JOIN should still include all departments");
    Assertions.assertEquals(employeeCount * departments.size(), rowCount,
        "Implicit CROSS JOIN should produce the cartesian product of both tables");
  }

  /**
   * Execute a query and collect distinct employee and department values for
   * assertions.
   *
   * @param sql
   *          the SQL statement to execute
   * @param employees
   *          receiver for discovered employee identifiers
   * @param departments
   *          receiver for discovered department names
   * @return the number of rows produced by the query
   */
  private int executeAndCollect(String sql, Set<Integer> employees, Set<String> departments) {
    final int[] count = {
        0
    };
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          employees.add(rs.getInt("employee_id"));
          departments.add(rs.getString("department_name"));
          count[0]++;
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
    return count[0];
  }

  /**
   * Count the number of rows available in the supplied table.
   *
   * @param table
   *          the table whose rows should be counted
   * @return the number of rows present in the table
   */
  private int countRows(String table) {
    final int[] count = {
        0
    };
    jparqSql.query("SELECT COUNT(*) AS cnt FROM " + table, rs -> {
      try {
        if (rs.next()) {
          count[0] = rs.getInt("cnt");
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
    return count[0];
  }
}
