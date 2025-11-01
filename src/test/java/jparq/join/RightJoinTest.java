package jparq.join;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering explicit {@code RIGHT [OUTER] JOIN} syntax using
 * {@code JOIN ... ON ...} in the {@code FROM} clause.
 */
class RightJoinTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = RightJoinTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Verify that a {@code RIGHT JOIN} retains each row from the right-hand table
   * even when the join condition cannot produce a match.
   */
  @Test
  void rightJoinIncludesRowsWithoutMatches() {
    List<Integer> expectedEmployees = fetchSalaryEmployees();

    String sql = """
        SELECT e.id, s.employee
        FROM employees e
        RIGHT JOIN salary s ON e.id = s.employee AND 1 = 0
        ORDER BY s.employee
        """;

    List<Integer> observedEmployees = new ArrayList<>();
    AtomicBoolean sawNullLeft = new AtomicBoolean(false);
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int employeeId = rs.getInt("employee");
          Assertions.assertFalse(rs.wasNull(), "Salary identifier should never be NULL");
          observedEmployees.add(employeeId);
          Object leftId = rs.getObject("id");
          if (leftId == null) {
            sawNullLeft.set(true);
          } else {
            Assertions.fail("LEFT side should be NULL when the join condition is unsatisfied");
          }
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(observedEmployees.isEmpty(), "Right join should return the right table rows");
    Assertions.assertEquals(expectedEmployees, observedEmployees,
        "Right join must preserve the ordering and identifiers from the right table");
    Assertions.assertTrue(sawNullLeft.get(), "Right join should yield NULL values for the left table when unmatched");
  }

  /**
   * Ensure the optional {@code OUTER} keyword is accepted for a
   * {@code RIGHT OUTER JOIN} and that matching rows remain available.
   */
  @Test
  void rightOuterJoinKeywordIsOptional() {
    List<Integer> expectedEmployees = fetchSalaryEmployees();

    String sql = """
        SELECT e.id, s.employee
        FROM employees e
        RIGHT OUTER JOIN salary s ON e.id = s.employee
        ORDER BY s.employee
        """;

    List<Integer> observedEmployees = new ArrayList<>();
    AtomicBoolean sawMatchingLeft = new AtomicBoolean(false);
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int employeeId = rs.getInt("employee");
          Assertions.assertFalse(rs.wasNull(), "Salary identifier should never be NULL");
          observedEmployees.add(employeeId);
          if (rs.getObject("id") != null) {
            sawMatchingLeft.set(true);
          }
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(expectedEmployees, observedEmployees,
        "Right outer join should include every salary identifier");
    Assertions.assertTrue(sawMatchingLeft.get(), "At least one left-hand row should match the right table");
  }

  /**
   * Retrieve the employee identifiers present in the salary table in a stable
   * order.
   *
   * @return sorted list of employee identifiers backed by the salary table
   */
  private static List<Integer> fetchSalaryEmployees() {
    List<Integer> employees = new ArrayList<>();
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
