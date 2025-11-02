package jparq.join;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import jparq.WhereTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class JoinTest {

  static JParqSql jparqSql;
  double delta = 0.000001;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = WhereTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");

    Path acmePath = Paths.get(acmesUrl.toURI());

    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * H2 equivalent: <code><pre>
   * select "first_name", "last_name" , "salary"
   * from employees E
   * join salary S on S."employee" = E."id"
   * </pre></code>
   */
  @Test
  void testSimpleJoin() {
    String sql = """
        select first_name, last_name, salary
        from employees as e join salary as s on e.id = s.employee
        """;

    jparqSql.query(sql, rs -> {
      try {
        int rowCount = 0;
        while (rs.next()) {
          String firstName = rs.getString("first_name");
          String lastName = rs.getString("last_name");
          double salary = rs.getDouble("salary");
          // System.out.printf("%s %s: %.2f%n", firstName, lastName, salary);
          rowCount++;
        }
        Assertions.assertEquals(8, rowCount, "Expected 8 rows from join");
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });
  }

  /**
   * H2 equivalent: <code><pre>
   * SELECT e."first_name", e."last_name", s."salary"
   * FROM employees e
   * JOIN (
   *   SELECT "employee", MAX("change_date") AS max_cd
   *   FROM salary
   *   GROUP BY "employee"
   * ) m ON m."employee" = e."id"
   * JOIN salary s
   *   ON s."employee" = m."employee"
   *  AND s."change_date" = m.max_cd
   *  </pre></code>
   */
  @Test
  void testLatestSalaryPerEmployee() {
    String sql = """
        SELECT e.first_name, e.last_name, s.salary
        FROM employees e
        JOIN (
          SELECT employee, MAX(change_date) AS max_cd
          FROM salary
          GROUP BY "employee"
        ) m ON m.employee = e.id
        JOIN salary s
          ON s.employee = m.employee
         AND s.change_date = m.max_cd
        """;
    Map<String, Double> salaries = new HashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        int rowCount = 0;
        while (rs.next()) {
          String firstName = rs.getString("first_name");
          String lastName = rs.getString("last_name");
          double salary = rs.getDouble("salary");
          salaries.put(firstName + " " + lastName, salary);
          // System.out.printf("%s %s: %.2f%n", firstName, lastName, salary);
          rowCount++;
        }
        Assertions.assertEquals(6, rowCount, "Expected 6 rows from join");
        Assertions.assertEquals(180000.0, salaries.get("Karin Pettersson"), delta, "Karin Pettersson's latest salary");
        Assertions.assertEquals(165000.0, salaries.get("Per Andersson"), delta, "Per Andersson's latest salary");
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });
  }

  /**
   * H2 equivalent: <code><pre>
   * select d."department", avg(s."salary") as avg_salary from
   * employees e
   * join salary s on e."id" = s."employee"
   * join employee_department ed
   * on e."id" = ed."employee"
   * join departments d on ed."department" = d."id"
   * group by d."department"
   * </pre></code>
   */
  @Test
  void testJoinGroupByAvg() {
    String sql = """
        select d.department, avg(s.salary) as avg_salary from
        employees e
        join salary s on e.id = s.employee
        join employee_department ed
        on e.id = ed.employee
        join departments d on ed.department = d.id
        group by d.department
        """;

    Map<String, Double> salaries = new HashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        int rowCount = 0;
        while (rs.next()) {
          String department = rs.getString("department");
          double savgSalary = rs.getDouble("avg_salary");
          salaries.put(department, savgSalary);
          rowCount++;
        }
        Assertions.assertEquals(3, rowCount, "Expected 3 rows from join");
        Assertions.assertEquals(135000.0, salaries.get("HR"), delta, "HR's average salary");
        Assertions.assertEquals(163750.0, salaries.get("IT"), delta, "IT's average salary");
        Assertions.assertEquals(212500.0, salaries.get("Sales"), delta, "Sales's average salary");
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });
  }
}
