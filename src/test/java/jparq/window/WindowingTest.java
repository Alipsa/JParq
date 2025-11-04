package jparq.window;

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

public class WindowingTest {

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
   * This query is a common and standard SQL pattern used to find the
   * greatest-n-per-group. In this case, get the single, most recent salary for
   * each employee.
   *
   * <p>
   * H2 equivalent: <code><pre>
   *   SELECT e."first_name", e."last_name", s."salary"
   *   FROM
   *   employees e
   *   JOIN (
   *     SELECT "employee",
   *     "salary",
   *     ROW_NUMBER() OVER (
   *       PARTITION BY "employee" ORDER BY "change_date" DESC, "id" DESC
   *     ) AS rn
   *     FROM salary
   *   ) s ON s."employee" = e."id"
   *   WHERE s.rn = 1
   * </pre></code> The expected result is
   * <table>
   * <thead>
   * <tr>
   * <th>first_name</th>
   * <th>last_name</th>
   * <th>salary</th>
   * </tr>
   * </thead> <tbody>
   * <tr>
   * <td>Karin</td>
   * <td>Pettersson</td>
   * <td>180000</td>
   * </tr>
   * <tr>
   * <td>Arne</td>
   * <td>Larsson</td>
   * <td>195000</td>
   * </tr>
   * <tr>
   * <td>Sixten</td>
   * <td>Svensson</td>
   * <td>230000</td>
   * </tr>
   * <tr>
   * <td>Tage</td>
   * <td>Lundström</td>
   * <td>140000</td>
   * </tr>
   * <tr>
   * <td>Per</td>
   * <td>Andersson</td>
   * <td>165000</td>
   * </tr>
   * </tbody>
   * </table>
   */
  @Test
  void testLatestSalaryPerEmployee() {
    String sql = """
        SELECT e.first_name, e.last_name, s.salary
        FROM employees e
        JOIN (
          SELECT employee, salary,
          ROW_NUMBER() OVER (
            PARTITION BY employee
            ORDER BY change_date DESC, id DESC
          ) AS rn
          FROM salary
        ) s ON s.employee = e.id
        WHERE s.rn = 1
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
          System.out.printf("%s %s: %.2f%n", firstName, lastName, salary);
          rowCount++;
        }
        Assertions.assertEquals(5, rowCount, "Expected 5 rows, one for each employee");
        Assertions.assertEquals(180000.0, salaries.get("Karin Pettersson"), delta, "Karin Pettersson's latest salary");
        Assertions.assertEquals(165000.0, salaries.get("Per Andersson"), delta, "Per Andersson's latest salary");
        Assertions.assertEquals(140000.0, salaries.get("Tage Lundström"), delta, "Tage Lundström's latest salary");
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });
  }

  /**
   * Verify that the window function correctly identifies the most recent salary
   * for each employee even when multiple changes share the same change date.
   */
  @Test
  void testRowNumberOrderingInSalaryTable() {
    String sql = """
        SELECT employee, salary,
          ROW_NUMBER() OVER (
            PARTITION BY employee
            ORDER BY change_date DESC, id DESC
          ) AS rn
        FROM salary
        ORDER BY employee, rn
        """;

    Map<Integer, Map<Long, Double>> salaryByEmployee = new HashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int employeeId = rs.getInt("employee");
          long rowNumber = rs.getLong("rn");
          double salary = rs.getDouble("salary");
          salaryByEmployee.computeIfAbsent(employeeId, ignored -> new HashMap<>()).put(rowNumber, salary);
        }
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(salaryByEmployee.isEmpty(), "Expected salary rows to be returned");
    Map<Integer, Double> expectedLatest = Map.of(
        1, 165000.0,
        2, 180000.0,
        3, 140000.0,
        4, 195000.0,
        5, 230000.0);

    expectedLatest.forEach((employeeId, expectedSalary) -> {
      Map<Long, Double> rows = salaryByEmployee.get(employeeId);
      Assertions.assertNotNull(rows, "Employee " + employeeId + " must be present in the result");
      Double latest = rows.get(1L);
      Assertions.assertNotNull(latest, "Employee " + employeeId + " must have a row number 1");
      Assertions.assertEquals(expectedSalary, latest, delta,
          "Employee " + employeeId + " latest salary should match");
    });
  }
}
