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
}
