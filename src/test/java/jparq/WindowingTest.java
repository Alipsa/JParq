package jparq;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
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
   * H2 equivalent: SELECT e."first_name", e."last_name", s."salary" FROM
   * employees e JOIN ( SELECT "employee", "salary", ROW_NUMBER() OVER ( PARTITION
   * BY "employee" ORDER BY "change_date" DESC, "id" DESC ) AS rn FROM salary ) s
   * ON s."employee" = e."id" WHERE s.rn = 1
   *
   */
  // @Test
  void testLatestSalaryPerEmployee() {
    String sql = """
        SELECT e.first_name, e.last_name, s.salary
        FROM employees e
        JOIN (
          SELECT employee, salary,
          ROW_NUMBER() OVER (
            PARTITION BY "employee"
            ORDER BY "change_date" DESC, "id" DESC
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
}
