package jparq.usage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class AcmeTest {
  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = AcmeTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Expected result: id first_name last_name salary 2 Karin Pettersson 180000 4
   * Arne Larsson 195000 5 Sixten Svensson 230000 3 Tage Lundström 140000 1 Per
   * Andersson 165000
   *
   * <p>
   * However, on my Mac the result of this is:
   * e__id	first_name	last_name	salary
   * 1	[B@2ba21449	[B@1c5b9581	165000
   * 2	[B@6aa6e512	[B@519da453	180000
   * 3	[B@11aad47c	[B@7dfee05a	140000
   * 4	[B@20ad0482	[B@1c2e83f8	195000
   * 5	[B@5b60bd35	[B@51967a33	230000
   */
  @Test
  void testClassicSubQuery() {
    String sql = """
        select e.*, s.salary from employees e join salary s on e.id = s.employee
        WHERE
            s.change_date = (
                SELECT
                    MAX(s2.change_date)
                FROM
                    salary s2
                WHERE
                    s2.employee = s.employee
            );
        """;
    List<Integer> ids = new ArrayList<>();
    Map<String, Double> salaries = new LinkedHashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          Integer id = rs.getInt("id");
          ids.add(id);
          String firstName = rs.getString("first_name");
          String lastName = rs.getString("last_name");
          Double salary = rs.getDouble("salary");
          System.out.printf("%s %s %s %s%n", id, firstName, lastName, salary);
          salaries.put(firstName + " " + lastName, salary);
        }
        assertEquals(5, ids.size(), "Expected 5 distinct employee ids");
        assertEquals(180000, salaries.get("Karin Pettersson"));
        assertEquals(195000, salaries.get("Arne Larsson"));
        assertEquals(230000, salaries.get("Sixten Svensson"));
        assertEquals(140000, salaries.get("Tage Lundström"));
        assertEquals(165000, salaries.get("Per Andersson"));
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });
  }
}
