package jparq;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import se.alipsa.jparq.JParqSql;

public class JoinTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = WhereTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");

    Path acmePath = Paths.get(acmesUrl.toURI());

    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  void testSimpleJoin() {
    String sql = "select first_name, last_name, salary from employees join salary on salary.employees";
  }

  void testJoinGroupByAvg() {
    String sql = """
        select d.department, avg(salary) from 
        employees as e
        join salary as s on e.id = s.employee
        join employee_department as ed
        on e.id = ed.employee
        join departments as d on ed.department = d.id 
        group by d.department
        """;
  }
}
