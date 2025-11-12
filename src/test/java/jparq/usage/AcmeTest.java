package jparq.usage;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import jparq.join.CrossJoinTest;
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
   * Current result: this is wrong (e_i_d, first_name and last_name, id 3 returned twice):
   * e__id	first_name	last_name	salary
   * 1	[B@343edb43	[B@22cd740d	165000
   * 2	[B@5c077437	[B@1d480945	180000
   * 3	[B@5f6e2d6c	[B@164a4790	130000
   * 3	[B@710b3fa2	[B@2c5412e7	140000
   * 4	[B@2ff5f4c	[B@464dfe44	195000
   * 5	[B@6e932f00	[B@62dccb1f	230000
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
  }
}
