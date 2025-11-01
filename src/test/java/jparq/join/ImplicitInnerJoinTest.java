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
 * Tests implicit inner join syntax using comma separated tables combined with a
 * {@code WHERE} clause join condition.
 */
class ImplicitInnerJoinTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = ImplicitInnerJoinTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Ensure that the legacy comma based join syntax produces the same set of
   * matching rows as the explicit join variant.
   */
  @Test
  void implicitJoinProducesMatches() {
    String sql = """
        SELECT e.first_name, s.salary
        FROM employees e, salary s
        WHERE e.id = s.employee
        """;

    Set<String> employees = new HashSet<>();
    jparqSql.query(sql, rs -> {
      try {
        int rowCount = 0;
        while (rs.next()) {
          employees.add(rs.getString("first_name"));
          rowCount++;
        }
        Assertions.assertEquals(8, rowCount, "Implicit join should match eight rows");
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertTrue(employees.contains("Karin"), "Karin should be present in the implicit join result");
    Assertions.assertTrue(employees.contains("Per"), "Per should be present in the implicit join result");
  }
}
