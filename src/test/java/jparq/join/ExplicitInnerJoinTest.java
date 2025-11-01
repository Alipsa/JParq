package jparq.join;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering explicit {@code INNER JOIN} syntax using
 * {@code JOIN ... ON ...} in the {@code FROM} clause.
 */
class ExplicitInnerJoinTest {

  private static final double DELTA = 0.000001d;
  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = ExplicitInnerJoinTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Verify that rows from the salary table are matched with the corresponding
   * employee when using an explicit {@code INNER JOIN}.
   */
  @Test
  void explicitInnerJoinMatchesRows() {
    String sql = """
        SELECT e.first_name, e.last_name, s.salary
        FROM employees e
        INNER JOIN salary s ON e.id = s.employee
        """;

    Map<String, Double> salaries = new HashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        int rowCount = 0;
        while (rs.next()) {
          String key = rs.getString("first_name") + " " + rs.getString("last_name");
          double salary = rs.getDouble("salary");
          salaries.put(key, salary);
          rowCount++;
        }
        Assertions.assertEquals(8, rowCount, "Expected eight matching salary rows");
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(180000.0d, salaries.get("Karin Pettersson"), DELTA,
        "Karin Pettersson's salary should be available after the join");
    Assertions.assertEquals(230000.0d, salaries.get("Sixten Svensson"), DELTA,
        "Sixten Svensson's salary should be available after the join");
  }

  /**
   * Ensure duplicate column names from different tables map to canonical join
   * fields and remain readable via the JDBC {@link java.sql.ResultSet} API.
   */
  @Test
  void explicitJoinHandlesDuplicateColumnNames() {
    String sql = """
        SELECT e.id, d.id
        FROM employees e
        INNER JOIN employee_department ed ON e.id = ed.employee
        INNER JOIN departments d ON ed.department = d.id
        ORDER BY e.id
        """;

    List<Integer> employeeIds = new ArrayList<>();
    List<Integer> departmentIds = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          employeeIds.add(rs.getInt(1));
          departmentIds.add(rs.getInt(2));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(employeeIds.isEmpty(), "Joined employee identifiers should be available");
    Assertions.assertEquals(employeeIds.size(), departmentIds.size(),
        "Each employee row should have a matching department identifier");
  }
}
