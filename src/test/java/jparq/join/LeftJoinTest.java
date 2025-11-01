package jparq.join;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering explicit {@code LEFT [OUTER] JOIN} syntax using
 * {@code JOIN ... ON ...} in the {@code FROM} clause.
 */
class LeftJoinTest {

  private static final double DELTA = 0.000001d;
  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = LeftJoinTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Verify that a {@code LEFT JOIN} returns every employee even when no salary
   * entry exists.
   */
  @Test
  void leftJoinIncludesRowsWithoutMatches() {
    Set<Integer> employeeIds = fetchEmployeeIds();

    String sql = """
        SELECT e.id, e.first_name, e.last_name, s.salary
        FROM employees e
        LEFT JOIN salary s ON e.id = s.employee AND s.salary > 200000
        ORDER BY e.id
        """;

    Map<String, Double> salaries = new HashMap<>();
    Set<Integer> observedIds = new LinkedHashSet<>();
    AtomicBoolean foundNullSalary = new AtomicBoolean(false);
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int employeeId = rs.getInt("id");
          observedIds.add(employeeId);
          String key = rs.getString("first_name") + " " + rs.getString("last_name");
          double salary = rs.getDouble("salary");
          if (rs.wasNull()) {
            foundNullSalary.set(true);
          } else {
            salaries.put(key, salary);
          }
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(employeeIds, observedIds,
        "Left join should include each employee identifier at least once");
    Assertions.assertTrue(foundNullSalary.get(), "At least one employee should lack a salary entry");
    Assertions.assertEquals(230000.0d, salaries.get("Sixten Svensson"), DELTA,
        "Known salaries should remain available for high earners");
  }

  /**
   * Ensure the optional {@code OUTER} keyword is accepted when performing a
   * {@code LEFT OUTER JOIN}.
   */
  @Test
  void leftOuterJoinKeywordIsOptional() {
    Set<Integer> employeeIds = fetchEmployeeIds();

    String sql = """
        SELECT e.id, s.salary
        FROM employees e
        LEFT OUTER JOIN salary s ON e.id = s.employee AND s.salary > 200000
        ORDER BY e.id
        """;

    Set<Integer> observedIds = new LinkedHashSet<>();
    AtomicBoolean foundNullSalary = new AtomicBoolean(false);
    List<Double> salaries = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          observedIds.add(rs.getInt("id"));
          Object value = rs.getObject("salary");
          if (value == null) {
            foundNullSalary.set(true);
          } else {
            salaries.add(((Number) value).doubleValue());
          }
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(employeeIds, observedIds, "Left outer join should produce rows for every employee");
    Assertions.assertTrue(foundNullSalary.get(), "Optional OUTER keyword should still allow unmatched rows");
    Assertions.assertFalse(salaries.isEmpty(), "Matched salary rows should still be returned");
  }

  private static Set<Integer> fetchEmployeeIds() {
    Set<Integer> ids = new LinkedHashSet<>();
    jparqSql.query("SELECT id FROM employees ORDER BY id", rs -> {
      try {
        while (rs.next()) {
          ids.add(rs.getInt(1));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
    Assertions.assertFalse(ids.isEmpty(), "Employees table should not be empty");
    return ids;
  }
}
