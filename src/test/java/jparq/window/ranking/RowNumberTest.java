package jparq.window.ranking;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jparq.WhereTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering the ROW_NUMBER ranking window function.
 */
public class RowNumberTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = WhereTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  @Test
  void testRowNumberGlobalOrdering() {
    String sql = """
        SELECT first_name, last_name,
               ROW_NUMBER() OVER(ORDER BY last_name, first_name) AS name_row_number
        FROM employees
        ORDER BY last_name, first_name
        """;

    List<Long> rowNumbers = new ArrayList<>();
    List<String> names = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          long rn = rs.getLong("name_row_number");
          String firstName = rs.getString("first_name");
          String lastName = rs.getString("last_name");
          rowNumbers.add(rn);
          names.add(lastName + "," + firstName);
        }
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(rowNumbers.isEmpty(), "Expected at least one employee");
    for (int i = 0; i < rowNumbers.size(); i++) {
      Assertions.assertEquals(i + 1, rowNumbers.get(i), "Row numbers must be sequential");
    }
    List<String> sortedNames = new ArrayList<>(names);
    Collections.sort(sortedNames);
    Assertions.assertEquals(sortedNames, names, "Rows must be ordered by last name then first name");
  }

  @Test
  void testPartitionedRowNumberLatestSalaryChangePerEmployee() {
    Map<Integer, LocalDate> expected = new HashMap<>();
    jparqSql.query("SELECT employee, change_date FROM salary", rs -> {
      try {
        while (rs.next()) {
          int employeeId = rs.getInt(1);
          Date changeDate = rs.getDate(2);
          LocalDate candidate = changeDate == null ? null : changeDate.toLocalDate();
          LocalDate existing = expected.get(employeeId);
          if (existing == null || (candidate != null && candidate.isAfter(existing))) {
            expected.put(employeeId, candidate);
          }
        }
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });
    Assertions.assertFalse(expected.isEmpty(), "Expected salary change information per employee");

    String sql = """
        WITH RankedChanges AS (
            SELECT s.employee, s.change_date,
                   ROW_NUMBER() OVER(
                       PARTITION BY s.employee
                       ORDER BY s.change_date DESC, s.id DESC
                   ) AS change_rank
            FROM salary s
        )
        SELECT employee, change_date
        FROM RankedChanges
        WHERE change_rank = 1
        ORDER BY employee
        """;

    Map<Integer, LocalDate> actual = new HashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int employeeId = rs.getInt(1);
          Date changeDate = rs.getDate(2);
          actual.put(employeeId, changeDate == null ? null : changeDate.toLocalDate());
        }
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(expected.size(), actual.size(), "Expected one latest change per employee");
    for (Map.Entry<Integer, LocalDate> entry : expected.entrySet()) {
      Assertions.assertEquals(entry.getValue(), actual.get(entry.getKey()),
          "Latest change date must match aggregate for employee " + entry.getKey());
    }
  }

  @Test
  void testTopSalariesPerDepartment() {
    Map<Integer, Double> maxSalary = new HashMap<>();
    String maxSql = """
        SELECT ed.department, s.salary
        FROM employee_department ed
        JOIN salary s ON ed.employee = s.employee
        """;
    jparqSql.query(maxSql, rs -> {
      try {
        while (rs.next()) {
          int department = rs.getInt(1);
          final double salary = rs.getDouble(2);
          Double existing = maxSalary.get(department);
          if (existing == null || salary > existing) {
            maxSalary.put(department, salary);
          }
        }
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });
    Assertions.assertFalse(maxSalary.isEmpty(), "Expected salary information per department");

    String sql = """
        WITH RankedSalaries AS (
            SELECT ed.department, e.first_name, e.last_name, s.salary, s.change_date,
                   ROW_NUMBER() OVER(
                       PARTITION BY ed.department
                       ORDER BY s.salary DESC, s.change_date DESC, s.id DESC
                   ) AS department_rank
            FROM employee_department ed
            JOIN employees e ON e.id = ed.employee
            JOIN salary s ON s.employee = ed.employee
        )
        SELECT department, first_name, last_name, salary, department_rank
        FROM RankedSalaries
        WHERE department_rank <= 3
        ORDER BY department, department_rank, last_name, first_name
        """;

    Map<Integer, Integer> counts = new HashMap<>();
    Map<Integer, Double> observedMax = new HashMap<>();
    Map<Integer, Long> lastRank = new HashMap<>();
    Map<Integer, Double> previousSalary = new HashMap<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          int department = rs.getInt(1);
          final double salary = rs.getDouble(4);
          final long rank = rs.getLong(5);

          Assertions.assertTrue(rank >= 1 && rank <= 3,
              "Row number must be within requested top three per department");

          long expectedRank = lastRank.getOrDefault(department, 0L) + 1L;
          Assertions.assertEquals(expectedRank, rank,
              "Row numbers must be sequential within each department partition");
          lastRank.put(department, rank);

          double previous = previousSalary.getOrDefault(department, Double.POSITIVE_INFINITY);
          Assertions.assertTrue(salary <= previous + 0.000001,
              "Salaries must be non-increasing within each department partition");
          previousSalary.put(department, salary);

          counts.merge(department, 1, Integer::sum);
          if (rank == 1L) {
            observedMax.put(department, salary);
          }
        }
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertFalse(counts.isEmpty(), "Expected ranked salary rows");
    for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
      Assertions.assertTrue(entry.getValue() <= 3, "At most three rows per department should be returned");
    }
    Assertions.assertEquals(maxSalary.keySet(), observedMax.keySet(),
        "Each department should yield a maximum salary entry");
    for (Map.Entry<Integer, Double> entry : maxSalary.entrySet()) {
      Assertions.assertEquals(entry.getValue(), observedMax.get(entry.getKey()), 0.000001,
          "Highest salary must match the aggregate maximum for department " + entry.getKey());
    }
  }

  @Test
  void testAliasQualifiedWhereClausePreserved() {
    List<Integer> expectedIds = new ArrayList<>();
    jparqSql.query("SELECT MIN(id) AS min_id FROM employees", rs -> {
      try {
        while (rs.next()) {
          expectedIds.add(rs.getInt(1));
        }
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });
    Assertions.assertFalse(expectedIds.isEmpty(), "Expected at least one employee identifier");
    int targetId = expectedIds.getFirst();

    String sql = "SELECT e.id FROM employees e WHERE e.id = " + targetId;
    List<Integer> filtered = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          filtered.add(rs.getInt(1));
        }
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(1, filtered.size(), "Alias-qualified WHERE clause must restrict the result set");
    Assertions.assertEquals(targetId, filtered.getFirst(),
        "Alias-qualified predicate must retain the requested employee identifier");
  }
}
