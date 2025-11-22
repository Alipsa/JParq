package se.alipsa.jparq.standards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import jparq.WhereTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.JParqSql;

public class SubqueryCorrelatedFiltersIsolatorTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL url = WhereTest.class.getResource("/acme");
    Assertions.assertNotNull(url, "acme must be on the test classpath (src/test/resources)");

    Path dir = Paths.get(url.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Expected output
   * 
   * <pre>
   * employee   department
   *        2            1
   *        4            3
   *        5            3
   * </pre>
   */
  @Test
  void testCteAndJoin() {
    jparqSql.query("""
          WITH high_salary AS (
            SELECT DISTINCT employee
            FROM salary
            WHERE salary >= 180000.0
          )
          SELECT ed.employee AS employee_id, ed.department AS department_id
          FROM employee_department ed
          JOIN high_salary hs ON hs.employee = ed.employee
          ORDER BY employee_id;
        """, rs -> {
      try {
        List<String> rows = new ArrayList<>();
        while (rs.next()) {
          int employeeId = rs.getInt("employee_id");
          int departmentId = rs.getInt("department_id");
          rows.add(employeeId + ":" + departmentId);
        }
        assertEquals(List.of("2:1", "4:3", "5:3"), rows, "Expected rows ordered by employee_id alias, got " + rows);
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  /**
   * Expected result:
   * 
   * <pre>
   *   DEPARTMENT_NAME
   *   IT
   *   Sales
   *   Sales
   * </pre>
   */
  @Test
  @Disabled("Temporarily disabled pending correlated subquery fix")
  void testDepartmentNameSubquery() {
    jparqSql.query("""
          WITH high_salary AS (
            SELECT DISTINCT employee
            FROM salary
            WHERE salary >= 180000.0
          )
          SELECT ed.employee AS employee_id, ed.department AS department_id
          FROM employee_department ed
          JOIN high_salary hs ON hs.employee = ed.employee
          ORDER BY employee_id;
        """, rs -> {
      try {
        List<String> derivedRows = new ArrayList<>();
        while (rs.next()) {
          derivedRows.add(rs.getInt("employee_id") + ":" + rs.getInt("department_id"));
        }
        assertEquals(List.of("2:1", "4:3", "5:3"), derivedRows,
            "Derived inline view should expose expected employee/department pairs");
      } catch (SQLException e) {
        fail(e);
      }
    });

    jparqSql.query("""
          WITH high_salary AS (
            SELECT DISTINCT employee
            FROM salary
            WHERE salary >= 180000.0
          )
          SELECT (SELECT d.department FROM departments d WHERE d.id = derived.department_id) AS department_name
          FROM (
            SELECT ed.employee AS employee_id, ed.department AS department_id
            FROM employee_department ed
            JOIN high_salary hs ON hs.employee = ed.employee
          ) AS derived
          ORDER BY department_name;
        """, rs -> {
      try {
        Field mappingField = JParqResultSet.class.getDeclaredField("qualifierColumnMapping");
        mappingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> qualifierMapping = (Map<String, Map<String, String>>) mappingField.get(rs);
        Map<String, String> derivedMapping = qualifierMapping.get("derived");
        Assertions.assertNotNull(derivedMapping, "Derived mapping must be present for correlation");
        Assertions.assertTrue(derivedMapping.containsKey("department_id"),
            "Derived mapping should expose department_id but was " + derivedMapping.keySet());
        Assertions.assertEquals("department_id", derivedMapping.get("department_id"),
            "department_id should resolve to the derived inline view field");
        Field qualifiersField = JParqResultSet.class.getDeclaredField("queryQualifiers");
        qualifiersField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> qualifiers = (List<String>) qualifiersField.get(rs);
        Assertions.assertTrue(qualifiers.contains("derived"), "Derived alias missing from qualifiers: " + qualifiers);
        List<String> departments = new ArrayList<>();
        while (rs.next()) {
          departments.add(rs.getString("department_name"));
        }
        assertEquals(List.of("IT", "Sales", "Sales"), departments,
            "Expected department names ordered by derived inline view correlation");
      } catch (SQLException | NoSuchFieldException | IllegalAccessException e) {
        fail(e);
      }
    });
  }

  @Disabled
  @Test
  void testSalaryChangeCountSubquery() {
    jparqSql.query("""
          WITH high_salary AS (
            SELECT DISTINCT employee
            FROM salary
            WHERE salary >= 180000.0
          )
          SELECT (SELECT COUNT(*) FROM salary s WHERE s.employee = derived.employee_id) AS salary_change_count
          FROM (
            SELECT ed.employee AS employee_id, ed.department AS department_id
            FROM employee_department ed
            JOIN high_salary hs ON hs.employee = ed.employee
          ) AS derived
          ORDER BY salary_change_count;
        """, rs -> {
      try {
        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(3, rows, "Expected 3 rows from the salary change count subquery, got " + rows);
      } catch (SQLException e) {
        fail(e);
      }
    });
  }
}
