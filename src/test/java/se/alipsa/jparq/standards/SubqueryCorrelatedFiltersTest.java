package se.alipsa.jparq.standards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import jparq.WhereTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class SubqueryCorrelatedFiltersTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL url = WhereTest.class.getResource("/acme");
    Assertions.assertNotNull(url, "acme must be on the test classpath (src/test/resources)");

    Path dir = Paths.get(url.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testExists() {

    jparqSql.query("""
        SELECT
        ed.employee AS employee_id,
        ed.department AS department_id
        FROM employee_department ed

        WHERE EXISTS (
          SELECT 1
          FROM salary s
          WHERE s.employee = ed.employee
            AND s.salary >= 180000.0
        )
        """, rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;
        while (rs.next()) {
          System.out.println(rs.getString(1));
          rows++;
        }
        assertEquals(3, rows, "Expected 3 rows, got " + rows);
      } catch (SQLException e) {
        // Convert SQLExceptions in the lambda to RuntimeExceptions
        fail(e);
      }
    });
  }

  @Disabled
  @Test
  void testSubqueryCorrelatedFilters() {

    jparqSql.query("""
        SELECT derived.employee_id,
                 (SELECT d.department FROM departments d WHERE d.id = derived.department_id) AS department_name,
                 (SELECT COUNT(*) FROM salary s WHERE s.employee = derived.employee_id) AS salary_change_count
        FROM (
          SELECT ed.employee AS employee_id, ed.department AS department_id
          FROM employee_department ed
        ) AS derived

        WHERE EXISTS (
          SELECT 1
          FROM salary s
          WHERE s.employee = derived.employee_id
            AND s.salary >= 180000.0
        )
        ORDER BY derived.employee_id;
        """, rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount(), "Expected 3 columns");

        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(3, rows, "Expected 3 rows, got " + rows);
      } catch (SQLException e) {
        // Convert SQLExceptions in the lambda to RuntimeExceptions
        fail(e);
      }
    });
  }

  @Disabled
  @Test
  void testSubqueryCorrelatedFiltersCTE() {

    jparqSql.query("""
          WITH high_salary AS (
            SELECT DISTINCT employee
            FROM salary
            WHERE salary >= 180000.0
          )
          SELECT derived.employee_id,
                 (SELECT d.department FROM departments d WHERE d.id = derived.department_id) AS department_name,
                 (SELECT COUNT(*) FROM salary s WHERE s.employee = derived.employee_id) AS salary_change_count
          FROM (
            SELECT ed.employee AS employee_id, ed.department AS department_id
            FROM employee_department ed
            JOIN high_salary hs ON hs.employee = ed.employee
          ) AS derived
          ORDER BY derived.employee_id;
        """, rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount(), "Expected 3 columns");

        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(3, rows, "Expected 3 rows, got " + rows);
      } catch (SQLException e) {
        // Convert SQLExceptions in the lambda to RuntimeExceptions
        fail(e);
      }
    });
  }

  @Disabled
  @Test
  void testSubqueryCorrelatedFiltersWithExist() {

    jparqSql.query("""
          WITH high_salary AS (
            SELECT DISTINCT employee
            FROM salary
            WHERE salary >= 180000.0
          )
          SELECT derived.employee_id,
                 (SELECT d.department FROM departments d WHERE d.id = derived.department_id) AS department_name,
                 (SELECT COUNT(*) FROM salary s WHERE s.employee = derived.employee_id) AS salary_change_count
          FROM (
            SELECT ed.employee AS employee_id, ed.department AS department_id
            FROM employee_department ed
            JOIN high_salary hs ON hs.employee = ed.employee
          ) AS derived
          WHERE EXISTS (
            SELECT 1
            FROM salary s
            WHERE s.employee = derived.employee_id
              AND s.salary >= 180000.0
          )
          ORDER BY derived.employee_id;
        """, rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount(), "Expected 3 columns");

        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(3, rows, "Expected 3 rows, got " + rows);
      } catch (SQLException e) {
        // Convert SQLExceptions in the lambda to RuntimeExceptions
        fail(e);
      }
    });
  }
}
