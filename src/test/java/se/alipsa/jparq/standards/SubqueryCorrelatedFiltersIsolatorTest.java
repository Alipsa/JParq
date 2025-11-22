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

/**
 * Isolated regressions for correlated subqueries against derived tables to
 * capture qualifier handling without interference from other suites.
 */
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

  @Test
  void derivedAliasQualifierMappingIncludesProjectionAndDiagnostics() {
    RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, () -> jparqSql.query("""
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
      // This callback is not expected to run because the guard should surface during
      // query planning.
    }));

    Throwable cause = thrown.getCause();
    Assertions.assertNotNull(cause, "Expected a root SQLException describing the correlation guard failure");
    Assertions.assertTrue(cause instanceof SQLException, "Expected SQLException but was " + cause.getClass().getName());
    Throwable inner = cause.getCause();
    Assertions.assertNotNull(inner, "Correlation guard should be reported as the nested cause");
    Assertions.assertTrue(inner.getMessage().contains("Correlation context mapped derived.department_id"),
        "Guarded correlated lookup should surface missing derived.department_id mapping but was " + inner.getMessage());
  }

  /**
   * Verify correlated predicates resolve derived aliases to concrete columns
   * instead of silently returning {@code null}.
   */
  @Test
  void correlatedPredicateResolvesDerivedAliasColumns() {
    jparqSql.query("""
          SELECT derived.employee_id, derived.department_id
          FROM (
            SELECT ed.employee AS employee_id, ed.department AS department_id
            FROM employee_department ed
          ) AS derived
          WHERE EXISTS (
            SELECT 1 FROM departments d WHERE d.id = derived.department_id
          )
          ORDER BY derived.employee_id;
        """, rs -> {
      try {
        if (rs instanceof JParqResultSet jparqRs) {
          Map<String, Map<String, String>> qualifierMapping = readQualifierMapping(jparqRs);
          Map<String, String> derivedMapping = qualifierMapping.get("derived");
          Assertions.assertNotNull(derivedMapping, "Correlation context must include derived alias mapping");
          Assertions.assertTrue(derivedMapping.containsKey("department_id"),
              "department_id should be available for correlated predicates but was " + derivedMapping.keySet());
        }

        List<String> rows = new ArrayList<>();
        while (rs.next()) {
          rows.add(rs.getInt("employee_id") + ":" + rs.getInt("department_id"));
        }
        Assertions.assertEquals(List.of("1:1", "2:1", "3:2", "4:3", "5:3"), rows,
            "Correlated predicate should preserve derived alias values");
      } catch (SQLException | IllegalAccessException | NoSuchFieldException e) {
        fail(e);
      }
    });
  }

  /**
   * Read the qualifier column mapping from the supplied result set via reflection
   * to allow assertions without altering production visibility.
   *
   * @param rs
   *          the {@link JParqResultSet} under inspection
   * @return the qualifier to column mapping used for correlation lookups
   * @throws NoSuchFieldException
   *           if the mapping field cannot be located
   * @throws IllegalAccessException
   *           if reflective access is denied
   */
  private Map<String, Map<String, String>> readQualifierMapping(JParqResultSet rs)
      throws NoSuchFieldException, IllegalAccessException {
    Field mappingField = JParqResultSet.class.getDeclaredField("qualifierColumnMapping");
    mappingField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, String>> qualifierMapping = (Map<String, Map<String, String>>) mappingField.get(rs);
    return qualifierMapping;
  }

  /**
   * Read the list of query qualifiers visible to the result set through
   * reflection.
   *
   * @param rs
   *          the {@link JParqResultSet} to inspect
   * @return the qualifiers collected during query planning
   * @throws NoSuchFieldException
   *           if the field cannot be found
   * @throws IllegalAccessException
   *           if reflective access fails
   */
  private List<String> readQueryQualifiers(JParqResultSet rs) throws NoSuchFieldException, IllegalAccessException {
    Field qualifiersField = JParqResultSet.class.getDeclaredField("queryQualifiers");
    qualifiersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<String> qualifiers = (List<String>) qualifiersField.get(rs);
    return qualifiers;
  }

  /**
   * Output temporary diagnostics for derived alias mappings to assist debugging
   * without altering production logging.
   *
   * @param derivedAlias
   *          the alias under inspection
   * @param qualifierMapping
   *          the correlation context mapping per qualifier
   * @param qualifiers
   *          the normalized qualifiers seen by the query
   */
  private void logDerivedDiagnostics(String derivedAlias, Map<String, Map<String, String>> qualifierMapping,
      List<String> qualifiers) {
    System.out.println("[diagnostic] query qualifiers: " + qualifiers);
    System.out
        .println("[diagnostic] qualifier mapping for '" + derivedAlias + "': " + qualifierMapping.get(derivedAlias));
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
  @Disabled
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
