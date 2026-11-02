package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies that qualified column references name a visible range variable: a
 * qualifier that does not name any table (or alias) in the current scope must
 * raise a syntax error (SQLState 42000) instead of silently resolving against
 * the base table.
 */
class UnknownQualifierTest {

  /**
   * Rejects unknown qualifiers in single-table and join queries, for both
   * predicate and projection pushdown configurations.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if JDBC setup fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rejectsUnknownTableQualifiers(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (Connection connection = DriverManager.getConnection(url)) {
      for (String sql : List.of(
          // single-table queries: unknown qualifier resolves silently today
          "SELECT y.id FROM acme.employees x", "SELECT y.id FROM (SELECT id FROM acme.employees) x",
          "SELECT id FROM acme.employees x WHERE y.id = 1", "SELECT id FROM acme.employees x GROUP BY id, y.id",
          "SELECT id FROM acme.employees x ORDER BY y.id",
          // a table alias hides the table name
          "SELECT employees.id FROM acme.employees e", "SELECT id FROM acme.employees e WHERE employees.id = 1",
          // join queries outside the ON clause
          "SELECT e.first_name FROM acme.employees e JOIN acme.employee_department ed ON e.id = ed.employee "
              + "WHERE d.id = 1",
          "SELECT nosuch.cyl FROM mtcars m JOIN (SELECT 1 AS k) j ON TRUE LIMIT 1",
          "SELECT x.a FROM (SELECT 1 AS a) d JOIN (VALUES (2)) j(k) ON TRUE",
          "SELECT a FROM (SELECT 1 AS a) d JOIN (VALUES (2)) j(k) ON TRUE GROUP BY d.a, y.a",
          "SELECT a FROM (SELECT 1 AS a) d JOIN (VALUES (2)) j(k) ON TRUE ORDER BY y.a",
          // DISTINCT ON, named WINDOW, and QUALIFY expressions (#261 review)
          "SELECT DISTINCT ON (y.id) id FROM acme.employees x",
          "SELECT id, ROW_NUMBER() OVER w FROM acme.employees x WINDOW w AS (ORDER BY y.id)",
          "SELECT x.id FROM acme.employees x JOIN acme.employee_department ed ON x.id = ed.employee "
              + "QUALIFY ROW_NUMBER() OVER (ORDER BY y.id) = 1",
          // join WHERE conjuncts with unknown qualifiers (#293)
          "SELECT COUNT(*) FROM mtcars m JOIN (SELECT 1 AS a) x ON TRUE WHERE zz.cyl = 4",
          "SELECT COUNT(*) FROM mtcars m JOIN (SELECT 1 AS a) x ON TRUE WHERE m.cyl = 4 AND zz.cyl = 99",
          "SELECT COUNT(*) FROM mtcars m JOIN (SELECT 1 AS a) x ON TRUE WHERE \"M\".cyl = 4",
          // self-join through the real table name, hidden by both aliases (#295)
          "SELECT mtcars.cyl FROM mtcars a JOIN mtcars b ON a.model = b.model LIMIT 1")) {
        SQLException failure = assertThrows(SQLException.class, () -> execute(connection, sql), sql);
        assertEquals("42000", failure.getSQLState(), sql + ": " + failure);
        assertTrue(failure.getMessage().contains("Unknown table qualifier"), failure::getMessage);
      }
      // Unknown qualifiers in HAVING are rejected as well; grouped HAVING
      // validation may report the missing output column before the qualifier
      // visibility check runs.
      for (String sql : List.of("SELECT COUNT(*) AS c FROM acme.employees x HAVING y.c > 0",
          "SELECT COUNT(*) AS c FROM (SELECT 1 AS a) d JOIN (VALUES (2)) j(k) ON TRUE HAVING y.c > 0")) {
        SQLException failure = assertThrows(SQLException.class, () -> execute(connection, sql), sql);
        assertEquals("42000", failure.getSQLState(), sql + ": " + failure);
      }
    }
  }

  /**
   * Accepts references to every visible qualifier form: table names, aliases,
   * derived tables, and unaliased parenthesized join groups.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void acceptsVisibleQualifiers() throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
    try (Connection connection = DriverManager.getConnection(url)) {
      assertTrue(execute(connection, "SELECT x.id FROM acme.employees x") > 0);
      assertTrue(execute(connection, "SELECT employees.id FROM acme.employees") > 0);
      assertTrue(execute(connection, "SELECT x.id FROM (SELECT id FROM acme.employees) x") > 0);
      assertTrue(execute(connection, "SELECT a.id FROM acme.employees a JOIN acme.employees b ON a.id = b.id") > 0);
      assertTrue(execute(connection,
          "SELECT e.id FROM (acme.employees e JOIN acme.employee_department ed ON e.id = ed.employee)") > 0);
      assertTrue(execute(connection, "SELECT T.a FROM (SELECT 1 AS a) t") > 0);
    }
  }

  /**
   * Keeps correlated subqueries working: outer qualifiers remain visible inside
   * EXISTS and scalar subqueries while unknown qualifiers are still rejected.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void preservesCorrelatedSubqueries() throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
    try (Connection connection = DriverManager.getConnection(url)) {
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT COUNT(*) AS c FROM acme.employees e "
              + "WHERE EXISTS (SELECT 1 FROM acme.employee_department ed WHERE ed.employee = e.id)")) {
        assertTrue(result.next());
        assertTrue(result.getLong(1) > 0);
      }
      SQLException failure = assertThrows(SQLException.class,
          () -> execute(connection, "SELECT COUNT(*) AS c FROM acme.employees e "
              + "WHERE EXISTS (SELECT 1 FROM acme.employee_department ed WHERE y.employee = e.id)"));
      assertEquals("42000", failure.getSQLState(), failure::getMessage);
      assertTrue(failure.getMessage().contains("Unknown table qualifier"), failure::getMessage);
    }
  }

  /**
   * Keeps LATERAL correlation working when an outer column flows through a nested
   * derived table, CTE, or join inside the LATERAL body. The outer reference is
   * bound after correlation rewriting, so the eager parse must not reject it.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void preservesNestedLateralCorrelation() throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
    try (Connection connection = DriverManager.getConnection(url)) {
      assertTrue(execute(connection,
          "SELECT e.id, l.c FROM acme.employees e CROSS JOIN LATERAL "
              + "(SELECT d.c FROM (SELECT COUNT(*) AS c FROM acme.employee_department ed "
              + "WHERE ed.employee = e.id) d) l") > 0);
      assertTrue(execute(connection,
          "SELECT e.id, l.c FROM acme.employees e CROSS JOIN LATERAL "
              + "(WITH w AS (SELECT ed.employee AS c FROM acme.employee_department ed "
              + "WHERE ed.employee = e.id) SELECT w.c FROM w) l") > 0);
      assertTrue(execute(connection, "SELECT e.id, l.c FROM acme.employees e CROSS JOIN LATERAL "
          + "(SELECT d.c FROM acme.departments x JOIN "
          + "(SELECT ed.employee AS c FROM acme.employee_department ed WHERE ed.employee = e.id) d ON TRUE) l") > 0);
      // Unknown qualifiers inside a LATERAL body are still rejected.
      SQLException failure = assertThrows(SQLException.class,
          () -> execute(connection,
              "SELECT e.id, l.c FROM acme.employees e CROSS JOIN LATERAL "
                  + "(SELECT d.c FROM (SELECT COUNT(*) AS c FROM acme.employee_department ed "
                  + "WHERE ed.employee = y.id) d) l"));
      assertEquals("42000", failure.getSQLState(), failure::getMessage);
      assertTrue(failure.getMessage().contains("Unknown table qualifier"), failure::getMessage);
    }
  }

  private static int execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
      assertNotNull(result, "ResultSet should be created for " + sql);
      int rows = 0;
      while (result.next()) {
        rows++;
      }
      return rows;
    }
  }
}
