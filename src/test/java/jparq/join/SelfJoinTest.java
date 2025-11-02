package jparq.join;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests covering explicit self joins where the same table appears
 * multiple times in the {@code FROM} clause using distinct aliases.
 */
class SelfJoinTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmeUrl = SelfJoinTest.class.getResource("/acme");
    Assertions.assertNotNull(acmeUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmeUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Verify that an explicit {@code INNER JOIN} can reference the same table twice
   * when distinct aliases are supplied.
   */
  @Test
  void explicitInnerSelfJoinMatchesRows() {
    List<Integer> employeeIds = fetchEmployeeIds();

    String sql = """
        SELECT e1.id AS employee_id, e1.first_name AS employee_first,
               e2.id AS mirror_id, e2.first_name AS mirror_first
        FROM employees e1
        INNER JOIN employees e2 ON e1.id = e2.id
        ORDER BY e1.id
        """;

    List<Integer> observedEmployees = new ArrayList<>();
    List<Integer> observedMirrorIds = new ArrayList<>();
    List<String> employeeNames = new ArrayList<>();
    List<String> mirrorNames = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          observedEmployees.add(rs.getInt("employee_id"));
          observedMirrorIds.add(rs.getInt("mirror_id"));
          employeeNames.add(rs.getString("employee_first"));
          mirrorNames.add(rs.getString("mirror_first"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(employeeIds.size(), observedEmployees.size(), "Self join should emit a row per employee");
    Assertions.assertEquals(observedEmployees, observedMirrorIds,
        "Joined identifiers should originate from both table aliases");
    Assertions.assertEquals(employeeNames, mirrorNames,
        "Column resolution must honor the alias used in the SELECT list");
  }

  /**
   * Perform a {@code LEFT JOIN} against the same table to ensure join conditions
   * can reference columns from both aliases and produce {@code NULL} values when
   * the predicate does not match.
   */
  @Test
  void leftSelfJoinSupportsNonMatchingPredicates() {
    List<Integer> employeeIds = fetchEmployeeIds();

    String sql = """
        SELECT e1.id AS employee_id, e2.id AS unmatched_id
        FROM employees e1
        LEFT JOIN employees e2 ON e1.id = e2.id + 1000
        ORDER BY e1.id
        """;

    Set<Integer> observedEmployees = new LinkedHashSet<>();
    List<Integer> matchedIds = new ArrayList<>();

    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          observedEmployees.add(rs.getInt("employee_id"));
          int value = rs.getInt("unmatched_id");
          if (!rs.wasNull()) {
            matchedIds.add(value);
          }
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(new LinkedHashSet<>(employeeIds), observedEmployees,
        "Left self join should include every row from the left-hand alias");
    Assertions.assertTrue(matchedIds.isEmpty(), "Join predicate should prevent matches and yield NULLs");
  }

  /**
   * Ensure that a self join works when the left-hand table is referenced without
   * an explicit alias while the right-hand table uses a distinct alias.
   */
  @Test
  void explicitSelfJoinAllowsUnaliasedBaseTable() {
    String sql = """
        SELECT employees.id AS base_id, e2.id AS mirror_id
        FROM employees
        INNER JOIN employees e2 ON employees.id = e2.id
        ORDER BY employees.id
        """;

    var parsed = se.alipsa.jparq.engine.SqlParser.parseSelect(sql);
    Assertions.assertEquals("employees", parsed.tableReferences().get(0).tableName());
    Assertions.assertNull(parsed.tableReferences().get(0).tableAlias());
    Assertions.assertEquals("employees", parsed.tableReferences().get(1).tableName());
    Assertions.assertEquals("e2", parsed.tableReferences().get(1).tableAlias());

    List<Integer> employeeIds = fetchEmployeeIds();

    List<Integer> observedBase = new ArrayList<>();
    List<Integer> observedMirror = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          observedBase.add(rs.getInt("base_id"));
          observedMirror.add(rs.getInt("mirror_id"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });

    Assertions.assertEquals(employeeIds, observedBase, "Base table without alias should still emit every row");
    Assertions.assertEquals(observedBase, observedMirror,
        "Join condition must resolve correctly when only the right-hand table is aliased");
  }

  private static List<Integer> fetchEmployeeIds() {
    List<Integer> ids = new ArrayList<>();
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
