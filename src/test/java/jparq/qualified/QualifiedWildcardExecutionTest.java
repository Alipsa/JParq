package jparq.qualified;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests verifying qualified wildcard projections behave as
 * expected.
 */
public class QualifiedWildcardExecutionTest {

  private static JParqSql mtcarsSql;
  private static JParqSql acmeSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = QualifiedWildcardExecutionTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI()).getParent();
    mtcarsSql = new JParqSql("jdbc:jparq:" + mtcarsPath.toAbsolutePath());

    URL acmeUrl = QualifiedWildcardExecutionTest.class.getResource("/acme");
    assertNotNull(acmeUrl, "acme dataset must be available on the classpath");
    Path acmePath = Paths.get(acmeUrl.toURI());
    acmeSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  @Test
  @DisplayName("Alias wildcard matches unqualified star projection")
  void aliasWildcardMatchesSelectStar() {
    ResultSnapshot star = captureResult(mtcarsSql, "SELECT * FROM mtcars ORDER BY model");
    ResultSnapshot qualified = captureResult(mtcarsSql, "SELECT m.* FROM mtcars m ORDER BY m.model");

    assertEquals(star.labels(), qualified.labels(),
        "Qualified wildcard should expose the same columns as SELECT * for the same table");
    assertEquals(star.rowCount(), qualified.rowCount(),
        "Qualified wildcard should return the same row count as SELECT *");
  }

  @Test
  @DisplayName("Table name wildcard matches unqualified star projection")
  void tableNameWildcardMatchesSelectStar() {
    ResultSnapshot star = captureResult(mtcarsSql, "SELECT * FROM mtcars ORDER BY model");
    ResultSnapshot qualified = captureResult(mtcarsSql, "SELECT mtcars.* FROM mtcars ORDER BY model");

    assertEquals(star.labels(), qualified.labels(),
        "Table-qualified wildcard should expose the same columns as SELECT * for the same table");
    assertEquals(star.rowCount(), qualified.rowCount(),
        "Table-qualified wildcard should return the same row count as SELECT *");
  }

  @Test
  @DisplayName("Qualified wildcard can be combined with additional columns")
  void qualifiedWildcardWithAdditionalColumns() {
    ResultSnapshot employeeSnapshot = captureResult(acmeSql, "SELECT * FROM employees ORDER BY id");
    ResultSnapshot projectionSnapshot = captureResult(acmeSql, """
        SELECT e.*, d.department
        FROM employees e
        JOIN employee_department ed ON e.id = ed.employee
        JOIN departments d ON ed.department = d.id
        ORDER BY e.id
        """);

    List<String> projectionColumns = projectionSnapshot.labels();
    assertFalse(projectionColumns.isEmpty(), "Projection should expose columns");
    assertEquals(employeeSnapshot.labels(), projectionColumns.subList(0, employeeSnapshot.labels().size()),
        "Wildcard expansion should honor the employee column order");
    assertEquals("department", projectionColumns.getLast(),
        "Additional columns should follow the expanded wildcard projection");
    assertTrue(projectionSnapshot.rowCount() > 0, "Join query should return rows");
  }

  @Test
  @DisplayName("Multiple qualified wildcards preserve projection order")
  void multipleQualifiedWildcardsPreserveOrder() {
    ResultSnapshot employeeSnapshot = captureResult(acmeSql, "SELECT * FROM employees ORDER BY id");
    ResultSnapshot departmentSnapshot = captureResult(acmeSql, """
        SELECT d.*
        FROM employees e
        JOIN employee_department ed ON e.id = ed.employee
        JOIN departments d ON ed.department = d.id
        ORDER BY e.id
        """);
    ResultSnapshot projectionSnapshot = captureResult(acmeSql, """
        SELECT e.*, d.*
        FROM employees e
        JOIN employee_department ed ON e.id = ed.employee
        JOIN departments d ON ed.department = d.id
        ORDER BY e.id
        """);

    List<String> labels = projectionSnapshot.labels();
    final int employeeSize = employeeSnapshot.labels().size();
    final int departmentSize = departmentSnapshot.labels().size();
    assertEquals(employeeSnapshot.labels(), labels.subList(0, employeeSize),
        "Employee columns should be emitted before the joined department columns");
    assertEquals(employeeSize + departmentSize, labels.size(),
        () -> "Projection size mismatch. Employee columns: " + employeeSnapshot.labels()
            + ", Department columns: " + departmentSnapshot.labels()
            + ", Projection labels: " + labels);
    assertEquals(departmentSnapshot.labels(), labels.subList(employeeSize, employeeSize + departmentSize),
        "Department wildcard expansion should retain the department column order");
    assertTrue(projectionSnapshot.rowCount() > 0, "Join query should return rows");
  }

  @Test
  @DisplayName("Quoted qualifier wildcard resolves columns")
  void quotedQualifierWildcardResolvesColumns() {
    ResultSnapshot expected = captureResult(acmeSql, "SELECT * FROM employees ORDER BY id");
    ResultSnapshot quoted = captureResult(acmeSql, """
        SELECT "Employees".*
        FROM employees "Employees"
        ORDER BY "Employees".id
        """);

    assertEquals(expected.labels(), quoted.labels(),
        "Quoted qualifier wildcard should expose the same columns as the base table");
    assertEquals(expected.rowCount(), quoted.rowCount(),
        "Quoted qualifier wildcard should return the same rows as the base table");
  }

  @Test
  @DisplayName("Schema-like qualifier wildcard resolves columns")
  void schemaLikeQualifierWildcardResolvesColumns() {
    ResultSnapshot expected = captureResult(acmeSql, "SELECT * FROM employees ORDER BY id");
    ResultSnapshot qualified = captureResult(acmeSql, """
        SELECT "acme.employees".*
        FROM employees "acme.employees"
        """);

    assertEquals(expected.labels(), qualified.labels(),
        "Schema-style qualifier wildcard should expose the same columns as the base table");
    assertEquals(expected.rowCount(), qualified.rowCount(),
        "Schema-style qualifier wildcard should return the same rows as the base table");
  }

  @Test
  @DisplayName("Qualified wildcard expands columns from a CTE")
  void qualifiedWildcardExpandsCteColumns() {
    ResultSnapshot expected = captureResult(acmeSql, """
        SELECT employee, salary AS salary__usd
        FROM salary
        ORDER BY employee
        """);
    ResultSnapshot cteSnapshot = captureResult(acmeSql, """
        WITH pay AS (
          SELECT employee, salary AS salary__usd
          FROM salary
        )
        SELECT pay.*
        FROM pay
        ORDER BY pay.employee
        """);

    assertEquals(expected.labels(), cteSnapshot.labels(),
        "CTE-backed wildcard should expose the projected columns in the same order");
    assertEquals(expected.rowCount(), cteSnapshot.rowCount(),
        "CTE-backed wildcard should return the same number of rows as the defining query");
    assertTrue(cteSnapshot.labels().contains("salary__usd"),
        "Column names containing double underscores must be preserved during expansion");
  }

  @Test
  @DisplayName("Qualified wildcard expands columns from a derived table")
  void qualifiedWildcardExpandsDerivedTableColumns() {
    ResultSnapshot expected = captureResult(acmeSql, """
        SELECT id, first_name, last_name
        FROM employees
        ORDER BY id
        """);
    ResultSnapshot derivedSnapshot = captureResult(acmeSql, """
        SELECT derived.*
        FROM (
          SELECT id, first_name, last_name
          FROM employees
        ) derived
        ORDER BY derived.id
        """);

    assertEquals(expected.labels(), derivedSnapshot.labels(),
        "Derived table wildcard should expose the inner projection columns");
    assertEquals(expected.rowCount(), derivedSnapshot.rowCount(),
        "Derived table wildcard should return the same rows as the inner projection");
  }

  @Test
  @DisplayName("Unknown qualifier raises an error")
  void unknownQualifierRaisesError() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> mtcarsSql.query("SELECT missing.* FROM mtcars", rs -> {
        }));
    Throwable cause = exception.getCause();
    assertNotNull(cause, "Expected a SQLException to be reported");
    assertEquals(SQLException.class, cause.getClass(), "Expected JDBC layer to surface a SQLException");
    Throwable rootCause = cause.getCause();
    assertNotNull(rootCause, "Underlying cause should describe the qualifier resolution failure");
    assertEquals(IllegalArgumentException.class, rootCause.getClass(),
        "Qualified wildcard resolution should report an IllegalArgumentException");
  }

  private static ResultSnapshot captureResult(JParqSql sql, String query) {
    List<String> labels = new ArrayList<>();
    AtomicInteger rowCount = new AtomicInteger();
    sql.query(query, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        if (labels.isEmpty()) {
          for (int i = 1; i <= meta.getColumnCount(); i++) {
            labels.add(meta.getColumnLabel(i));
          }
        }
        while (rs.next()) {
          rowCount.incrementAndGet();
        }
      } catch (SQLException e) {
        throw new AssertionError(e);
      }
    });
    return new ResultSnapshot(List.copyOf(labels), rowCount.get());
  }

  private record ResultSnapshot(List<String> labels, int rowCount) {
  }
}
