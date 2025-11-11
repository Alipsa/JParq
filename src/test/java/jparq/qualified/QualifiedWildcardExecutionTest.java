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

/** Integration tests verifying qualified wildcard projections behave as expected. */
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
    List<String> starColumns = new ArrayList<>();
    AtomicInteger starCount = new AtomicInteger();
    mtcarsSql.query("SELECT * FROM mtcars ORDER BY model", rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          starColumns.add(meta.getColumnLabel(i));
        }
        while (rs.next()) {
          starCount.incrementAndGet();
        }
      } catch (SQLException e) {
        throw new AssertionError(e);
      }
    });

    List<String> qualifiedColumns = new ArrayList<>();
    AtomicInteger qualifiedCount = new AtomicInteger();
    mtcarsSql.query("SELECT m.* FROM mtcars m ORDER BY m.model", rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          qualifiedColumns.add(meta.getColumnLabel(i));
        }
        while (rs.next()) {
          qualifiedCount.incrementAndGet();
        }
      } catch (SQLException e) {
        throw new AssertionError(e);
      }
    });

    assertEquals(starColumns, qualifiedColumns,
        "Qualified wildcard should expose the same columns as SELECT * for the same table");
    assertEquals(starCount.get(), qualifiedCount.get(),
        "Qualified wildcard should return the same row count as SELECT *");
  }

  @Test
  @DisplayName("Qualified wildcard can be combined with additional columns")
  void qualifiedWildcardWithAdditionalColumns() {
    List<String> employeeColumns = new ArrayList<>();
    acmeSql.query("SELECT * FROM employees ORDER BY id", rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          employeeColumns.add(meta.getColumnLabel(i));
        }
      } catch (SQLException e) {
        throw new AssertionError(e);
      }
    });

    List<String> projectionColumns = new ArrayList<>();
    AtomicInteger rowCount = new AtomicInteger();
    acmeSql.query("""
        SELECT e.*, d.department
        FROM employees e
        JOIN employee_department ed ON e.id = ed.employee
        JOIN departments d ON ed.department = d.id
        ORDER BY e.id
        """, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        if (projectionColumns.isEmpty()) {
          for (int i = 1; i <= meta.getColumnCount(); i++) {
            projectionColumns.add(meta.getColumnLabel(i));
          }
        }
        while (rs.next()) {
          rowCount.incrementAndGet();
        }
      } catch (SQLException e) {
        throw new AssertionError(e);
      }
    });

    assertFalse(projectionColumns.isEmpty(), "Projection should expose columns");
    assertEquals(employeeColumns, projectionColumns.subList(0, employeeColumns.size()),
        "Wildcard expansion should honour the employee column order");
    assertEquals("department", projectionColumns.get(projectionColumns.size() - 1),
        "Additional columns should follow the expanded wildcard projection");
    assertTrue(rowCount.get() > 0, "Join query should return rows");
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
}
