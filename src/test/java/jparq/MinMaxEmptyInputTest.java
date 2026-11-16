package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.Test;

/**
 * Verifies that MIN and MAX retain their argument's declared type for empty and
 * all-NULL input, so set operations reject incompatible branches (#320).
 */
class MinMaxEmptyInputTest {

  private static String url() {
    return "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
  }

  /**
   * An empty MIN reports the argument's declared numeric type and class.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void minOverEmptyInputReportsArgumentType() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT MIN(mpg) FROM mtcars WHERE cyl = 99")) {
        var meta = result.getMetaData();
        assertEquals(Types.DOUBLE, meta.getColumnType(1));
        assertEquals("java.lang.Double", meta.getColumnClassName(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * An empty MAX reports the argument's declared character type and class.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void maxOverEmptyInputReportsArgumentType() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT MAX(model) FROM mtcars WHERE cyl = 99")) {
        var meta = result.getMetaData();
        assertEquals(Types.VARCHAR, meta.getColumnType(1));
        assertEquals("java.lang.String", meta.getColumnClassName(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * An empty MIN over an integer argument reports INTEGER.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void minOverEmptyInputReportsIntegerArgumentType() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT MIN(cyl) FROM mtcars WHERE cyl = 99")) {
        var meta = result.getMetaData();
        assertEquals(Types.INTEGER, meta.getColumnType(1));
        assertEquals("java.lang.Integer", meta.getColumnClassName(1));
      }
    }
  }

  /**
   * Non-empty MIN and MAX keep reporting the observed argument values.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void minMaxOverNonEmptyInputKeepObservedTypes() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT MIN(mpg) FROM mtcars")) {
        assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertEquals(10.4, result.getDouble(1), 0.0);
      }
      try (var result = statement.executeQuery("SELECT MAX(model) FROM mtcars")) {
        assertEquals(Types.VARCHAR, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertNotNull(result.getString(1));
      }
    }
  }

  /**
   * A group whose values are all NULL reports the argument's declared type, which
   * VALUES typing derives from the non-NULL literals.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void groupedAllNullInputReportsArgumentType() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement
          .executeQuery("SELECT g, MIN(v) FROM (VALUES (1, 1.5), (2, NULL)) t(g, v) WHERE v IS NULL GROUP BY g")) {
        var meta = result.getMetaData();
        assertEquals(Types.DECIMAL, meta.getColumnType(2));
        assertEquals(2, meta.getPrecision(2));
        assertEquals(1, meta.getScale(2));
        assertTrue(result.next());
        assertEquals(2, result.getInt(1));
        assertNull(result.getObject(2));
        assertFalse(result.next());
      }
    }
  }

  /**
   * An empty DECIMAL MIN retains the argument's declared decimal bounds instead
   * of collapsing to OTHER.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void emptyDecimalMinimumRetainsDeclaredBounds() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement
          .executeQuery("SELECT MIN(v) FROM (VALUES (CAST(1.5 AS DECIMAL(10,2)))) t(v) WHERE 1 = 0")) {
        var meta = result.getMetaData();
        assertEquals(Types.DECIMAL, meta.getColumnType(1));
        assertEquals(3, meta.getPrecision(1));
        assertEquals(2, meta.getScale(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
      }
    }
  }

  /**
   * A set operation rejects a character branch against an empty numeric MIN.
   *
   * @throws Exception
   *           if connection creation fails
   */
  @Test
  void setOperationRejectsCharacterBranchForEmptyNumericMinimum() throws Exception {
    assertSetOperationRejected("SELECT MIN(mpg) FROM mtcars WHERE cyl = 99 UNION ALL SELECT 'a'");
  }

  /**
   * A set operation rejects a numeric branch against an empty character MAX.
   *
   * @throws Exception
   *           if connection creation fails
   */
  @Test
  void setOperationRejectsNumericBranchForEmptyCharacterMaximum() throws Exception {
    assertSetOperationRejected("SELECT MAX(model) FROM mtcars WHERE cyl = 99 UNION ALL SELECT 1");
  }

  /**
   * A set operation merges an empty MIN with a compatible numeric branch.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void setOperationMergesEmptyMinimumWithCompatibleBranch() throws Exception {
    String sql = "SELECT MIN(mpg) FROM mtcars WHERE cyl = 99 UNION ALL SELECT 2.5";
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery(sql)) {
        assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
        assertTrue(result.next());
        assertEquals(2.5, result.getDouble(1), 0.0);
        assertFalse(result.next());
      }
    }
  }

  private static void assertSetOperationRejected(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      SQLException error = assertThrows(SQLException.class, () -> {
        try (var statement = connection.prepareStatement(sql)) {
          statement.executeQuery().close();
        }
      }, sql);
      assertEquals("42000", error.getSQLState(), sql);
      assertTrue(error.getMessage().contains("Set operation components must use compatible column types"),
          error.getMessage());
    }
  }
}
