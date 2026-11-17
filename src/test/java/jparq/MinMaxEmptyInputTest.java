package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.Test;

/**
 * Verifies that MIN and MAX retain their argument's declared type and decimal
 * bounds regardless of row count, so set operations reject incompatible
 * branches and merge stable bounds (#320).
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
      try (var result = statement.executeQuery("SELECT MIN(CAST(mpg AS DECIMAL(10,2))) FROM mtcars WHERE cyl = 99")) {
        var meta = result.getMetaData();
        assertEquals(Types.DECIMAL, meta.getColumnType(1));
        assertEquals(10, meta.getPrecision(1));
        assertEquals(2, meta.getScale(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
      }
    }
  }

  /**
   * A DECIMAL MIN reports the argument's declared decimal bounds with and without
   * rows, so the bounds do not depend on the returned values.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void decimalMinimumBoundsDoNotDependOnRows() throws Exception {
    String sql = "SELECT MIN(CAST(mpg AS DECIMAL(10,2))) FROM mtcars";
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery(sql)) {
        var meta = result.getMetaData();
        assertEquals(Types.DECIMAL, meta.getColumnType(1));
        assertEquals(10, meta.getPrecision(1));
        assertEquals(2, meta.getScale(1));
        assertTrue(result.next());
        assertEquals(new java.math.BigDecimal("10.40"), result.getBigDecimal(1));
      }
      try (var result = statement.executeQuery(sql + " WHERE cyl = 99")) {
        var meta = result.getMetaData();
        assertEquals(Types.DECIMAL, meta.getColumnType(1));
        assertEquals(10, meta.getPrecision(1));
        assertEquals(2, meta.getScale(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
      }
    }
  }

  /**
   * A set operation over a DECIMAL MIN merges the declared decimal bounds whether
   * or not the MIN branch returns rows.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void setOperationBoundsDoNotDependOnRows() throws Exception {
    String suffix = " UNION ALL SELECT CAST(1 AS DECIMAL(3,0))";
    String emptyBranch = "SELECT MIN(CAST(mpg AS DECIMAL(10,2))) FROM mtcars WHERE cyl = 99" + suffix;
    String filledBranch = "SELECT MIN(CAST(mpg AS DECIMAL(10,2))) FROM mtcars" + suffix;
    int[] declaredBounds = {
        Types.DECIMAL, 10, 2
    };
    try (var connection = DriverManager.getConnection(url())) {
      assertArrayEquals(declaredBounds, setBounds(connection, emptyBranch), "merged bounds must not depend on rows");
      assertArrayEquals(declaredBounds, setBounds(connection, filledBranch), "merged bounds must not depend on rows");
    }
  }

  /**
   * An empty MIN over an unbounded decimal argument reports DECIMAL instead of
   * OTHER, matching the argument's plain empty projection.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void emptyUnboundedDecimalMinimumReportsDecimal() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT MIN(cyl / 3) FROM mtcars WHERE cyl = 99")) {
        var meta = result.getMetaData();
        assertEquals(Types.DECIMAL, meta.getColumnType(1));
        assertEquals(0, meta.getPrecision(1));
        assertEquals(0, meta.getScale(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
      }
      try (var result = statement.executeQuery("SELECT MIN(ROUND(mpg, 1)) FROM mtcars WHERE cyl = 99")) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
      }
      try (var result = statement.executeQuery("SELECT MIN(cyl / 3) FROM mtcars")) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertNotNull(result.getBigDecimal(1));
      }
    }
  }

  /**
   * An empty MAX over an unbounded decimal argument reports DECIMAL instead of
   * OTHER, matching the argument's plain empty projection.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void emptyUnboundedDecimalMaximumReportsDecimal() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT MAX(cyl / 3) FROM mtcars WHERE cyl = 99")) {
        var meta = result.getMetaData();
        assertEquals(Types.DECIMAL, meta.getColumnType(1));
        assertEquals(0, meta.getPrecision(1));
        assertEquals(0, meta.getScale(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
      }
      try (var result = statement.executeQuery("SELECT MAX(ROUND(mpg, 1)) FROM mtcars WHERE cyl = 99")) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
      }
      try (var result = statement.executeQuery("SELECT MAX(cyl / 3) FROM mtcars")) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertNotNull(result.getBigDecimal(1));
      }
    }
  }

  /**
   * Reads a set operation's first-column JDBC type, precision, and scale.
   *
   * @param connection
   *          open connection
   * @param sql
   *          set operation to execute
   * @return the column type, precision, and scale
   * @throws Exception
   *           if query execution fails
   */
  private static int[] setBounds(java.sql.Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        result.getObject(1);
      }
      var meta = result.getMetaData();
      return new int[]{
          meta.getColumnType(1), meta.getPrecision(1), meta.getScale(1)
      };
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
