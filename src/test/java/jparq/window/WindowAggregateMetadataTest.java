package jparq.window;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Verifies aggregate window declarations independently of returned rows. */
class WindowAggregateMetadataTest {
  @TempDir
  Path directory;

  /**
   * Supplies aggregate operands and their expected numeric result declarations.
   *
   * @return aggregate expression, JDBC type, class, precision, and scale
   */
  static Stream<Arguments> numericAggregates() {
    return Stream.of("INTEGER", "BIGINT", "REAL", "DOUBLE", "DECIMAL(8,2)")
        .flatMap(type -> Stream.of("COUNT", "SUM", "AVG", "MIN", "MAX").map(function -> {
          int operandType = switch (type) {
            case "INTEGER" -> Types.INTEGER;
            case "BIGINT" -> Types.BIGINT;
            case "REAL" -> Types.REAL;
            case "DOUBLE" -> Types.DOUBLE;
            default -> Types.DECIMAL;
          };
          int resultType = switch (function) {
            case "COUNT" -> Types.BIGINT;
            case "AVG" -> Types.DOUBLE;
            case "SUM" ->
              operandType == Types.INTEGER ? Types.BIGINT : operandType == Types.REAL ? Types.DOUBLE : operandType;
            default -> operandType;
          };
          Class<?> resultClass = switch (resultType) {
            case Types.INTEGER -> Integer.class;
            case Types.BIGINT -> Long.class;
            case Types.REAL -> Float.class;
            case Types.DOUBLE -> Double.class;
            default -> BigDecimal.class;
          };
          int precision = resultType == Types.DECIMAL ? "SUM".equals(function) ? 27 : 8 : -1;
          int scale = resultType == Types.DECIMAL ? 2 : 0;
          return Arguments.of(function + "(CAST(id AS " + type + "))", resultType, resultClass, precision, scale);
        }));
  }

  /**
   * Checks numeric declarations before iteration, after iteration, through a
   * derived table, and when the source has no rows.
   *
   * @param aggregate
   *          aggregate expression
   * @param type
   *          expected JDBC type
   * @param valueClass
   *          expected Java value class
   * @param precision
   *          expected decimal precision, or -1 for nondecimal results
   * @param scale
   *          expected scale
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @MethodSource("numericAggregates")
  void numericDeclarations(String aggregate, int type, Class<?> valueClass, int precision, int scale)
      throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (boolean empty : new boolean[]{
          false, true
      }) {
        String query = "SELECT " + aggregate + " OVER () AS id FROM (VALUES (1), (2)) t(id)"
            + (empty ? " WHERE id < 0" : "");
        for (boolean derived : new boolean[]{
            false, true
        }) {
          try (var result = statement.executeQuery(derived ? "SELECT id FROM (" + query + ") w" : query)) {
            assertDeclaration(result.getMetaData(), 1, type, valueClass, precision, scale);
            int rows = 0;
            while (result.next()) {
              assertInstanceOf(valueClass, result.getObject(1));
              rows++;
            }
            assertEquals(empty ? 0 : 2, rows);
            assertDeclaration(result.getMetaData(), 1, type, valueClass, precision, scale);
          }
        }
      }
    }
  }

  /**
   * Reproduces the issue's five-column query and checks actual aggregate values.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void integerAggregatesAndCountStar() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT COUNT(*) OVER () AS c, SUM(id) OVER () AS s, AVG(id) OVER () AS a,
                   MIN(id) OVER () AS mn, MAX(id) OVER () AS mx
            FROM (VALUES (1), (2)) t(id)
            """)) {
      int[] types = {
          Types.BIGINT, Types.BIGINT, Types.DOUBLE, Types.INTEGER, Types.INTEGER
      };
      Object[] values = {
          2L, 3L, 1.5, 1, 2
      };
      for (int column = 1; column <= values.length; column++) {
        assertDeclaration(result.getMetaData(), column, types[column - 1], values[column - 1].getClass(), -1, 0);
        assertEquals(column == 1 ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable,
            result.getMetaData().isNullable(column));
      }
      while (result.next()) {
        for (int column = 1; column <= values.length; column++) {
          assertEquals(values[column - 1], result.getObject(column));
        }
      }
    }
  }

  /**
   * Preserves nonnumeric MIN/MAX operands and typed NULL numeric declarations.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void nonnumericAndNullOperands() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (boolean empty : new boolean[]{
          false, true
      }) {
        try (var result = statement.executeQuery("""
            SELECT MIN(CAST(id AS VARCHAR)) OVER () AS lo,
                   MAX(CAST(id AS DATE)) OVER () AS hi,
                   SUM(CAST(NULL AS DECIMAL(8,2))) OVER () AS total
            FROM (VALUES (1), (2)) t(id)
            """ + (empty ? " WHERE id < 0" : ""))) {
          assertDeclaration(result.getMetaData(), 1, Types.VARCHAR, String.class, -1, 0);
          assertDeclaration(result.getMetaData(), 2, Types.DATE, java.sql.Date.class, -1, 0);
          assertDeclaration(result.getMetaData(), 3, Types.DECIMAL, BigDecimal.class, 27, 2);
          if (!empty) {
            assertTrue(result.next());
            assertInstanceOf(String.class, result.getObject(1));
            assertInstanceOf(java.sql.Date.class, result.getObject(2));
            assertNull(result.getObject(3));
          }
        }
      }
    }
  }

  /**
   * Checks expression operands in partitioned, ordered, bounded windows,
   * including a frame with no rows.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void partitionedFramesAndExpressionOperands() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT COUNT(t.id) OVER (PARTITION BY grp ORDER BY t.id
                     ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS c,
                   SUM(t.id + 1) OVER (PARTITION BY grp ORDER BY t.id
                     ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s,
                   AVG(t.id + 1) OVER (PARTITION BY grp ORDER BY t.id
                     ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a,
                   MIN(CASE WHEN t.id > 0 THEN t.id + 1 ELSE 0 END) OVER (PARTITION BY grp) AS mn,
                   MAX(CAST(t.id AS VARCHAR)) OVER (PARTITION BY grp) AS mx,
                   SUM(t.id) OVER (PARTITION BY grp ORDER BY t.id
                     ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS following
            FROM (VALUES (1, 1), (2, 1), (4, 2)) t(id, grp)
            ORDER BY t.id
            """)) {
      assertDeclaration(result.getMetaData(), 1, Types.BIGINT, Long.class, -1, 0);
      assertDeclaration(result.getMetaData(), 2, Types.BIGINT, Long.class, -1, 0);
      assertDeclaration(result.getMetaData(), 3, Types.DOUBLE, Double.class, -1, 0);
      assertDeclaration(result.getMetaData(), 4, Types.BIGINT, Long.class, -1, 0);
      assertDeclaration(result.getMetaData(), 5, Types.VARCHAR, String.class, -1, 0);
      assertDeclaration(result.getMetaData(), 6, Types.BIGINT, Long.class, -1, 0);
      Object[][] expected = {
          {
              1L, 2L, 2.0, 2L, "2", 2L
          }, {
              2L, 5L, 2.5, 2L, "2", null
          }, {
              1L, 5L, 5.0, 5L, "4", null
          }
      };
      for (Object[] row : expected) {
        assertTrue(result.next());
        for (int column = 1; column <= row.length; column++) {
          assertEquals(row[column - 1], result.getObject(column));
        }
      }
      assertFalse(result.next());
    }
  }

  /**
   * Checks the public metadata describing a single aggregate result.
   *
   * @param metadata
   *          result metadata
   * @param column
   *          one-based column index
   * @param type
   *          expected JDBC type
   * @param valueClass
   *          expected Java value class
   * @param precision
   *          expected precision, or -1 to omit the check
   * @param scale
   *          expected scale
   * @throws SQLException
   *           if metadata access fails
   */
  private static void assertDeclaration(ResultSetMetaData metadata, int column, int type, Class<?> valueClass,
      int precision, int scale) throws SQLException {
    assertEquals(type, metadata.getColumnType(column));
    assertEquals(valueClass.getName(), metadata.getColumnClassName(column));
    assertEquals(scale, metadata.getScale(column));
    if (precision >= 0) {
      assertEquals(precision, metadata.getPrecision(column));
    }
  }
}
