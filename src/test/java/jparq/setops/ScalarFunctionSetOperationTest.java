package jparq.setops;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies unbounded scalar numeric functions retain their set-operation types.
 */
class ScalarFunctionSetOperationTest {

  /**
   * Supplies functions whose numeric declarations have no fixed decimal bounds.
   *
   * @return SQL, expected precision, scale, values, and pushdown setting
   */
  static Stream<Arguments> numericFunctions() {
    List<Arguments> cases = List.of(
        Arguments.of("SELECT ROUND(mpg, 1) FROM mtcars WHERE cyl = 6 AND gear = 5 UNION ALL SELECT 1", 11, 1,
            List.of("19.7", "1.0")),
        Arguments.of("SELECT ABS(CAST(1.5 AS DOUBLE)) UNION ALL SELECT 1", 11, 1, List.of("1.5", "1.0")),
        Arguments.of("SELECT COALESCE(NULL, 1 / 4) UNION ALL SELECT 1", 12, 2, List.of("0.25", "1.00")),
        Arguments.of("SELECT ROUND(mpg, 0) FROM mtcars WHERE cyl = 6 INTERSECT SELECT 21", 10, 0, List.of("21")),
        Arguments.of("SELECT ROUND(1.25, 1) UNION ALL SELECT ROUND(2.5, 0)", 2, 1, List.of("1.3", "3.0")),
        Arguments.of("SELECT ROUND(1.25, 1) UNION SELECT 1.3", 2, 1, List.of("1.3")),
        Arguments.of("SELECT 1 UNION ALL SELECT ABS(CAST(1.5 AS DOUBLE))", 11, 1, List.of("1.0", "1.5")),
        Arguments.of("SELECT 21 EXCEPT SELECT ROUND(20.4, 0)", 10, 0, List.of("21")),
        Arguments.of(
            "SELECT ABS(x) FROM (SELECT cyl AS x FROM mtcars WHERE cyl = 4 AND gear = 5) t UNION ALL SELECT 1.5", 11, 1,
            List.of("4.0", "4.0", "1.5")),
        Arguments.of("SELECT ROUND(mpg, 1) FROM mtcars WHERE cyl = 99 UNION ALL SELECT 1", 10, 0, List.of("1")),
        Arguments.of("SELECT 1 UNION ALL SELECT ROUND(mpg, 1) FROM mtcars WHERE cyl = 99", 10, 0, List.of("1")),
        Arguments.of("SELECT ABS(mpg) FROM mtcars WHERE cyl = 99 UNION ALL SELECT 1", 10, 0, List.of("1")),
        Arguments.of(
            "SELECT COALESCE(NULL, CAST(mpg AS DECIMAL(5,2)) / 4) FROM mtcars WHERE cyl = 99 UNION ALL SELECT 1", 10, 0,
            List.of("1")),
        Arguments.of("SELECT ROUND(CAST(NULL AS DECIMAL(5,2)), 1) UNION ALL SELECT 1", 10, 0, Arrays.asList(null, "1")),
        Arguments.of("SELECT ABS(CAST(NULL AS DOUBLE)) UNION ALL SELECT 1", 10, 0, Arrays.asList(null, "1")),
        Arguments.of("SELECT COALESCE(NULL, CAST(NULL AS DECIMAL(5,2)) / 4) UNION ALL SELECT 1", 10, 0,
            Arrays.asList(null, "1")),
        Arguments.of("SELECT ROUND(mpg, 1) FROM mtcars WHERE cyl = 99 UNION ALL SELECT ROUND(NULL, 1)", 1, 0,
            Arrays.asList((String) null)),
        Arguments.of(
            "SELECT ROUND(mpg, 1) FROM mtcars WHERE cyl = 99 UNION ALL " + "SELECT ABS(mpg) FROM mtcars WHERE cyl = 99",
            1, 0, List.of()));
    return cases.stream().flatMap(value -> Stream.of(true, false).map(pushdown -> {
      Object[] args = Arrays.copyOf(value.get(), value.get().length + 1);
      args[args.length - 1] = pushdown;
      return Arguments.of(args);
    }));
  }

  /**
   * COALESCE uses both joined source declarations even when every selected value
   * comes from its INTEGER argument.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void promotesJoinedCoalesceToDouble(boolean pushdown) throws Exception {
    String sql = "SELECT COALESCE(a.cyl, b.mpg) FROM mtcars a JOIN mtcars b ON a.model = b.model "
        + "WHERE a.cyl = 4 AND a.gear = 5 UNION ALL SELECT 1";
    Path directory = Path.of(Objects.requireNonNull(getClass().getResource("/mtcars.parquet")).toURI()).getParent();
    try (
        var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      var metadata = result.getMetaData();
      assertEquals(Types.DOUBLE, metadata.getColumnType(1), sql);
      assertEquals(Double.class.getName(), metadata.getColumnClassName(1), sql);
      for (Double value : List.of(4.0, 4.0, 1.0)) {
        assertTrue(result.next(), sql);
        assertEquals(value, result.getObject(1), sql);
      }
      assertFalse(result.next(), sql);
    }
  }

  /**
   * Checks decimal metadata and exact values without a CASE wrapper.
   *
   * @param sql
   *          query containing top-level scalar functions
   * @param precision
   *          expected common decimal precision
   * @param scale
   *          expected common decimal scale
   * @param expected
   *          expected values in result order
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @ParameterizedTest
  @MethodSource("numericFunctions")
  void retainsNumericType(String sql, int precision, int scale, List<String> expected, boolean pushdown)
      throws Exception {
    Path directory = Path.of(Objects.requireNonNull(getClass().getResource("/mtcars.parquet")).toURI()).getParent();
    try (
        var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      var metadata = result.getMetaData();
      assertEquals(Types.DECIMAL, metadata.getColumnType(1), sql);
      assertEquals(BigDecimal.class.getName(), metadata.getColumnClassName(1), sql);
      assertEquals(precision, metadata.getPrecision(1), sql);
      assertEquals(scale, metadata.getScale(1), sql);
      for (String value : expected) {
        assertTrue(result.next(), sql);
        assertEquals(value == null ? null : new BigDecimal(value), result.getObject(1), sql);
      }
      assertFalse(result.next(), sql);
    }
  }

  /**
   * Numeric function values cannot bypass incompatible set-branch validation.
   *
   * @param expression
   *          numeric scalar function expression
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "ROUND(1.234, 2)", "ABS(CAST(1.5 AS DOUBLE))", "COALESCE(NULL, 1 / 4)",
      "ROUND(mpg, 1) FROM mtcars WHERE cyl = 99", "ABS(mpg) FROM mtcars WHERE cyl = 99",
      "COALESCE(NULL, mpg / 4) FROM mtcars WHERE cyl = 99", "ROUND(CAST(NULL AS DECIMAL(5,2)), 1)",
      "ABS(CAST(NULL AS DOUBLE))", "COALESCE(NULL, CAST(NULL AS DOUBLE) / 4)"
  })
  void rejectsNonnumericBranch(String expression) throws Exception {
    Path directory = Path.of(Objects.requireNonNull(getClass().getResource("/mtcars.parquet")).toURI()).getParent();
    for (boolean reverse : List.of(false, true)) {
      String sql = reverse
          ? "SELECT 'a' UNION ALL SELECT " + expression
          : "SELECT " + expression + " UNION ALL SELECT 'a'";
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
          var statement = connection.createStatement()) {
        SQLException error = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals("42000", error.getSQLState(), sql);
      }
    }
  }
}
