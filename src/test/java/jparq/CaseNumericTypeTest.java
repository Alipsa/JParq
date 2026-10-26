package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies common numeric CASE types and equality keys for issue #236. */
class CaseNumericTypeTest {
  @TempDir
  Path directory;

  private static final String INPUT = " FROM (VALUES (1), (2), (3)) t(id)";

  /**
   * Supplies explicit expectations independently of each case's position.
   *
   * @return expression, type, precision, scale, NULL row ids, and pushdown
   *         setting
   */
  static Stream<Arguments> commonTypeCases() {
    List<Arguments> cases = List.of(
        Arguments.of("CASE WHEN id = 1 THEN 1 ELSE 1.00 END", Types.DECIMAL, 3, 2, Set.of()),
        Arguments.of("CASE id WHEN 1 THEN 1.00 WHEN 2 THEN 1 ELSE 1.0 END", Types.DECIMAL, 3, 2, Set.of()),
        Arguments.of("CASE WHEN id = 1 THEN NULL ELSE 1.00 END", Types.DECIMAL, 3, 2, Set.of(1)),
        Arguments.of("CASE WHEN id = 1 THEN 1.00 END", Types.DECIMAL, 3, 2, Set.of(2, 3)),
        Arguments.of("CASE WHEN id = 1 THEN (NULL) ELSE CASE WHEN id = 2 THEN 1 ELSE 1.00 END END", Types.DECIMAL, 3, 2,
            Set.of(1)),
        Arguments.of("CASE WHEN id = 1 THEN CAST(1 AS DECIMAL(8,2)) ELSE CAST(1 AS DECIMAL(6,3)) END", Types.DECIMAL, 9,
            3, Set.of()),
        Arguments.of("CASE WHEN id = 1 THEN CAST(1 AS BIGINT) ELSE 1 END", Types.BIGINT, 19, 0, Set.of()),
        Arguments.of("CASE WHEN id = 1 THEN CAST(1 AS REAL) ELSE CAST(1 AS REAL) END", Types.REAL, -1, 0, Set.of()),
        Arguments.of("CASE WHEN id = 1 THEN CAST(1 AS REAL) ELSE 1.00 END", Types.DOUBLE, -1, 0, Set.of()),
        Arguments.of("CASE WHEN id = 1 THEN CAST(1 AS DOUBLE) ELSE 1 END", Types.DOUBLE, -1, 0, Set.of()),
        Arguments.of("CASE WHEN id = 1 THEN COALESCE(id, 0) ELSE 1.00 END", Types.DECIMAL, -1, 2, Set.of()),
        Arguments.of("CASE WHEN id = 1 THEN ABS(id) ELSE 1.00 END", Types.DECIMAL, -1, 2, Set.of()),
        Arguments.of("CASE WHEN id = 1 THEN ROUND(id, 1) ELSE 1.0 END", Types.DECIMAL, -1, 1, Set.of()));
    return cases.stream().flatMap(value -> Stream.of(true, false).map(pushdown -> {
      Object[] args = Arrays.copyOf(value.get(), value.get().length + 1);
      args[args.length - 1] = pushdown;
      return Arguments.of(args);
    }));
  }

  /**
   * Checks CASE types, values, and bounds across materialization boundaries.
   *
   * @param expression
   *          CASE projection
   * @param type
   *          expected JDBC type
   * @param precision
   *          expected precision, or -1 when inferred from values
   * @param scale
   *          expected decimal scale
   * @param nullIds
   *          row ids expected to yield NULL
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @MethodSource("commonTypeCases")
  void commonTypes(String expression, int type, int precision, int scale, Set<Integer> nullIds, boolean pushdown)
      throws Exception {
    Object one = switch (type) {
      case Types.BIGINT -> 1L;
      case Types.REAL -> 1.0f;
      case Types.DOUBLE -> 1.0;
      default -> BigDecimal.ONE.setScale(scale);
    };
    try (
        var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
        var statement = connection.createStatement()) {
      for (boolean empty : List.of(false, true)) {
        String inner = "SELECT " + expression + " AS k, id" + INPUT + (empty ? " WHERE id = 99" : "");
        for (String sql : boundaries(inner)) {
          try (var result = statement.executeQuery(sql + " ORDER BY id")) {
            var metadata = result.getMetaData();
            assertEquals(type, metadata.getColumnType(1), sql);
            if (type == Types.DECIMAL && precision >= 0) {
              assertEquals(scale, metadata.getScale(1), sql);
              assertEquals(precision, metadata.getPrecision(1), sql);
            }
            int count = 0;
            while (result.next()) {
              int id = result.getInt(2);
              boolean nullValue = nullIds.contains(id);
              Object actual = result.getObject(1);
              assertEquals(nullValue ? null : one, actual, sql);
              if (actual != null) {
                assertEquals(actual.getClass().getName(), metadata.getColumnClassName(1), sql);
              }
              count++;
            }
            assertEquals(empty ? 0 : 3, count, sql);
          }
        }
      }
    }
  }

  /**
   * Equal numeric branches form one DISTINCT, GROUP BY, and window partition key.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void equalityKeys(boolean pushdown) throws Exception {
    try (
        var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
        var statement = connection.createStatement()) {
      for (String expression : List.of("CASE WHEN id = 1 THEN 1 ELSE 1.0 END",
          "CASE id WHEN 1 THEN 1.00 WHEN 2 THEN 1.0 ELSE 1 END", "CASE WHEN id = 1 THEN 2 / 2 ELSE 1.0 END",
          "CASE WHEN id = 1 THEN COALESCE(id, 0) ELSE 1.0 END", "CASE WHEN id = 1 THEN ABS(id) ELSE 1.0 END",
          "CASE WHEN id = 1 THEN ROUND(id, 0) ELSE 1.0 END")) {
        for (String inner : List.of("SELECT DISTINCT " + expression + " AS k" + INPUT,
            "SELECT " + expression + " AS k, COUNT(*) AS n" + INPUT + " GROUP BY " + expression)) {
          for (String sql : boundaries(inner)) {
            try (var result = statement.executeQuery(sql)) {
              assertTrue(result.next(), sql);
              assertEquals(0, BigDecimal.ONE.compareTo(result.getBigDecimal(1)), sql);
              if (result.getMetaData().getColumnCount() == 2) {
                assertEquals(3, result.getInt(2), sql);
              }
              assertFalse(result.next(), sql);
            }
          }
        }
        for (String sql : boundaries("SELECT id, COUNT(*) OVER (PARTITION BY " + expression + ") AS n" + INPUT)) {
          try (var result = statement.executeQuery(sql + " ORDER BY id")) {
            for (int id = 1; id <= 3; id++) {
              assertTrue(result.next(), sql);
              assertEquals(id, result.getInt(1), sql);
              assertEquals(3, result.getInt(2), sql);
            }
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Grouped aggregate branches expose values matching their original CASE type.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void groupedAggregateBranches(boolean pushdown) throws Exception {
    try (
        var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
        var statement = connection.createStatement()) {
      for (String aggregate : List.of("SUM(id)", "COUNT(*)", "AVG(id)")) {
        boolean approximate = aggregate.startsWith("AVG");
        String alternative = aggregate.startsWith("SUM") ? "1.5" : "1.0";
        String inner = "SELECT CASE WHEN id = 1 THEN " + aggregate + " ELSE " + alternative + " END AS k, id" + INPUT
            + " GROUP BY id";
        for (String sql : boundaries(inner)) {
          try (var result = statement.executeQuery(sql + " ORDER BY id")) {
            var metadata = result.getMetaData();
            assertEquals(approximate ? Types.DOUBLE : Types.DECIMAL, metadata.getColumnType(1), sql);
            if (!approximate) {
              assertEquals(20, metadata.getPrecision(1), sql);
              assertEquals(1, metadata.getScale(1), sql);
            }
            for (int id = 1; id <= 3; id++) {
              assertTrue(result.next(), sql);
              Object expected = approximate ? Double.valueOf(1) : new BigDecimal(id == 1 ? "1.0" : alternative);
              Object actual = result.getObject(1);
              assertEquals(expected, actual, sql);
              assertEquals(actual.getClass().getName(), metadata.getColumnClassName(1), sql);
            }
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Keeps branch evaluation lazy and retains unbounded division values.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void lazyAndUnboundedBranches(boolean pushdown) throws Exception {
    try (
        var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
        var statement = connection.createStatement()) {
      for (String sql : boundaries(
          "SELECT CASE WHEN id = 1 THEN 1.0 ELSE CAST('bad' AS INTEGER) END AS k" + INPUT + " WHERE id = 1")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(new BigDecimal("1.0"), result.getObject(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      for (String sql : boundaries("SELECT CASE WHEN id = 1 THEN 1 / 3 ELSE 1 END AS k, id" + INPUT)) {
        try (var result = statement.executeQuery(sql + " ORDER BY id")) {
          assertTrue(result.next(), sql);
          assertEquals(new BigDecimal("0.3333333333333333"), result.getObject(1), sql);
          for (int id = 2; id <= 3; id++) {
            assertTrue(result.next(), sql);
            assertEquals(0, BigDecimal.ONE.compareTo(assertInstanceOf(BigDecimal.class, result.getObject(1))), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Builds direct, derived, CTE, and set-operation variants.
   *
   * @param inner
   *          the query to wrap
   * @return queries exercising each materialization boundary
   */
  private List<String> boundaries(String inner) {
    return List.of(inner, "SELECT * FROM (" + inner + ") q", "WITH q AS (" + inner + ") SELECT * FROM q",
        "SELECT * FROM (" + inner + " UNION ALL SELECT * FROM (" + inner + ") empty WHERE 1 = 0) q");
  }
}
