package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies common numeric CASE types and equality keys for issue #236. */
class CaseNumericTypeTest {
  @TempDir
  Path directory;

  private static final String INPUT = " FROM (VALUES (1), (2), (3)) t(id)";

  /**
   * Checks CASE types, values, and bounds across materialization boundaries.
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
  void commonTypes(boolean pushdown) throws Exception {
    List<String> expressions = List.of("CASE WHEN id = 1 THEN 1 ELSE 1.00 END",
        "CASE id WHEN 1 THEN 1.00 WHEN 2 THEN 1 ELSE 1.0 END", "CASE WHEN id = 1 THEN NULL ELSE 1.00 END",
        "CASE WHEN id = 1 THEN 1.00 END", "CASE WHEN id = 1 THEN (NULL) ELSE CASE WHEN id = 2 THEN 1 ELSE 1.00 END END",
        "CASE WHEN id = 1 THEN CAST(1 AS DECIMAL(8,2)) ELSE CAST(1 AS DECIMAL(6,3)) END",
        "CASE WHEN id = 1 THEN CAST(1 AS BIGINT) ELSE 1 END",
        "CASE WHEN id = 1 THEN CAST(1 AS REAL) ELSE CAST(1 AS REAL) END",
        "CASE WHEN id = 1 THEN CAST(1 AS REAL) ELSE 1.00 END", "CASE WHEN id = 1 THEN CAST(1 AS DOUBLE) ELSE 1 END");
    try (
        var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
        var statement = connection.createStatement()) {
      for (int index = 0; index < expressions.size(); index++) {
        String expression = expressions.get(index);
        int type = index == 6 ? Types.BIGINT : index == 7 ? Types.REAL : index >= 8 ? Types.DOUBLE : Types.DECIMAL;
        int scale = index == 5 ? 3 : 2;
        Object one = switch (type) {
          case Types.BIGINT -> 1L;
          case Types.REAL -> 1.0f;
          case Types.DOUBLE -> 1.0;
          default -> BigDecimal.ONE.setScale(scale);
        };
        for (boolean empty : List.of(false, true)) {
          String inner = "SELECT " + expression + " AS k, id" + INPUT + (empty ? " WHERE id = 99" : "");
          for (String sql : boundaries(inner)) {
            try (var result = statement.executeQuery(sql + " ORDER BY id")) {
              var metadata = result.getMetaData();
              assertEquals(type, metadata.getColumnType(1), sql);
              if (type == Types.DECIMAL) {
                assertEquals(scale, metadata.getScale(1), sql);
                assertEquals(index == 5 ? 9 : 3, metadata.getPrecision(1), sql);
              }
              int count = 0;
              while (result.next()) {
                int id = result.getInt(2);
                boolean nullValue = (index == 2 || index == 4) && id == 1 || index == 3 && id != 1;
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
          "CASE id WHEN 1 THEN 1.00 WHEN 2 THEN 1.0 ELSE 1 END")) {
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
        try (var result = statement
            .executeQuery("SELECT id, COUNT(*) OVER (PARTITION BY " + expression + ")" + INPUT + " ORDER BY id")) {
          for (int id = 1; id <= 3; id++) {
            assertTrue(result.next());
            assertEquals(id, result.getInt(1));
            assertEquals(3, result.getInt(2));
          }
          assertFalse(result.next());
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
