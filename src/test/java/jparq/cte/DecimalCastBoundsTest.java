package jparq.cte;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Keeps decimal CAST evaluation consistent with declared materialization
 * bounds.
 */
class DecimalCastBoundsTest {
  private static String jdbcUrl;

  /**
   * Locates the decimal fixtures.
   * 
   * @throws Exception
   *           if fixture lookup fails
   */
  @BeforeAll
  static void locateFixtures() throws Exception {
    jdbcUrl = "jdbc:jparq:" + Path.of(DecimalCastBoundsTest.class.getResource("/hf").toURI());
  }

  /**
   * Checks rounding before precision validation for every materialization path.
   * 
   * @param expression
   *          the decimal CAST projection
   * @param expected
   *          the rounded decimal value
   * @param precision
   *          the declared precision
   * @param scale
   *          the declared or defaulted scale
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "CAST(amount AS NUMERIC(6)) | 0 | 6 | 0", "CAST(1.5 AS NUMERIC(6)) | 2 | 6 | 0",
      "CAST(-1.5 AS NUMBER(6)) | -2 | 6 | 0", "CAST(9.94 AS DECIMAL(2,1)) | 9.9 | 2 | 1",
      "CAST(0.5 AS NUMBER(10,2)) | 0.50 | 10 | 2"
  })
  void castsRoundWithinDeclaredBounds(String expression, String expected, int precision, int scale) throws Exception {
    String body = "SELECT " + expression + " AS n FROM optimized WHERE id <= 2";
    for (String sql : List.of(body, "WITH c AS (" + body + ") SELECT n FROM c", "SELECT d.n FROM (" + body + ") d")) {
      try (var connection = DriverManager.getConnection(jdbcUrl);
          var statement = connection.createStatement();
          var result = statement.executeQuery(sql)) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertEquals(precision, result.getMetaData().getPrecision(1));
        assertEquals(scale, result.getMetaData().getScale(1));
        for (int row = 0; row < 2; row++) {
          assertTrue(result.next(), sql);
          assertEquals(new BigDecimal(expected), result.getObject(1), sql);
        }
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Rejects value overflow after rounding at the JDBC boundary.
   * 
   * @param expression
   *          the overflowing CAST projection
   * @throws Exception
   *           if opening a connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CAST(123 AS DECIMAL(2,0))", "CAST(99.5 AS NUMERIC(2))", "CAST(9.95 AS NUMBER(2,1))"
  })
  void overflowingCastsReportNumericSqlState(String expression) throws Exception {
    String body = "SELECT " + expression + " AS n";
    for (String sql : List.of(body, "WITH c AS (" + body + ") SELECT n FROM c", "SELECT d.n FROM (" + body + ") d")) {
      try (var connection = DriverManager.getConnection(jdbcUrl); var statement = connection.createStatement()) {
        SQLException failure = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals("22003", failure.getSQLState(), sql);
      }
    }
  }
  /**
   * Rejects invalid declarations before data access or parameter binding.
   *
   * @param sql
   *          the statement containing invalid decimal bounds
   * @throws Exception
   *           if opening a connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT CAST(1 AS DECIMAL(2,5))", "SELECT CAST(NULL AS NUMBER(2,5))", "SELECT CAST(? AS NUMERIC(0,0))",
      "SELECT CAST(amount AS DECIMAL(2,5)) FROM optimized WHERE id < 0",
      "SELECT 1 FROM optimized WHERE id < 0 AND CAST(amount AS NUMBER(0)) = 0",
      "WITH c AS (SELECT CAST(NULL AS NUMERIC(2,5)) AS n) SELECT n FROM c",
      "WITH c AS (SELECT CAST(NULL AS NUMERIC(2,5)) AS n) SELECT 1",
      "SELECT d.n FROM (SELECT CAST(1 AS DECIMAL(0)) AS n) d",
      "SELECT CASE WHEN FALSE THEN CAST(1 AS DECIMAL(2,5)) ELSE 1 END",
      "SELECT 1 UNION ALL SELECT CAST(NULL AS NUMBER(2,5))",
      "SELECT m.id FROM optimized m, LATERAL " + "(SELECT CAST(m.amount AS DECIMAL(2,5)) AS n FROM (VALUES (1)) v(z)) x"
  })
  void invalidDeclarationsFailDuringPreparation(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl)) {
      SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
      assertEquals("42000", failure.getSQLState(), sql);
      assertTrue(failure.getMessage().contains("Invalid decimal bounds"), failure.getMessage());
    }
  }

  /**
   * Preserves a syntax SQLState when the SQL parser itself rejects decimal
   * bounds.
   *
   * @param sql
   *          the statement with unsupported negative type parameters
   * @throws Exception
   *           if opening a connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT CAST(1 AS NUMERIC(2,-1))", "SELECT CAST(1 AS DECIMAL(-1,0))"
  })
  void invalidDecimalSyntaxFailsDuringPreparation(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl)) {
      SQLException failure = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
      assertEquals("42000", failure.getSQLState(), sql);
    }
  }

}
