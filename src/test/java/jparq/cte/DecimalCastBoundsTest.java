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
   * Rejects overflow after rounding and invalid bounds at the JDBC boundary.
   * 
   * @param expression
   *          the invalid or overflowing CAST projection
   * @throws Exception
   *           if opening a connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CAST(123 AS DECIMAL(2,0))", "CAST(99.5 AS NUMERIC(2))", "CAST(9.95 AS NUMBER(2,1))", "CAST(1 AS DECIMAL(2,5))",
      "CAST(1 AS NUMERIC(0,0))", "CAST(NULL AS NUMBER(2,5))"
  })
  void invalidCastsReportNumericSqlState(String expression) throws Exception {
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
}
