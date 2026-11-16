package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Exercises decimal correlation and stable schemas for LATERAL materialization.
 */
class LateralDecimalTest {
  private static String jdbcUrl;

  /**
   * Locates the decimal fixtures.
   * 
   * @throws Exception
   *           if fixture lookup fails
   */
  @BeforeAll
  static void locateFixtures() throws Exception {
    jdbcUrl = "jdbc:jparq:" + Path.of(LateralDecimalTest.class.getResource("/hf").toURI());
  }

  /**
   * Retains the navigation operand's decimal declaration even when the initial
   * NULL correlation probe returns no rows.
   * 
   * @param function
   *          the decimal window function
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LAG", "LEAD"
  })
  void windowDecimalsSurviveEmptyCorrelationProbe(String function) throws Exception {
    String sql = "SELECT m.id, x.l FROM optimized m, LATERAL (SELECT o.id AS oid, " + function
        + "(o.amount) OVER (ORDER BY o.id) AS l FROM optimized o WHERE o.id <= m.id AND o.id <= 4) x "
        + "WHERE m.id <= 3 ORDER BY m.id, x.oid";
    List<String> expected = "LAG".equals(function)
        ? Arrays.asList(null, null, "0.01", null, "0.01", "0.02")
        : Arrays.asList(null, "0.02", null, "0.02", "0.03", null);
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(2));
      int row = 0;
      for (String value : expected) {
        assertTrue(result.next());
        assertEquals(List.of(1L, 2L, 2L, 3L, 3L, 3L).get(row++), result.getLong(1));
        assertEquals(value == null ? null : new BigDecimal(value), result.getObject(2));
        if (value == null) {
          assertNull(result.getBigDecimal(2));
        } else {
          assertEquals(new BigDecimal(value), result.getBigDecimal(2));
        }
      }
      assertFalse(result.next());
    }
  }

  /**
   * Retains the decimal declaration of a LATERAL arithmetic projection even when
   * the initial NULL correlation probe produces no value (#475, fixed shape).
   * Division and ARRAY projections remain open in #475 and #480.
   *
   * @throws Exception
   *           if execution fails
   */
  @Test
  void lateralArithmeticRetainsDecimalOnNullProbe() throws Exception {
    String sql = "SELECT q.id, x.v FROM (VALUES (NULL), (2)) q(id)"
        + " CROSS JOIN LATERAL (SELECT q.id * 1.5 AS v FROM (VALUES (1)) s(z)) x";
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(2));
      assertEquals(12, result.getMetaData().getPrecision(2));
      assertEquals(1, result.getMetaData().getScale(2));
      assertEquals(BigDecimal.class.getName(), result.getMetaData().getColumnClassName(2));
      List<BigDecimal> values = new ArrayList<>();
      int nulls = 0;
      while (result.next()) {
        if (result.getObject(2) == null) {
          nulls++;
        } else {
          values.add(result.getBigDecimal(2));
        }
      }
      assertEquals(1, nulls);
      assertEquals(List.of(new BigDecimal("3.0")), values);
    }
  }

  /**
   * Decodes the outer decimal value before substituting it into a correlated
   * CAST.
   *
   * @param type
   *          the target decimal type
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "DECIMAL", "DECIMAL(12,2)", "NUMBER(12,2)"
  })
  void correlatedCastsReceiveDecodedDecimalValues(String type) throws Exception {
    String sql = "SELECT m.id, x.a FROM optimized m, LATERAL (SELECT CAST(m.amount AS " + type
        + ") AS a FROM (VALUES (1)) v(z)) x WHERE m.id <= 3 ORDER BY m.id";
    try (var connection = DriverManager.getConnection(jdbcUrl);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      for (String expected : List.of("0.01", "0.02", "0.03")) {
        assertTrue(result.next());
        assertEquals(new BigDecimal(expected), result.getBigDecimal(2));
      }
      assertFalse(result.next());
    }
  }
}
