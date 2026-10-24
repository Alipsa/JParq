package jparq.setops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers decimal bounds, literal declarations, and mixed numeric
 * multiplicities.
 */
class SetNumericBoundaryTest {

  @TempDir
  Path directory;

  /**
   * Preserves declared bounds and exact values through widening and empty sets.
   *
   * @param empty
   *          whether branches are empty
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void decimalBoundaries(boolean empty) throws Exception {
    String filter = empty ? " WHERE FALSE" : "";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String sql : List.of("SELECT CAST(99999999.99 AS DECIMAL(10,2)) AS n" + filter
          + " UNION ALL SELECT CAST(0.0001 AS DECIMAL(10,4))" + filter,
          "SELECT 9223372036854775808 AS n" + filter + " UNION ALL SELECT 0.0001" + filter)) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          var meta = result.getMetaData();
          assertEquals(Types.DECIMAL, meta.getColumnType(1), sql);
          assertEquals(sql.contains("CAST") ? 12 : 23, meta.getPrecision(1), sql);
          assertEquals(4, meta.getScale(1), sql);
          if (!empty) {
            assertTrue(result.next());
            assertEquals(new BigDecimal(sql.contains("CAST") ? "99999999.9900" : "9223372036854775808.0000"),
                result.getObject(1), sql);
            assertTrue(result.next());
            assertEquals(new BigDecimal("0.0001"), result.getObject(1), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
      String widenedCast = "WITH x AS (SELECT CAST(1.25 AS DECIMAL(6,2)) AS n) "
          + "SELECT CAST(n AS DECIMAL(10,4)) AS n FROM x" + filter + " UNION ALL SELECT CAST(2 AS INTEGER)" + filter;
      try (var statement = connection.createStatement(); var result = statement.executeQuery(widenedCast)) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertEquals(14, result.getMetaData().getPrecision(1));
        assertEquals(4, result.getMetaData().getScale(1));
        if (!empty) {
          assertTrue(result.next());
          assertEquals(new BigDecimal("1.2500"), result.getObject(1));
          assertTrue(result.next());
          assertEquals(new BigDecimal("2.0000"), result.getObject(1));
        }
        assertFalse(result.next());
      }
      String sql = "SELECT CAST(NULL AS INTEGER) AS n" + filter + " UNION ALL SELECT -1" + filter;
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
        if (!empty) {
          assertTrue(result.next());
          assertEquals(null, result.getObject(1));
          assertTrue(result.next());
          assertEquals(-1L, result.getObject(1));
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Applies approximate conversion before equality, where DOUBLE loses a digit.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void approximateComparisonUsesConvertedValues() throws Exception {
    String left = "SELECT 9007199254740993 AS n";
    String right = "SELECT CAST(9007199254740992 AS DOUBLE)";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String operator : List.of("UNION", "INTERSECT", "EXCEPT")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery(left + " " + operator + " " + right)) {
          assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1));
          if (!operator.equals("EXCEPT")) {
            assertTrue(result.next());
            assertEquals(9007199254740992.0, result.getObject(1));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Counts duplicates only after branches share a numeric representation.
   *
   * @param intersect
   *          whether INTERSECT ALL rather than EXCEPT ALL is tested
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void mixedNumericMultiplicity(boolean intersect) throws Exception {
    String sql = "(SELECT CAST(1 AS INTEGER) AS n UNION ALL SELECT CAST(1 AS INTEGER) "
        + "UNION ALL SELECT CAST(2 AS INTEGER)) " + (intersect ? "INTERSECT ALL" : "EXCEPT ALL")
        + " SELECT CAST(1 AS DECIMAL(10,3)) ORDER BY n";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
      assertEquals(13, result.getMetaData().getPrecision(1));
      assertEquals(3, result.getMetaData().getScale(1));
      assertTrue(result.next());
      assertEquals(new BigDecimal("1.000"), result.getObject(1));
      if (!intersect) {
        assertTrue(result.next());
        assertEquals(new BigDecimal("2.000"), result.getObject(1));
      }
      assertFalse(result.next());
    }
  }
}
