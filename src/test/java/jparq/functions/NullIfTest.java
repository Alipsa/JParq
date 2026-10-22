package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies NULLIF uses SQL equality and preserves its first argument's value.
 */
class NullIfTest {
  /**
   * Covers equal, unequal, NULL, numeric, string, Boolean, and temporal
   * arguments.
   *
   * @throws Exception
   *           if connection setup or query execution fails
   */
  @Test
  void evaluatesScalarArguments() throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl());
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT NULLIF(1, 1), NULLIF(1, 2), NULLIF('a', 'a'), NULLIF(NULL, 1),
                   NULLIF(1, NULL), NULLIF(NULL, NULL), NULLIF(1, 1.0), NULLIF(1.50, 2),
                   NULLIF('it''s', 'it''s'), NULLIF('a', 'b'), NULLIF(TRUE, FALSE),
                   NULLIF(DATE '2024-01-01', DATE '2024-01-02')
            """)) {
      assertTrue(result.next());
      assertNull(result.getObject(1));
      assertTrue(result.wasNull());
      assertEquals(1L, result.getObject(2));
      assertFalse(result.wasNull());
      for (int column : List.of(3, 4, 6, 7, 9)) {
        assertNull(result.getObject(column));
      }
      assertEquals(1L, result.getObject(5));
      assertEquals(new BigDecimal("1.50"), result.getObject(8));
      assertEquals("a", result.getObject(10));
      assertEquals(true, result.getObject(11));
      assertEquals(Date.valueOf("2024-01-01"), result.getObject(12));
      assertFalse(result.next());
    }
  }

  /**
   * Evaluates current row values through filters, aggregates, and
   * materialization.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void evaluatesColumnArguments(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = jdbcUrl() + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      String body = "SELECT NULLIF(cyl, 4) AS x FROM mtcars";
      for (String sql : List.of("SELECT COUNT(NULLIF(cyl, 4)) FROM mtcars",
          "SELECT COUNT(*) FROM mtcars WHERE NULLIF(cyl, 4) IS NOT NULL", "SELECT COUNT(x) FROM (" + body + ") d",
          "WITH c AS (" + body + ") SELECT COUNT(x) FROM c",
          "SELECT COUNT(*) FROM mtcars a WHERE EXISTS (SELECT 1 FROM mtcars b "
              + "WHERE b.cyl = a.cyl AND NULLIF(a.cyl, 4) IS NOT NULL)")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(21, result.getInt(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      try (var result = statement.executeQuery(body + " ORDER BY cyl")) {
        for (int row = 0; row < 32; row++) {
          assertTrue(result.next());
          if (row < 11) {
            assertNull(result.getObject(1));
          } else {
            assertEquals(row < 18 ? 6 : 8, result.getObject(1));
          }
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Groups rows by evaluated NULLIF values, including one NULL group.
   *
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @Test
  void groupsByNullIf() throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT COUNT(*) FROM mtcars GROUP BY NULLIF(cyl, 4)")) {
      List<Integer> counts = new ArrayList<>();
      while (result.next()) {
        counts.add(result.getInt(1));
      }
      assertEquals(List.of(7, 11, 14), counts.stream().sorted().toList());
    }
  }

  /**
   * Rejects an invalid argument count with a classified, descriptive SQL error.
   *
   * @throws Exception
   *           if connection setup fails
   */
  @Test
  void rejectsInvalidArgumentCounts() throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl()); var statement = connection.createStatement()) {
      for (String expression : List.of("NULLIF()", "NULLIF(1)", "NULLIF(1, 2, 3)")) {
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var result = statement.executeQuery("SELECT " + expression)) {
            assertTrue(result.next());
            result.getObject(1);
          }
        }, expression);
        assertEquals("42000", failure.getSQLState());
        assertTrue(failure.getMessage().contains("NULLIF requires exactly two arguments"), failure::getMessage);
      }
    }
  }

  /**
   * Rejects aggregate modifiers on the scalar NULLIF function.
   *
   * @throws Exception
   *           if connection setup fails
   */
  @Test
  void rejectsInvalidModifiers() throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl()); var statement = connection.createStatement()) {
      for (String expression : List.of("NULLIF(DISTINCT cyl, 4)", "NULLIF(UNIQUE cyl, 4)", "NULLIF(*)")) {
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var result = statement.executeQuery("SELECT " + expression + " FROM mtcars LIMIT 1")) {
            assertTrue(result.next());
            result.getObject(1);
          }
        }, expression);
        assertEquals("42000", failure.getSQLState());
        assertTrue(failure.getMessage().contains("NULLIF does not support"), failure::getMessage);
      }
    }
  }

  /**
   * Locates the committed table fixture.
   *
   * @return the JDBC URL for its directory
   * @throws Exception
   *           if fixture lookup fails
   */
  private static String jdbcUrl() throws Exception {
    return "jdbc:jparq:" + Path.of(NullIfTest.class.getResource("/mtcars.parquet").toURI()).getParent();
  }
}
