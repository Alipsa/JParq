package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies numeric conversion failures at JDBC evaluation boundaries. */
class NumericCastFailureTest {
  /**
   * Rejects invalid and non-finite numeric inputs consistently through
   * materialization.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void invalidNumericCastsReportSqlState(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = jdbcUrl() + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String expression : List.of("CAST('Infinity' AS DOUBLE)", "CAST('-Infinity' AS DOUBLE PRECISION)",
          "CAST('NaN' AS FLOAT8)", "CAST('Infinity' AS REAL)", "CAST('NaN' AS FLOAT4)", "CAST('-Infinity' AS FLOAT)",
          "CAST('NaN' AS DECIMAL(8,2))", "CAST('Infinity' AS INTEGER)", "CAST('not a number' AS DOUBLE)")) {
        String body = "SELECT " + expression + " AS x FROM mtcars WHERE cyl = 6 LIMIT 1";
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c",
            "SELECT (" + body + ") AS x", "SELECT " + expression + " AS x",
            "SELECT cyl FROM mtcars WHERE " + expression + " > 0 LIMIT 1")) {
          SQLDataException failure = assertThrows(SQLDataException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              assertTrue(result.next(), sql);
              result.getObject(1);
            }
          }, sql);
          assertEquals("22018", failure.getSQLState(), sql);
          assertTrue(failure.getMessage().contains("numeric"), failure::getMessage);
          Throwable root = failure;
          while (root.getCause() != null) {
            root = root.getCause();
          }
          assertInstanceOf(NumberFormatException.class, root, sql);
        }
      }
    }
  }

  /**
   * Ensures every projection getter exposes the classified JDBC failure.
   *
   * @throws Exception
   *           if fixture lookup or query setup fails
   */
  @Test
  void projectionGettersWrapNumericFailures() throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CAST('Infinity' AS DOUBLE) AS x FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      assertEquals("22018", assertThrows(SQLDataException.class, () -> result.getObject(1)).getSQLState());
      assertEquals("22018", assertThrows(SQLDataException.class, () -> result.getObject("x")).getSQLState());
      assertEquals("22018", assertThrows(SQLDataException.class, () -> result.getDouble(1)).getSQLState());
      assertEquals("22018", assertThrows(SQLDataException.class, () -> result.getFloat("x")).getSQLState());
      assertEquals("22018", assertThrows(SQLDataException.class, () -> result.getString(1)).getSQLState());
      assertEquals("22018", assertThrows(SQLDataException.class, () -> result.getBigDecimal("x")).getSQLState());
    }
  }

  /**
   * Evaluates only the selected CASE branch, including failures on later rows.
   *
   * @throws Exception
   *           if fixture lookup or query setup fails
   */
  @Test
  void caseOnlyRejectsTheSelectedNonFiniteBranch() throws Exception {
    String body = "SELECT CASE WHEN cyl = 4 THEN 1.5 ELSE CAST('Infinity' AS DOUBLE) END AS x FROM mtcars";
    try (var connection = DriverManager.getConnection(jdbcUrl()); var statement = connection.createStatement()) {
      for (String sql : List.of(body + " WHERE cyl = 4 LIMIT 1", "SELECT x FROM (" + body + " WHERE cyl = 4 LIMIT 1) d",
          "WITH c AS (" + body + " WHERE cyl = 4 LIMIT 1) SELECT x FROM c")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1.5, result.getDouble(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      for (String sql : List.of(body + " WHERE cyl = 6 LIMIT 1", "SELECT x FROM (" + body + " WHERE cyl = 6 LIMIT 1) d",
          "WITH c AS (" + body + " WHERE cyl = 6 LIMIT 1) SELECT x FROM c")) {
        SQLException failure = assertThrows(SQLDataException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            assertTrue(result.next(), sql);
            result.getObject(1);
          }
        }, sql);
        assertEquals("22018", failure.getSQLState(), sql);
      }
      try (var result = statement.executeQuery(body + " WHERE cyl IN (4,6) ORDER BY cyl")) {
        for (int row = 0; row < 11; row++) {
          assertTrue(result.next());
          assertEquals(1.5, result.getDouble(1));
        }
        assertTrue(result.next());
        assertEquals("22018", assertThrows(SQLDataException.class, () -> result.getObject(1)).getSQLState());
      }
    }
  }

  /**
   * Locates the committed Parquet fixture.
   *
   * @return the fixture directory's JDBC URL
   * @throws Exception
   *           if fixture lookup fails
   */
  private static String jdbcUrl() throws Exception {
    return "jdbc:jparq:" + Path.of(NumericCastFailureTest.class.getResource("/mtcars.parquet").toURI()).getParent();
  }
}
