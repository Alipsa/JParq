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
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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
            "SELECT cyl FROM mtcars WHERE " + expression + " > 0 LIMIT 1",
            "SELECT cyl FROM mtcars WHERE mpg > " + expression + " LIMIT 1",
            "SELECT a.cyl FROM mtcars a JOIN mtcars b ON a.cyl = b.cyl WHERE b.mpg > " + expression + " LIMIT 1")) {
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
   * Reports range overflow through each JDBC evaluation and materialization path.
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
  void overflowingNumericCastsReportSqlState(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = jdbcUrl() + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String expression : List.of("CAST('1e10' AS INTEGER)", "CAST('300' AS TINYINT)",
          "CAST('-32769' AS SMALLINT)", "CAST('1e999999999' AS BIGINT)", "CAST('1e400' AS DOUBLE)",
          "CAST('-1e39' AS REAL)", "CAST(mpg * 1000000000000 AS INTEGER)")) {
        String body = "SELECT " + expression + " AS x FROM mtcars WHERE cyl = 6 LIMIT 1";
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c",
            "SELECT (" + body + ") AS x", "SELECT cyl FROM mtcars WHERE mpg > " + expression + " LIMIT 1",
            "SELECT a.cyl FROM mtcars a JOIN mtcars b ON a.cyl = b.cyl WHERE b.mpg > "
                + expression.replace("mpg *", "b.mpg *") + " LIMIT 1")) {
          var failure = assertThrows(SQLDataException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              assertTrue(result.next(), sql);
              result.getObject(1);
            }
          }, sql);
          assertEquals("22003", failure.getSQLState(), sql);
        }
        if (!expression.contains("mpg")) {
          var failure = assertThrows(SQLDataException.class, () -> {
            try (var result = statement.executeQuery("SELECT " + expression)) {
              assertTrue(result.next());
              result.getObject(1);
            }
          });
          assertEquals("22003", failure.getSQLState());
        }
      }
    }
  }

  /**
   * Documents the single-precision FLOAT mapping even with explicit precision.
   *
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @Test
  void precisionQualifiedFloatRetainsSinglePrecision() throws Exception {
    try (var connection = DriverManager.getConnection(jdbcUrl()); var statement = connection.createStatement()) {
      for (int precision : List.of(24, 25, 53)) {
        String body = "SELECT CAST(1.25 AS FLOAT(" + precision + ")) AS x FROM mtcars LIMIT 1";
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c")) {
          try (var result = statement.executeQuery(sql)) {
            assertEquals(Types.REAL, result.getMetaData().getColumnType(1), sql);
            assertTrue(result.next(), sql);
            assertEquals(Float.valueOf(1.25f), result.getObject(1), sql);
          }
        }
        try (var result = statement.executeQuery(body.replace("LIMIT 1", "WHERE 1 = 0"))) {
          assertEquals(Types.REAL, result.getMetaData().getColumnType(1));
          assertFalse(result.next());
        }
        String sql = "SELECT CAST(1e300 AS FLOAT(" + precision + ")) FROM mtcars LIMIT 1";
        var failure = assertThrows(SQLDataException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            assertTrue(result.next(), sql);
            result.getObject(1);
          }
        }, sql);
        assertEquals("22003", failure.getSQLState(), sql);
      }
    }
  }

  /**
   * Bounds numeric diagnostics when the large input is produced by an expression.
   *
   * @param target
   *          the numeric target type
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "INTEGER", "DECIMAL(5,0)"
  })
  void numericCastDiagnosticsBoundComputedInputs(String target) throws Exception {
    String sql = "SELECT CAST(REPEAT('9', 200000) AS " + target + ") FROM mtcars LIMIT 1";
    try (var connection = DriverManager.getConnection(jdbcUrl()); var statement = connection.createStatement()) {
      var failure = assertThrows(SQLDataException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next());
          result.getObject(1);
        }
      });
      assertEquals("22003", failure.getSQLState());
      assertTrue(failure.getMessage().length() < 512, () -> "Message length: " + failure.getMessage().length());
      assertTrue(failure.getMessage().contains("9".repeat(64) + "…"));
    }
  }

  /**
   * Checks declared target names through the SQL parser and JDBC error wrappers.
   *
   * @param target
   *          the declared cast type
   * @param displayedTarget
   *          the parser-rendered type name
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "DOUBLE PRECISION | DOUBLE PRECISION", "INT4 | INT4", "SIGNED | SIGNED", "FLOAT(53) | FLOAT (53)",
      "INT(11) | INT (11)"
  })
  void numericCastDiagnosticsRetainParsedTargetNames(String target, String displayedTarget) throws Exception {
    String sql = "SELECT CAST('1e400' AS " + target + ") FROM mtcars LIMIT 1";
    try (var connection = DriverManager.getConnection(jdbcUrl()); var statement = connection.createStatement()) {
      var failure = assertThrows(SQLDataException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next());
          result.getObject(1);
        }
      });
      assertEquals("22003", failure.getSQLState());
      assertTrue(failure.getMessage().contains("to " + displayedTarget + ": value out of range"), failure::getMessage);
    }
  }

  /**
   * Preserves range errors while planning either participant's pushed join
   * predicate.
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
  void joinPlanningRetainsNumericRangeState(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = jdbcUrl() + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String qualifier : List.of("a", "b")) {
        String sql = "SELECT a.cyl FROM mtcars a JOIN mtcars b ON a.cyl = b.cyl WHERE " + qualifier
            + ".mpg > CAST(123.45 AS DECIMAL(2,1))";
        var failure = assertThrows(SQLDataException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals("22003", failure.getSQLState(), sql);
        if (predicatePushdown) {
          assertTrue(failure.getMessage().contains("Failed to prepare query: " + sql), failure::getMessage);
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
