package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies unary signs behave consistently with and without pushdown. */
class UnarySignTest {
  /**
   * Converts numeric text in literals, parameters, and predicates.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void convertsNumericText(boolean pushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown);
        var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT -'5', +'5'")) {
        assertTrue(result.next());
        assertEquals(new BigDecimal("-5"), result.getBigDecimal(1));
        assertEquals(new BigDecimal("5"), result.getBigDecimal(2));
        assertFalse(result.next());
      }
      try (var prepared = connection.prepareStatement("SELECT -?, +?")) {
        prepared.setString(1, "5");
        prepared.setString(2, "5");
        try (var result = prepared.executeQuery()) {
          assertTrue(result.next());
          assertEquals(new BigDecimal("-5"), result.getBigDecimal(1));
          assertEquals(new BigDecimal("5"), result.getBigDecimal(2));
        }
      }
      for (String predicate : List.of("cyl > -'5'", "cyl + 0 > -'5'")) {
        try (var result = statement.executeQuery("SELECT COUNT(*) FROM mtcars WHERE " + predicate)) {
          assertTrue(result.next());
          assertEquals(32, result.getInt(1));
          assertFalse(result.next());
        }
      }
      try (var prepared = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE cyl > -?")) {
        prepared.setString(1, "5");
        try (var result = prepared.executeQuery()) {
          assertTrue(result.next());
          assertEquals(32, result.getInt(1));
        }
      }
    }
  }

  /**
   * Rejects nonnumeric text even when the comparison could otherwise be pushed.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void rejectsNonnumericText(boolean pushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT -'x'", "SELECT +'x'", "SELECT -model FROM mtcars LIMIT 1",
          "SELECT model FROM mtcars WHERE model = -'x'", "SELECT cyl FROM mtcars WHERE cyl > -'x'",
          "SELECT cyl FROM mtcars WHERE cyl + 0 > -'x'", "SELECT cyl FROM mtcars WHERE cyl NOT IN (6, -'x')")) {
        var failure = assertThrows(SQLDataException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals("22018", failure.getSQLState());
        assertTrue(failure.getMessage().contains("Cannot convert unary sign operand"), failure::getMessage);
        assertFalse(failure.getMessage().contains("Mazda RX4"), failure::getMessage);
        if (sql.contains("-model")) {
          assertTrue(failure.getMessage().contains("-model"), failure::getMessage);
        }
      }
      try (var prepared = connection.prepareStatement("SELECT -?")) {
        prepared.setString(1, "x");
        var failure = assertThrows(SQLDataException.class, () -> {
          try (var result = prepared.executeQuery()) {
            assertTrue(result.next());
            result.getObject(1);
          }
        });
        assertEquals("22018", failure.getSQLState());
      }
    }
  }

  /**
   * Leaves row-dependent IN operands for evaluation against the current row.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture lookup or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void evaluatesNegatedColumnInLists(boolean pushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown)) {
      for (String predicate : List.of("cyl NOT IN (6, -mpg)", "NOT (cyl IN (6, -mpg))", "cyl IN (6, -mpg)")) {
        try (
            var statement = connection
                .prepareStatement("SELECT DISTINCT cyl FROM mtcars WHERE " + predicate + " ORDER BY cyl");
            var result = statement.executeQuery()) {
          List<Integer> cylinders = new ArrayList<>();
          while (result.next()) {
            cylinders.add(result.getInt(1));
          }
          assertEquals(predicate.startsWith("cyl IN") ? List.of(6) : List.of(4, 8), cylinders, predicate);
        }
      }
      try (var statement = connection.prepareStatement("SELECT cyl FROM mtcars WHERE cyl NOT IN (6, -'x')")) {
        var failure = assertThrows(SQLDataException.class, () -> {
          try (var result = statement.executeQuery()) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        });
        assertEquals("22018", failure.getSQLState());
      }
      try (
          var statement = connection
              .prepareStatement("SELECT cyl FROM (SELECT cyl FROM mtcars WHERE 1 = 0) t WHERE cyl NOT IN (6, -'x')");
          var result = statement.executeQuery()) {
        assertFalse(result.next());
      }
    }
  }

  /**
   * Keeps unsupported operand types classified as syntax failures.
   *
   * @param expression
   *          signed expression with an invalid operand type
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "-true", "-DATE '2024-01-01'"
  })
  void rejectsInvalidOperandTypes(String expression) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
        try (var result = statement.executeQuery("SELECT " + expression)) {
          assertTrue(result.next());
          result.getObject(1);
        }
      });
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains(expression), failure::getMessage);
    }
  }

  /**
   * Omits internal binding functions from correlated conversion diagnostics.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture lookup or connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void hidesCorrelatedBindingsInDiagnostics(boolean pushdown) throws Exception {
    Path directory = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown);
        var statement = connection.createStatement()) {
      for (String operand : List.of("-a.model", "-COALESCE(a.model, 'x')", "-(a.model)")) {
        var failure = assertThrows(SQLDataException.class, () -> {
          try (var result = statement.executeQuery(
              "SELECT cyl FROM mtcars a WHERE EXISTS " + "(SELECT 1 FROM mtcars b WHERE b.cyl = " + operand + ")")) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, operand);
        assertEquals("22018", failure.getSQLState());
        assertTrue(failure.getMessage().contains("Cannot convert unary sign operand to a numeric value"),
            failure::getMessage);
        assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure::getMessage);
      }
    }
  }

}
