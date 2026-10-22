package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
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
      for (String sql : List.of("SELECT -'x'", "SELECT +'x'", "SELECT model FROM mtcars WHERE model = -'x'",
          "SELECT cyl FROM mtcars WHERE cyl > -'x'", "SELECT cyl FROM mtcars WHERE cyl + 0 > -'x'")) {
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, sql);
        assertEquals("42000", failure.getSQLState());
        assertTrue(failure.getMessage().contains("Unary sign requires"), failure::getMessage);
      }
      try (var prepared = connection.prepareStatement("SELECT -?")) {
        prepared.setString(1, "x");
        var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var result = prepared.executeQuery()) {
            assertTrue(result.next());
            result.getObject(1);
          }
        });
        assertEquals("42000", failure.getSQLState());
      }
    }
  }
}
