package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Locale;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression tests for local datetime value functions in nested expressions.
 */
class LocalDatetimeExpressionTest {

  /**
   * Evaluates local datetime keywords inside constructors and scalar functions
   * while reading a physical table.
   *
   * @param expression
   *          expression containing a local datetime keyword
   * @throws Exception
   *           if the query fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "LOCALTIMESTAMP", "LOCALTIME", "ARRAY[LOCALTIMESTAMP]", "ARRAY[LOCALTIME]",
      "COALESCE(LOCALTIMESTAMP, LOCALTIMESTAMP)", "COALESCE(LOCALTIME, LOCALTIME)", "COALESCE(NULL, localtimestamp)",
      "COALESCE(NULL, localtime)", "ARRAY[COALESCE(NULL, LOCALTIMESTAMP)]", "ARRAY[COALESCE(NULL, LOCALTIME)]"
  })
  void evaluatesNestedLocalDatetime(String expression) throws Exception {
    Path directory = Path.of(LocalDatetimeExpressionTest.class.getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression + " FROM mtcars LIMIT 1")) {
      assertTrue(result.next());
      Object value = result.getObject(1);
      if (expression.startsWith("ARRAY[")) {
        Object[] values = assertInstanceOf(Object[].class, value);
        value = values[0];
      }
      if (expression.toUpperCase(Locale.ROOT).contains("LOCALTIMESTAMP")) {
        assertTrue(value instanceof Timestamp || value instanceof LocalDateTime);
      } else {
        assertTrue(value instanceof Time || value instanceof LocalTime);
      }
      assertFalse(result.next());
    }
  }

  /**
   * Keeps delimited and qualified keyword-shaped names bound to their columns.
   *
   * @param expression
   *          column reference nested in a scalar function
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if the query fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "\"LOCALTIME\"", "\"LOCALTIMESTAMP\"", "t.\"LOCALTIME\"", "t.\"LOCALTIMESTAMP\""
  })
  void preservesColumnReferences(String expression, @TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(
            "SELECT COALESCE(" + expression + ", 0) FROM (SELECT 42 AS \"LOCALTIME\", 42 AS \"LOCALTIMESTAMP\") t")) {
      assertTrue(result.next());
      assertEquals(42, result.getInt(1));
      assertFalse(result.next());
    }
  }
}
