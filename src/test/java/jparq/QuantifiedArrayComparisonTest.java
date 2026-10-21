package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.ExpressionEvaluator;

/** Regression coverage for quantified comparisons over SQL arrays. */
class QuantifiedArrayComparisonTest {

  /**
   * Checks every comparison operator, empty arrays, and SQL UNKNOWN without a
   * subquery executor.
   *
   * @param predicate
   *          SQL predicate
   * @param expected
   *          expected truth value, or null for UNKNOWN
   * @throws Exception
   *           if parsing or evaluation fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', quoteCharacter = '"', nullValues = "UNKNOWN", value = {
      "2 = ANY(ARRAY[1,2]) | true", "2 = SOME(ARRAY[1,3]) | false", "2 <> ALL(ARRAY[1,3]) | true",
      "2 != ANY(ARRAY[2,3]) | true", "2 < ANY(ARRAY[1,3]) | true", "2 <= ALL(ARRAY[2,3]) | true",
      "2 > SOME(ARRAY[1,3]) | true", "2 >= ALL(ARRAY[1,2]) | true", "ALL(ARRAY[1,2]) <= 2 | true",
      "ANY(ARRAY[1,3]) > 2 | true", "2 = ANY(ARRAY[]) | false", "2 = SOME(ARRAY[]) | false", "2 = ALL(ARRAY[]) | true",
      "NULL = ANY(ARRAY[]) | false", "NULL = ALL(ARRAY[]) | true", "2 = ANY(ARRAY[NULL,2]) | true",
      "2 = ANY(ARRAY[NULL,1]) | UNKNOWN", "2 = SOME(ARRAY[NULL]) | UNKNOWN", "2 = ALL(ARRAY[NULL,2]) | UNKNOWN",
      "2 = ALL(ARRAY[NULL,1]) | false", "NULL = ANY(ARRAY[1,2]) | UNKNOWN", "NULL = ALL(ARRAY[1,2]) | UNKNOWN",
      "2 = ANY(NULL) | UNKNOWN", "2 = ALL(NULL) | UNKNOWN", "NOT (2 = ANY(ARRAY[NULL,1])) | UNKNOWN",
      "(2 = ANY(ARRAY[NULL,1])) IS UNKNOWN | true", "2 = ANY(ARRAY[1,2.5]) | false",
      "'cat' = SOME(ARRAY['dog','cat']) | true"
  })
  void evaluatesArrayTruth(String predicate, Boolean expected) throws Exception {
    var schema = SchemaBuilder.record("empty").fields().endRecord();
    var evaluator = new ExpressionEvaluator(schema);
    assertEquals(expected,
        evaluator.evalTruth(CCJSqlParserUtil.parseCondExpression(predicate), new GenericData.Record(schema)),
        predicate);
  }

  /**
   * Reproduces issue 197 with pushdown both enabled and disabled and verifies
   * row-dependent arrays survive projection pruning and nested materialization.
   *
   * @param pushdown
   *          predicate and projection pushdown setting
   * @throws Exception
   *           if connection setup or querying fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void matchesInAndSubqueryResults(boolean pushdown) throws Exception {
    Path root = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (Connection connection = DriverManager
        .getConnection("jdbc:jparq:" + root + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown)) {
      List<String> expected = models(connection, "SELECT model FROM mtcars WHERE cyl IN (4,6) ORDER BY model");
      assertEquals(18, expected.size());
      for (String condition : List.of("cyl = ANY(ARRAY[4,6])", "cyl = SOME(ARRAY[4,6])", "cyl <> ALL(ARRAY[8])",
          "cyl = ANY(SELECT cyl FROM mtcars WHERE cyl IN (4,6))",
          "cyl = ANY(ARRAY(SELECT cyl FROM mtcars WHERE cyl IN (4,6)))")) {
        assertEquals(expected, models(connection, "SELECT model FROM mtcars WHERE " + condition + " ORDER BY model"));
      }
      try (var statement = connection.createStatement(); var result = statement.executeQuery("""
          SELECT (2 = ANY(ARRAY[1,2])) AS matched,
                 (2 = ALL(ARRAY[NULL,2])) AS unknown_value,
                 (2 = ALL(ARRAY[])) AS empty_all
          """)) {
        assertTrue(result.next());
        assertEquals(true, result.getObject(1));
        assertEquals(null, result.getObject(2));
        assertEquals(true, result.getObject(3));
        assertFalse(result.next());
      }
      List<String> all = models(connection, "SELECT model FROM mtcars ORDER BY model");
      assertEquals(all,
          models(connection, "SELECT model FROM mtcars WHERE cyl = ANY(ARRAY[cyl, cyl + 1]) ORDER BY model"));
      assertEquals(expected, models(connection, """
          WITH chosen AS (SELECT model FROM mtcars WHERE cyl = ANY(ARRAY[4,6]))
          SELECT model FROM (SELECT model FROM chosen) x ORDER BY model
          """));
      try (var statement = connection
          .prepareStatement("SELECT model FROM mtcars WHERE cyl = ANY(ARRAY[?,?]) ORDER BY model")) {
        statement.setInt(1, 4);
        statement.setInt(2, 6);
        try (var result = statement.executeQuery()) {
          List<String> actual = new ArrayList<>();
          while (result.next()) {
            actual.add(result.getString(1));
          }
          assertEquals(expected, actual);
        }
        statement.setInt(1, 8);
        statement.setInt(2, 8);
        try (var result = statement.executeQuery()) {
          int count = 0;
          while (result.next()) {
            count++;
          }
          assertEquals(14, count);
        }
      }
    }
  }

  /**
   * Invalid scalar operands must fail explicitly through JDBC.
   *
   * @param quantifier
   *          quantified comparison keyword
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "ANY", "SOME", "ALL"
  })
  void rejectsScalarOperands(String quantifier) throws Exception {
    Path root = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + root);
        var statement = connection.createStatement()) {
      SQLException exception = assertThrows(SQLException.class, () -> {
        try (var result = statement.executeQuery("SELECT model FROM mtcars WHERE cyl = " + quantifier + "(4)")) {
          result.next();
        }
      });
      Throwable cause = exception;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(cause.getMessage().contains("array operand"), cause.getMessage());
    }
  }

  /**
   * Reads model names in query order.
   *
   * @param connection
   *          open test connection
   * @param sql
   *          query returning model names
   * @return model names
   * @throws SQLException
   *           if query execution fails
   */
  private List<String> models(Connection connection, String sql) throws SQLException {
    List<String> models = new ArrayList<>();
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        models.add(result.getString(1));
      }
      assertFalse(result.next());
    }
    return models;
  }
}
