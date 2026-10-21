package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.JSqlParserSupport;

/** Regression coverage for quantified comparisons over SQL arrays. */
class QuantifiedArrayComparisonTest {

  /** Verifies null-safe recognition of parser-created function operands. */
  @Test
  void recognizesQuantifierNamesSafely() {
    assertFalse(JSqlParserSupport.isQuantifiedOperand(null));
    assertFalse(JSqlParserSupport.isQuantifiedOperand(new Function()));
    assertFalse(JSqlParserSupport.isQuantifiedOperand(new Function().withName("ABS")));
    for (String name : List.of("any", "SoMe", "ALL")) {
      assertTrue(JSqlParserSupport.isQuantifiedOperand(new Function().withName(name)));
    }
  }

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
   * Keeps NOT outside quantified comparisons during predicate normalization.
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
  void negatedQuantifiersMatchScalarPredicates(boolean pushdown) throws Exception {
    Path root = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager
        .getConnection("jdbc:jparq:" + root + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown)) {
      for (String quantifier : List.of("ANY", "SOME", "ALL")) {
        boolean all = "ALL".equals(quantifier);
        for (String[] conditions : List.of(new String[]{
            "NOT (cyl = " + quantifier + "(ARRAY[4,6]))", all ? "1=1" : "cyl NOT IN (4,6)"
        }, new String[]{
            "NOT (cyl < " + quantifier + "(ARRAY[6,8]))", all ? "cyl >= 6" : "cyl >= 8"
        }, new String[]{
            "NOT (cyl > " + quantifier + "(ARRAY[4,6]))", all ? "cyl <= 6" : "cyl <= 4"
        }, new String[]{
            "NOT (" + quantifier + "(ARRAY[4,6]) = cyl)", all ? "1=1" : "cyl NOT IN (4,6)"
        }, new String[]{
            "NOT (cyl = " + quantifier + "(ARRAY[]))", all ? "1=0" : "1=1"
        }, new String[]{
            "NOT (cyl = " + quantifier + "(ARRAY[NULL,4]))", all ? "cyl <> 4" : "1=0"
        })) {
          for (String suffix : List.of("", " AND mpg > 0")) {
            String query = "SELECT model FROM mtcars WHERE " + conditions[0] + suffix + " ORDER BY model";
            assertEquals(
                models(connection, "SELECT model FROM mtcars WHERE " + conditions[1] + suffix + " ORDER BY model"),
                models(connection, query), query);
          }
        }
      }
    }
  }

  /**
   * Invalid quantified operands must raise a JDBC error instead of losing rows.
   *
   * @param predicate
   *          invalid comparison
   * @param expectedMessage
   *          expected root-cause message
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "ANY(ARRAY[4,6]) = ANY(ARRAY[6]) | requires a scalar operand",
      "cyl = ANY(ARRAY[[4],[6]]) | requires scalar array elements",
      "cyl = SOME(ARRAY[[4],[6]]) | requires scalar array elements",
      "cyl = ALL(ARRAY[[4],[6]]) | requires scalar array elements", "cyl = ANY() | requires exactly one array operand",
      "cyl = ANY(ARRAY[4],ARRAY[6]) | requires exactly one array operand"
  })
  void rejectsInvalidComparisons(String predicate, String expectedMessage) throws Exception {
    Path root = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    for (boolean pushdown : List.of(true, false)) {
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + root + "?predicatePushdown=" + pushdown);
          var statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery("SELECT model FROM mtcars WHERE " + predicate)) {
            result.next();
          }
        }, predicate);
        Throwable cause = rootCause(exception);
        assertInstanceOf(IllegalArgumentException.class, cause, predicate);
        assertTrue(cause.getMessage().contains(expectedMessage), predicate + ": " + cause.getMessage());
      }
    }
  }

  /**
   * Rejects Avro records inside array columns before attempting scalar
   * comparison.
   *
   * @param quantifier
   *          quantified comparison keyword
   * @throws Exception
   *           if expression parsing fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "ANY", "SOME", "ALL"
  })
  void rejectsStructArrayElements(String quantifier) throws Exception {
    var elementSchema = SchemaBuilder.record("element").fields().requiredInt("value").endRecord();
    var schema = SchemaBuilder.record("struct_arrays").fields().name("items").type().array().items(elementSchema)
        .noDefault().endRecord();
    var element = new GenericData.Record(elementSchema);
    element.put("value", 4);
    var record = new GenericData.Record(schema);
    record.put("items", List.of(element));
    var evaluator = new ExpressionEvaluator(schema);
    var predicate = CCJSqlParserUtil.parseCondExpression("4 = " + quantifier + "(items)");
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> evaluator.evalTruth(predicate, record));
    assertEquals("ANY/SOME/ALL requires scalar array elements", exception.getMessage());
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
      Throwable cause = rootCause(exception);
      assertTrue(cause.getMessage().contains("array operand"), cause.getMessage());
    }
  }

  /**
   * Unwraps JDBC execution errors to their original cause.
   *
   * @param exception
   *          exception to unwrap
   * @return deepest cause
   */
  private static Throwable rootCause(Throwable exception) {
    Throwable cause = exception;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
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
