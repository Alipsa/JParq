package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.SqlTruth;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;

/** Tests SQL UNKNOWN preservation independently of Parquet pushdown. */
class SqlTruthTest {
  @org.junit.jupiter.api.io.TempDir
  java.nio.file.Path directory;

  /**
   * Verifies the complete AND, OR, and NOT truth tables against fixed
   * expectations.
   */
  @Test
  void truthTablesPreserveUnknown() {
    Boolean[] operands = {
        Boolean.TRUE, Boolean.FALSE, null
    };
    Boolean[][] and = {
        {
            true, false, null
        }, {
            false, false, false
        }, {
            null, false, null
        }
    };
    Boolean[][] or = {
        {
            true, true, true
        }, {
            true, false, null
        }, {
            true, null, null
        }
    };
    Boolean[] not = {
        false, true, null
    };
    for (int i = 0; i < operands.length; i++) {
      assertEquals(not[i], SqlTruth.not(operands[i]));
      for (int j = 0; j < operands.length; j++) {
        assertEquals(and[i][j], SqlTruth.and(operands[i], operands[j]));
        assertEquals(or[i][j], SqlTruth.or(operands[i], operands[j]));
      }
    }
  }

  /**
   * Exercises each documented regression without a Parquet predicate or NULL
   * oracle.
   */
  @Test
  void residualPredicatesExcludeUnknownRows() throws Exception {
    Schema schema = SchemaBuilder.record("nulls").fields().requiredInt("id").optionalInt("n").endRecord();
    var evaluator = new ExpressionEvaluator(schema);
    String[] predicates = {
        "n + 0 <> 3", "NOT (n + 0 = 3)", "NOT (n + 0 = 1 OR id = 4)", "NOT (n + 0 IN (1, 3))", "n NOT IN (3, NULL)",
        "NOT (CAST(n AS VARCHAR) LIKE '3%')", "CAST(n AS VARCHAR) NOT LIKE '3%'", "NOT (n = 1 AND id = 1)"
    };
    List<List<Integer>> expected = List.of(List.of(1, 4), List.of(1, 4), List.of(3), List.of(4), List.of(),
        List.of(1, 4), List.of(1, 4), List.of(2, 3, 4));
    for (int index = 0; index < predicates.length; index++) {
      var expression = CCJSqlParserUtil.parseCondExpression(predicates[index]);
      var rows = new java.util.ArrayList<Integer>();
      for (int id = 1; id <= 4; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("n", id == 2 ? null : Integer.valueOf(id));
        if (evaluator.eval(expression, row)) {
          rows.add(id);
        }
      }
      assertEquals(expected.get(index), rows, predicates[index]);
    }
  }

  /**
   * Confirms that boolean values remain NULL while CASE WHEN uses the
   * row-selection boundary.
   */
  @Test
  void projectionsAndCaseTreatUnknownCorrectly() throws Exception {
    Schema schema = SchemaBuilder.record("nullable").fields().optionalInt("n").endRecord();
    var row = new GenericData.Record(schema);
    var evaluator = new ExpressionEvaluator(schema);
    var values = new ValueExpressionEvaluator(schema);
    for (String expression : List.of("n = 1", "n <> 1", "NOT (n = 1)", "n NOT IN (1, NULL)", "n BETWEEN 1 AND 3",
        "n NOT LIKE '1%'", "n SIMILAR TO '1%'")) {
      var predicate = CCJSqlParserUtil.parseCondExpression(expression);
      assertNull(evaluator.evalTruth(predicate, row), expression);
      assertFalse(evaluator.eval(predicate, row), expression);
      assertNull(values.eval(predicate, row), expression);
    }
    assertEquals(0L, values.eval(CCJSqlParserUtil.parseExpression("CASE WHEN NOT (n = 1) THEN 1 ELSE 0 END"), row));
    for (String expression : Arrays.asList("n IS NULL", "n IS NOT DISTINCT FROM NULL")) {
      assertTrue(evaluator.eval(CCJSqlParserUtil.parseCondExpression(expression), row));
    }
  }

  /** Checks JDBC selection boundaries without relying on Parquet filtering. */
  @Test
  void whereJoinHavingAndQuantifiersUseSqlTruth() throws Exception {
    String input = "(VALUES (1, 1), (2, CAST(NULL AS INTEGER)), (3, 3), (4, 4)) AS a(id, n)";
    assertEquals(List.of(1L, 4L), query("SELECT id FROM " + input + " WHERE NOT (n = 3) ORDER BY id"));
    assertEquals(List.of(1L, 4L),
        query("SELECT MIN(id) FROM " + input + " GROUP BY n HAVING NOT (n = 3) ORDER BY MIN(id)"));
    assertEquals(List.of(),
        query("SELECT a.id FROM " + input + " JOIN (VALUES (1), (3)) AS b(n) ON NOT (a.n = b.n) WHERE a.id = 2"));
    assertEquals(List.of(), query("SELECT id FROM " + input
        + " WHERE NOT (n = ANY (SELECT n FROM (VALUES (1), (3), (CAST(NULL AS INTEGER))) AS b(n)))"));
    assertEquals(List.of(), query("SELECT id FROM " + input
        + " WHERE n NOT IN (SELECT n FROM (VALUES (1), (3), (CAST(NULL AS INTEGER))) AS b(n))"));
  }

  /** Collects a numeric JDBC projection from an in-memory VALUES query. */
  private List<Long> query(String sql) throws Exception {
    var result = new java.util.ArrayList<Long>();
    try (var connection = java.sql.DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      while (rows.next()) {
        result.add(rows.getLong(1));
      }
    }
    return result;
  }
}
