package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.CorrelationMappings;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.SqlComparisons;
import se.alipsa.jparq.engine.SqlTruth;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.engine.window.WindowState;

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
      assertEquals(Boolean.TRUE.equals(operands[i]), SqlTruth.is(operands[i], true, false));
      assertEquals(!Boolean.TRUE.equals(operands[i]), SqlTruth.is(operands[i], true, true));
      assertEquals(Boolean.FALSE.equals(operands[i]), SqlTruth.is(operands[i], false, false));
      assertEquals(!Boolean.FALSE.equals(operands[i]), SqlTruth.is(operands[i], false, true));
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

  /**
   * Confirms that NaN is the largest value in quantified comparisons, so a NaN
   * operand is decisive instead of UNKNOWN.
   */
  @Test
  void quantifiedComparisonsOrderNaNAboveNumbers() throws Exception {
    String input = "(VALUES (1), (2), (3), (4)) AS t(id)";
    String others = "(VALUES (1.0)) AS t2(x)";
    String caseExpr = "CASE WHEN id = 1 THEN ACOS(2) ELSE 2.0 END";
    assertEquals(List.of(1L, 2L, 3L, 4L),
        query("SELECT id FROM " + input + " WHERE " + caseExpr + " > ALL (SELECT x FROM " + others + ") ORDER BY id"));
    assertEquals(List.of(1L, 2L, 3L, 4L),
        query("SELECT id FROM " + input + " WHERE " + caseExpr + " > ANY (SELECT x FROM " + others + ") ORDER BY id"));
    String larger = "(VALUES (3.0)) AS t3(x)";
    assertEquals(List.of(1L),
        query("SELECT id FROM " + input + " WHERE " + caseExpr + " > ALL (SELECT x FROM " + larger + ") ORDER BY id"));
    assertEquals(List.of(2L, 3L, 4L),
        query("SELECT id FROM " + input + " WHERE " + caseExpr + " < ALL (SELECT x FROM " + larger + ") ORDER BY id"));
  }

  /**
   * Confirms that NOT IN against an empty subquery keeps every group in HAVING,
   * mirroring the WHERE boundary.
   */
  @Test
  void havingInEmptySubqueryKeepsAllGroups() throws Exception {
    String input = "(VALUES (1, 1), (2, 2)) AS t(g, n)";
    String others = "(VALUES (1)) AS t2(x)";
    assertEquals(List.of(1L, 2L), query("SELECT g FROM " + input + " GROUP BY g HAVING MAX(n) NOT IN (SELECT x FROM "
        + others + " WHERE 1 = 0) ORDER BY g"));
    assertEquals(List.of(),
        query("SELECT g FROM " + input + " GROUP BY g HAVING MAX(n) IN (SELECT x FROM " + others + " WHERE 1 = 0)"));
  }

  /**
   * Confirms that boolean columns work as Boolean projections, including NULL
   * (UNKNOWN) results.
   */
  @Test
  void booleanColumnsWorkInBooleanProjections() throws Exception {
    String input = "(VALUES (TRUE, 1), (FALSE, 2), (CAST(NULL AS BOOLEAN), 3)) AS t(active, n)";
    assertEquals(Arrays.asList(false, true, null), queryObjects("SELECT NOT active FROM " + input));
    assertEquals(Arrays.asList(false, false, null), queryObjects("SELECT active AND n > 1 FROM " + input));
    String nullInput = "(VALUES (CAST(NULL AS BOOLEAN))) AS t(active)";
    assertEquals(List.of(), query("SELECT 1 FROM " + nullInput + " WHERE active"));
    assertEquals(List.of(1L), query("SELECT 1 FROM " + nullInput + " WHERE active IS NOT TRUE"));
    assertEquals(List.of(), query("SELECT 1 FROM " + nullInput + " WHERE NULL"));
    assertEquals(Arrays.asList(null, null),
        queryObjects("SELECT NOT active FROM (VALUES (NULL), (CAST(NULL AS BOOL))) AS t(active)"));
  }

  /**
   * Confirms that non-Boolean columns and operators are rejected even when their
   * operands are NULL.
   */
  @Test
  void invalidPredicatesThrowInsteadOfSilentlyExcludingRows() throws Exception {
    Schema schema = SchemaBuilder.record("mix").fields().requiredInt("id").optionalInt("n").optionalString("s")
        .endRecord();
    var row = new GenericData.Record(schema);
    row.put("id", 1);
    row.put("n", Integer.valueOf(1));
    row.put("s", "a");
    var evaluator = new ExpressionEvaluator(schema);
    for (String expression : List.of("n", "s", "1", "n + 1", "s || 'x'")) {
      var predicate = CCJSqlParserUtil.parseCondExpression(expression);
      assertThrows(IllegalArgumentException.class, () -> evaluator.eval(predicate, row), expression);
      var nullRow = new GenericData.Record(schema);
      nullRow.put("id", 1);
      assertThrows(IllegalArgumentException.class, () -> evaluator.eval(predicate, nullRow), expression);
    }
    for (String input : List.of("(VALUES (1, 1, 'a'), (2, 2, 'b')) AS t(id, n, s)",
        "(VALUES (1, CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR))) AS t(id, n, s)")) {
      for (String expression : List.of("n", "t.n", "s", "1", "n + 1", "s || 'x'")) {
        assertThrows(java.sql.SQLException.class, () -> query("SELECT id FROM " + input + " WHERE " + expression));
        assertThrows(java.sql.SQLException.class,
            () -> query("SELECT id FROM " + input + " GROUP BY id, n, s HAVING " + expression));
      }
      assertThrows(IllegalArgumentException.class, () -> query("SELECT CASE WHEN n THEN 1 ELSE 0 END FROM " + input));
      assertThrows(IllegalArgumentException.class, () -> queryObjects("SELECT NOT s FROM " + input));
    }
  }

  /** Checks that operator validation precedes UNKNOWN propagation. */
  @Test
  void unsupportedOperatorsThrowForUnknownComparisons() {
    for (String operator : List.of("+", "-", "*", "/", "||")) {
      assertThrows(IllegalArgumentException.class, () -> SqlComparisons.compare(operator, null), operator);
      assertThrows(IllegalArgumentException.class, () -> SqlComparisons.compare(operator, 0), operator);
    }
    for (String operator : List.of("=", "!=", "<>", "<", ">", "<=", ">=")) {
      assertNull(SqlComparisons.compare(operator, null), operator);
    }
  }

  /** Ensures that IN stops evaluating candidates after a decisive match. */
  @Test
  void inListsSkipCandidatesAfterFirstMatch() throws Exception {
    String input = "(VALUES (1)) AS t(id)";
    String candidates = "(NULL, 1, CAST('invalid' AS INTEGER))";
    assertEquals(List.of(1L), query("SELECT id FROM " + input + " WHERE id IN " + candidates));
    assertEquals(List.of(), query("SELECT id FROM " + input + " WHERE id NOT IN " + candidates));
    assertEquals(List.of(1L), query("SELECT MAX(id) FROM " + input + " HAVING MAX(id) IN " + candidates));
    assertEquals(List.of(), query("SELECT MAX(id) FROM " + input + " HAVING MAX(id) NOT IN " + candidates));
  }

  /** Checks bare Boolean grouping columns with TRUE, FALSE, and NULL values. */
  @Test
  void havingAcceptsBooleanGroupingColumns() throws Exception {
    String input = "(VALUES (1, TRUE), (2, FALSE), (3, CAST(NULL AS BOOLEAN))) AS t(id, active)";
    String grouped = "SELECT id FROM " + input + " GROUP BY id, active HAVING ";
    assertEquals(List.of(1L), query(grouped + "active ORDER BY id"));
    assertEquals(List.of(1L), query(grouped + "t.active ORDER BY id"));
    assertEquals(List.of(2L), query(grouped + "NOT active ORDER BY id"));
    assertEquals(List.of(1L), query(grouped + "active AND id > 0 ORDER BY id"));
    assertEquals(List.of(2L, 3L), query(grouped + "active IS NOT TRUE ORDER BY id"));
    assertEquals(List.of(1L),
        query("SELECT id FROM " + input + " WHERE id > 0 GROUP BY id, active HAVING t.ACTIVE ORDER BY id"));
    String nullInput = "(VALUES (1, CAST(NULL AS BOOLEAN))) AS t(id, active)";
    assertEquals(List.of(), query("SELECT id FROM " + nullInput + " GROUP BY id, active HAVING active"));
    assertEquals(List.of(1L), query("SELECT id FROM " + nullInput + " GROUP BY id, active HAVING active IS NOT TRUE"));
  }

  /** Checks NULL membership without evaluating any candidate expressions. */
  @Test
  void nullInOperandsSkipCandidateEvaluation() throws Exception {
    for (boolean negated : List.of(false, true)) {
      assertNull(SqlTruth.in(null, List.of(1, 2), candidate -> {
        throw new AssertionError("A NULL operand must not evaluate candidates");
      }, negated));
      assertEquals(negated, SqlTruth.in(null, List.of(), negated));
      assertNull(SqlTruth.in(null, List.of(1, 2), negated));
    }
    String input = "(VALUES (1, CAST(NULL AS INTEGER))) AS t(id, n)";
    String candidates = "(CAST('invalid' AS INTEGER), 1)";
    for (String operator : List.of("IN", "NOT IN")) {
      assertEquals(List.of(), query("SELECT id FROM " + input + " WHERE n " + operator + " " + candidates));
      assertEquals(List.of(), query("SELECT MAX(n) FROM " + input + " HAVING MAX(n) " + operator + " " + candidates));
    }
  }

  /**
   * Confirms that IS [NOT] TRUE/FALSE inspects the three-valued truth of the
   * operand, including UNKNOWN.
   */
  @Test
  void isTruePredicatesFollowThreeValuedLogic() throws Exception {
    String input = "(VALUES (1, 1), (2, CAST(NULL AS INTEGER)), (3, 3), (4, 4)) AS t(id, n)";
    assertEquals(List.of(3L, 4L), query("SELECT id FROM " + input + " WHERE (n > 1) IS TRUE ORDER BY id"));
    assertEquals(List.of(1L, 2L), query("SELECT id FROM " + input + " WHERE (n > 1) IS NOT TRUE ORDER BY id"));
    assertEquals(List.of(1L), query("SELECT id FROM " + input + " WHERE (n > 1) IS FALSE ORDER BY id"));
    assertEquals(List.of(2L, 3L, 4L), query("SELECT id FROM " + input + " WHERE (n > 1) IS NOT FALSE ORDER BY id"));
    assertEquals(List.of(false, true, true, true), queryObjects("SELECT (n > 1) IS NOT FALSE FROM " + input));
    assertEquals(List.of(3L, 4L),
        query("SELECT id FROM " + input + " GROUP BY id HAVING (MAX(n) > 1) IS TRUE ORDER BY id"));
    assertEquals(List.of(1L, 2L),
        query("SELECT id FROM " + input + " GROUP BY id HAVING (MAX(n) > 1) IS NOT TRUE ORDER BY id"));
  }

  /**
   * Confirms that BETWEEN with a NULL bound follows the AND truth table, so NOT
   * BETWEEN can be TRUE when one bound comparison is FALSE.
   */
  @Test
  void betweenWithNullBoundFollowsAndTruthTable() throws Exception {
    String input = "(VALUES (1), (2), (3), (4)) AS t(id)";
    assertEquals(List.of(3L, 4L),
        query("SELECT id FROM " + input + " WHERE id + 0 NOT BETWEEN CAST(NULL AS INTEGER) AND 2 ORDER BY id"));
    assertEquals(List.of(3L, 4L),
        query("SELECT id FROM " + input + " WHERE NOT (id + 0 >= CAST(NULL AS INTEGER) AND id + 0 <= 2) ORDER BY id"));
    String groups = "(VALUES (1, 1), (2, 2), (3, 3), (4, 4)) AS t(g, n)";
    assertEquals(List.of(3L, 4L), query(
        "SELECT g FROM " + groups + " GROUP BY g HAVING MAX(n) NOT BETWEEN CAST(NULL AS INTEGER) AND 2 ORDER BY g"));
    assertEquals(List.of(3L, 4L), query("SELECT g FROM " + groups
        + " GROUP BY g HAVING NOT (MAX(n) >= CAST(NULL AS INTEGER) AND MAX(n) <= 2) ORDER BY g"));
    assertEquals(List.of(1L),
        query("SELECT id FROM " + input + " WHERE id + 0 NOT BETWEEN 2 AND CAST(NULL AS INTEGER) ORDER BY id"));
    assertEquals(List.of(),
        query("SELECT id FROM " + input + " WHERE id + 0 NOT BETWEEN CAST(NULL AS INTEGER) AND CAST(NULL AS INTEGER)"));
    assertEquals(List.of(false, false, false, false), queryObjects("SELECT id + 0 BETWEEN 5 AND NULL FROM " + input));
  }

  /**
   * Confirms that NaN is ordered above every number and equals itself in IN,
   * BETWEEN, DISTINCT, HAVING, and subquery predicates.
   */
  @Test
  void nanComparisonsAreOrderedAboveNumbersAcrossAllPredicateKinds() throws Exception {
    String input = "(VALUES (1), (2)) AS t(id)";
    assertEquals(List.of(), query("SELECT id FROM " + input + " WHERE ACOS(2) IN (1.0, 2.0)"));
    assertEquals(List.of(1L, 2L), query("SELECT id FROM " + input + " WHERE ACOS(2) NOT IN (1.0, 2.0)"));
    assertEquals(List.of(), query("SELECT id FROM " + input + " WHERE ACOS(2) BETWEEN 0 AND 5"));
    assertEquals(List.of(1L, 2L), query("SELECT id FROM " + input + " WHERE ACOS(2) NOT BETWEEN 0 AND 5"));
    assertEquals(List.of(1L, 2L), query("SELECT id FROM " + input + " WHERE ACOS(2) IS NOT DISTINCT FROM ACOS(2)"));
    assertEquals(List.of(), query("SELECT id FROM " + input + " WHERE ACOS(2) IS DISTINCT FROM ACOS(2)"));
    assertEquals(List.of(1L, 2L), query("SELECT id FROM " + input + " WHERE 2.0 IN (1.0, 2.0)"));
    assertEquals(List.of(1L, 2L), query("SELECT id FROM " + input + " WHERE 2.0 BETWEEN 0 AND 5"));
    assertEquals(List.of(1L, 2L), query("SELECT id FROM " + input + " WHERE ACOS(2) = ACOS(2)"));
    assertEquals(List.of(1L, 2L), query("SELECT id FROM " + input + " WHERE ACOS(2) > 1.0E300"));
    assertEquals(List.of(), query("SELECT id FROM " + input + " WHERE ACOS(2) < 1.0E300"));
    String doubles = "(VALUES (1, CAST(1 AS DOUBLE)), (2, ACOS(2)), "
        + "(3, CAST(2 AS DOUBLE)), (4, CAST(4 AS DOUBLE))) AS t(id, dbl)";
    assertEquals(List.of(1L, 3L), query("SELECT id FROM " + doubles + " WHERE dbl IN (1.0, 2.0) ORDER BY id"));
    assertEquals(List.of(1L, 2L, 3L, 4L),
        query("SELECT id FROM " + doubles + " WHERE dbl IS NOT DISTINCT FROM dbl ORDER BY id"));
    assertEquals(List.of(1L, 2L, 3L, 4L),
        query("SELECT id FROM " + doubles + " GROUP BY id HAVING MAX(dbl) > 0 ORDER BY id"));
    assertEquals(List.of(1L, 3L),
        query("SELECT id FROM " + doubles + " GROUP BY id HAVING MAX(dbl) IN (1.0, 2.0) ORDER BY id"));
    assertEquals(List.of(1L, 3L, 4L),
        query("SELECT id FROM " + doubles + " GROUP BY id HAVING MAX(dbl) BETWEEN 0 AND 5 ORDER BY id"));
    String candidates = "(SELECT candidate FROM (VALUES (CAST(1 AS DOUBLE)), (CAST(2 AS DOUBLE)), "
        + "(ACOS(2))) AS q(candidate))";
    assertEquals(List.of(1L, 2L, 3L),
        query("SELECT id FROM " + doubles + " WHERE dbl IN " + candidates + " ORDER BY id"));
    assertEquals(List.of(4L), query("SELECT id FROM " + doubles + " WHERE dbl NOT IN " + candidates));
    assertEquals(List.of(1L, 2L, 3L),
        query("SELECT id FROM " + doubles + " GROUP BY id HAVING MAX(dbl) IN " + candidates + " ORDER BY id"));
    assertEquals(List.of(4L), query("SELECT id FROM " + doubles + " GROUP BY id HAVING MAX(dbl) NOT IN " + candidates));
    assertEquals(List.of(1L, 3L, 4L, 2L), query("SELECT id FROM " + doubles + " ORDER BY dbl"));
    assertEquals(List.of(2L, 4L, 3L, 1L), query("SELECT id FROM " + doubles + " ORDER BY dbl DESC"));
  }

  /**
   * Checks the shared numeric order: -Infinity, finite values, +Infinity, NaN,
   * with NaN equal to NaN and -0.0 equal to 0.0 for every numeric type.
   */
  @Test
  void numbersFollowOneTotalOrder() {
    Number[] ascending = {
        Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY, -1e300, -1, -0.5f, 0, 1L, 1.5, new java.math.BigDecimal("2"),
        1e300, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, Double.NaN, Float.NaN
    };
    for (int i = 0; i < ascending.length; i++) {
      for (int j = 0; j < ascending.length; j++) {
        int expected = Integer.signum(Integer.compare(rank(i), rank(j)));
        assertEquals(expected, Integer.signum(SqlComparisons.typedCompare(ascending[i], ascending[j])),
            ascending[i] + " vs " + ascending[j]);
        assertEquals(expected, Integer.signum(SqlComparisons.compareNumbers(ascending[i], ascending[j])));
        assertEquals(expected, Integer.signum(ExpressionEvaluator.typedCompare(ascending[i], ascending[j])));
        assertEquals(expected, Integer.signum(SqlComparisons.compareOrUnknown(ascending[i], ascending[j])));
      }
    }
    assertEquals(0, SqlComparisons.compareNumbers(-0.0, 0.0));
    assertEquals(0, SqlComparisons.compareNumbers(0.0f, -0.0));
    assertEquals(0, SqlComparisons.compareNumbers(-0.0f, 0));
    assertEquals(0, SqlComparisons.compareNumbers(Float.NaN, Double.NaN));
    assertEquals(0, SqlComparisons.compareNumbers(Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
    assertEquals(0, SqlComparisons.compareNumbers(Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
    assertEquals(Boolean.TRUE, SqlComparisons.compare("<", SqlComparisons.compareOrUnknown(1e300, Double.NaN)));
    assertEquals(Boolean.TRUE, SqlComparisons.compare("=", SqlComparisons.compareOrUnknown(Double.NaN, Double.NaN)));
    assertNull(SqlComparisons.compareOrUnknown(Double.NaN, null));
  }

  /**
   * Groups the numbers of {@link #numbersFollowOneTotalOrder()} into equal ranks.
   */
  private static int rank(int index) {
    // -Inf (2), -1e300, -1, -0.5, 0, 1, 1.5, 2, 1e300, +Inf (2), NaN (2)
    return switch (index) {
      case 0, 1 -> 0;
      case 2 -> 1;
      case 3 -> 2;
      case 4 -> 3;
      case 5 -> 4;
      case 6 -> 5;
      case 7 -> 6;
      case 8 -> 7;
      case 9 -> 8;
      case 10, 11 -> 9;
      default -> 10;
    };
  }

  /**
   * Confirms that window functions keep working inside predicates and Boolean
   * projections by sharing the projection's precomputed window state.
   */
  @Test
  void windowFunctionsWorkInsideBooleanProjections() throws Exception {
    Schema schema = SchemaBuilder.record("ids").fields().requiredInt("id").endRecord();
    var row = new GenericData.Record(schema);
    row.put("id", 1);
    var like = (LikeExpression) CCJSqlParserUtil
        .parseCondExpression("CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR) LIKE '1%'");
    var notLike = (LikeExpression) CCJSqlParserUtil
        .parseCondExpression("CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR) NOT LIKE '1%'");

    var rowNumberMap = new IdentityHashMap<GenericRecord, Long>();
    rowNumberMap.put(row, 1L);
    var rowNumberState = new IdentityHashMap<AnalyticExpression, IdentityHashMap<GenericRecord, Long>>();
    rowNumberState.put((AnalyticExpression) ((CastExpression) like.getLeftExpression()).getLeftExpression(),
        rowNumberMap);
    rowNumberState.put((AnalyticExpression) ((CastExpression) notLike.getLeftExpression()).getLeftExpression(),
        rowNumberMap);
    var windowState = WindowState.builder().rowNumberValues(rowNumberState).build();

    var values = new ValueExpressionEvaluator(schema, null, CorrelationMappings.empty(), windowState);
    assertEquals(Boolean.TRUE, values.eval(like, row));
    assertEquals(Boolean.FALSE, values.eval(notLike, row));
    assertEquals(List.of(true, false), queryObjects(
        "SELECT CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR)" + " LIKE '1%' FROM (VALUES (1), (2)) AS t(id)"));
  }

  /**
   * Collects a numeric JDBC projection from an in-memory VALUES query.
   *
   * @param sql
   *          the SQL text to execute
   * @return the first column values as {@code Long} results
   * @throws Exception
   *           if executing the query fails
   */
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

  /**
   * Collects a JDBC projection as raw objects from an in-memory VALUES query.
   *
   * @param sql
   *          the SQL text to execute
   * @return the first column values of the result set
   * @throws Exception
   *           if executing the query fails
   */
  private List<Object> queryObjects(String sql) throws Exception {
    var result = new java.util.ArrayList<Object>();
    try (var connection = java.sql.DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      while (rows.next()) {
        result.add(rows.getObject(1));
      }
    }
    return result;
  }
}
