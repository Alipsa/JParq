package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqPreparedStatement;

/** Verifies scalar, datetime, and window numeric declarations for PR #560. */
class NumericDeclarationRegressionTest {
  private static final String INPUT = " FROM (VALUES (1), (2)) t(id)";

  /**
   * NULL subqueries cannot hide another branch's non-NULL numeric result.
   *
   * @param expression
   *          scalar projection containing a NULL subquery
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CASE WHEN 1 = 0 THEN (SELECT NULL) ELSE EXTRACT(YEAR FROM DATE '2020-01-01') END",
      "COALESCE((SELECT NULL), EXTRACT(YEAR FROM DATE '2020-01-01'))"
  })
  void nestedNullBranches(String expression) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String sql = "SELECT CASE WHEN id = 1 THEN (SELECT " + expression + ") ELSE 1.00 END AS k" + INPUT;
      try (var result = statement.executeQuery(sql + " ORDER BY id")) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertEquals(new BigDecimal("2020.00"), result.getObject(1));
        assertTrue(result.next());
        assertEquals(new BigDecimal("1.00"), result.getObject(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Ranking and navigation operands use a common representation before DISTINCT.
   *
   * @param function
   *          window invocation with value one on the first row
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "ROW_NUMBER()", "RANK()", "DENSE_RANK()", "NTILE(2)", "LAG(id, 1, 1)", "LEAD(1)", "FIRST_VALUE(id)",
      "LAST_VALUE(id)", "NTH_VALUE(id, 1)"
  })
  void windowDistinctKeys(String function) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String sql = "SELECT DISTINCT CASE WHEN id = 1 THEN " + function + " OVER (ORDER BY id) ELSE 1.0 END AS k"
          + INPUT;
      try (var result = statement.executeQuery(sql)) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1), sql);
        assertTrue(result.next(), sql);
        assertEquals(new BigDecimal("1.0"), result.getObject(1), sql);
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Distribution windows retain DOUBLE when combined with decimal branches.
   *
   * @param function
   *          distribution function name
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "PERCENT_RANK", "CUME_DIST"
  })
  void distributionWindowDistinctKeys(String function) throws Exception {
    double expected = "PERCENT_RANK".equals(function) ? 0.0 : 0.5;
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String sql = "SELECT DISTINCT CASE WHEN id = 1 THEN " + function + "() OVER (ORDER BY id) ELSE " + expected
          + " END AS k" + INPUT;
      try (var result = statement.executeQuery(sql)) {
        assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1), sql);
        assertTrue(result.next(), sql);
        assertEquals(expected, result.getObject(1), sql);
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * EXTRACT exact numeric fields share DISTINCT keys with decimal branches.
   *
   * @param field
   *          datetime field to extract
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "YEAR", "SECOND"
  })
  void extractDistinctKeys(String field) throws Exception {
    String expected = "YEAR".equals(field) ? "2020.0" : "1.0";
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String expression = "EXTRACT(" + field + " FROM TIMESTAMP '2020-01-01 00:00:01')";
      for (String branch : List.of(expression, "(SELECT " + expression + ")")) {
        String sql = "SELECT DISTINCT CASE WHEN id = 1 THEN " + branch + " ELSE " + expected + " END AS k" + INPUT;
        try (var result = statement.executeQuery(sql)) {
          assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1), sql);
          assertTrue(result.next(), sql);
          assertEquals(0, new BigDecimal(expected).compareTo(assertInstanceOf(BigDecimal.class, result.getObject(1))),
              sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Joined derived, VALUES, and LATERAL inputs retain scalar declarations.
   *
   * @param subquery
   *          scalar query returning a declared numeric input
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT d.v FROM (SELECT 1 AS v) d JOIN mtcars m ON 1 = 1 LIMIT 1",
      "SELECT d.v FROM mtcars m JOIN (SELECT 1 AS v) d ON 1 = 1 LIMIT 1",
      "SELECT d.v FROM (VALUES (1)) d(v) JOIN (VALUES (2)) e(w) ON 1 = 1",
      "SELECT e.w FROM (VALUES (2)) d(v) JOIN (VALUES (1)) e(w) ON 1 = 1",
      "SELECT d.v FROM mtcars m, LATERAL (SELECT 1 AS v) d LIMIT 1",
      "SELECT d.v FROM (VALUES (1)) q(x), LATERAL (SELECT q.x AS v FROM (VALUES (0)) z(dummy)) d",
      "SELECT d.v FROM mtcars m JOIN (SELECT 1 AS x) d(v) ON 1 = 1 LIMIT 1",
      "SELECT v FROM mtcars m JOIN (SELECT 1 AS v) d ON 1 = 1 LIMIT 1",
      "SELECT d.v FROM mtcars m JOIN (SELECT x AS v FROM (VALUES (1)) q(x)) d ON 1 = 1 LIMIT 1",
      "SELECT (SELECT d.v) FROM mtcars m JOIN (SELECT 1 AS v) d ON 1 = 1 LIMIT 1",
      "SELECT d.v FROM (VALUES (1)) q(x) JOIN (SELECT 1 AS v UNION ALL SELECT 2.5) d ON 1 = 1 LIMIT 1",
      "SELECT d.v FROM mtcars m JOIN (SELECT (SELECT q.x) AS v FROM (VALUES (1)) q(x)) d ON 1 = 1 LIMIT 1"
  })
  void joinedScalarDistinctKeys(String subquery) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String sql = "SELECT DISTINCT CASE WHEN cyl = 4 THEN (" + subquery
          + ") ELSE 1.0 END AS k FROM mtcars WHERE cyl IN (4,6)";
      try (var prepared = (JParqPreparedStatement) connection.prepareStatement(subquery)) {
        assertNotNull(prepared.firstProjectionNumericType(), subquery);
      }
      try (var result = statement.executeQuery(sql)) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1), sql);
        assertTrue(result.next(), sql);
        assertEquals(new BigDecimal("1.0"), result.getObject(1), sql);
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Top-level scalar arithmetic and COALESCE retain metadata without CASE (#550).
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void topLevelScalarNumericTypes() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (boolean empty : List.of(false, true)) {
        String scalar = "(SELECT cyl FROM mtcars" + (empty ? " WHERE 1 = 0" : "") + " LIMIT 1)";
        try (var result = statement.executeQuery("SELECT " + scalar + " + 1, COALESCE(" + scalar + ", 0)")) {
          assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
          assertEquals(Types.INTEGER, result.getMetaData().getColumnType(2));
          assertTrue(result.next());
          if (empty) {
            assertNull(result.getObject(1));
            assertEquals(0, result.getObject(2));
          } else {
            assertEquals(7L, result.getObject(1));
            assertEquals(6, result.getObject(2));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * A WITH inside a joined derived table resolves its own CTEs before its table
   * references, so a CTE contributes its declared type and shadows any physical
   * table with the same name.
   *
   * @param body
   *          the derived table body containing a WITH clause
   * @throws Exception
   *           if preparation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      // CTE name clashes with the physical mtcars table.
      "SELECT d.cyl FROM (VALUES (1)) q(x) JOIN (WITH mtcars AS (SELECT 1.5 AS cyl) SELECT cyl FROM mtcars) d ON 1 = 1",
      // CTE name does not clash with any table.
      "SELECT d.cyl FROM mtcars m JOIN (SELECT * FROM (WITH c AS (SELECT 1.5 AS cyl) SELECT cyl FROM c) z) d ON 1 = 1 LIMIT 1",
      // CTE column aliases name the exported columns.
      "SELECT d.cyl FROM (VALUES (1)) q(x) JOIN (WITH t(x, cyl) AS (SELECT 2, 1.5) SELECT cyl FROM t) d ON 1 = 1"
  })
  void derivedWithResolvesOwnCtes(String body) throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var prepared = (JParqPreparedStatement) connection.prepareStatement(body)) {
      assertNotNull(prepared.firstProjectionSchema(), body);
      assertNotNull(prepared.firstProjectionNumericType(), body);
      assertEquals(Types.DECIMAL, prepared.firstProjectionNumericType().jdbcType(), body);
      try (var result = statement.executeQuery(
          "SELECT DISTINCT CASE WHEN id = 1 THEN (" + body + ") ELSE 1 END AS k" + INPUT + " ORDER BY k")) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1), body);
        assertTrue(result.next(), body);
        assertEquals(0, BigDecimal.ONE.compareTo(result.getBigDecimal(1)), body);
        assertTrue(result.next(), body);
        assertEquals(0, new BigDecimal("1.5").compareTo(result.getBigDecimal(1)), body);
        assertFalse(result.next(), body);
      }
    }
  }

  /**
   * A CTE body only sees siblings declared before it, so a later CTE that shadows
   * a physical table keeps the earlier CTE bound to the table's own declaration.
   *
   * @throws Exception
   *           if preparation or query execution fails
   */
  @Test
  void derivedCteBodyIgnoresLaterSibling() throws Exception {
    String scalar = "SELECT d.cyl FROM (VALUES (1)) q(x) JOIN (WITH a AS (SELECT MAX(hp) AS cyl FROM mtcars), "
        + "mtcars AS (SELECT 1.5 AS hp) SELECT cyl FROM a) d ON 1 = 1";
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var prepared = (JParqPreparedStatement) connection.prepareStatement(scalar)) {
      assertNotNull(prepared.firstProjectionSchema());
      assertNotNull(prepared.firstProjectionNumericType());
      assertEquals(Types.INTEGER, prepared.firstProjectionNumericType().jdbcType());
      try (var result = statement.executeQuery(
          "SELECT DISTINCT CASE WHEN id = 1 THEN (" + scalar + ") ELSE 1 END AS k" + INPUT + " ORDER BY k")) {
        assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertEquals(Integer.valueOf(1), result.getObject(1));
        assertTrue(result.next());
        assertEquals(Integer.valueOf(335), result.getObject(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * A CTE body can use a preceding sibling CTE, directly or through a derived
   * table of its own.
   *
   * @param body
   *          the derived table body chaining sibling CTEs
   * @throws Exception
   *           if preparation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT d.cyl FROM (VALUES (1)) q(x) JOIN (WITH a AS (SELECT 1.5 AS v), "
          + "b AS (SELECT a.v + 1 AS v FROM a) SELECT v AS cyl FROM b) d ON 1 = 1",
      "SELECT d.cyl FROM (VALUES (1)) q(x) JOIN (WITH a AS (SELECT 1.5 AS v), "
          + "b AS (SELECT z.v + 1 AS v FROM (SELECT a.v AS v FROM a) z) SELECT v AS cyl FROM b) d ON 1 = 1"
  })
  void derivedWithSiblingCteChain(String body) throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var prepared = (JParqPreparedStatement) connection.prepareStatement(body)) {
      assertNotNull(prepared.firstProjectionSchema(), body);
      assertNotNull(prepared.firstProjectionNumericType(), body);
      assertEquals(Types.DECIMAL, prepared.firstProjectionNumericType().jdbcType(), body);
      try (var result = statement.executeQuery(
          "SELECT DISTINCT CASE WHEN id = 1 THEN (" + body + ") ELSE 1 END AS k" + INPUT + " ORDER BY k")) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1), body);
        assertTrue(result.next(), body);
        assertEquals(0, BigDecimal.ONE.compareTo(result.getBigDecimal(1)), body);
        assertTrue(result.next(), body);
        assertEquals(0, new BigDecimal("2.5").compareTo(result.getBigDecimal(1)), body);
        assertFalse(result.next(), body);
      }
    }
  }

  /**
   * A recursive CTE in a derived body leaves its declaration unknown rather than
   * guessing from the anchor branch.
   *
   * @throws Exception
   *           if preparation fails
   */
  @Test
  void derivedWithRecursiveCteStaysUnknown() throws Exception {
    String body = "SELECT d.cyl FROM (VALUES (1)) q(x) JOIN "
        + "(WITH RECURSIVE r(cyl) AS (SELECT 1.5 AS cyl UNION ALL SELECT cyl + 1 FROM r WHERE cyl < 3) "
        + "SELECT MIN(cyl) AS cyl FROM r) d ON 1 = 1";
    try (var connection = DriverManager.getConnection(url());
        var prepared = (JParqPreparedStatement) connection.prepareStatement(body)) {
      assertNull(prepared.firstProjectionSchema(), body);
      assertNull(prepared.firstProjectionNumericType(), body);
    }
  }

  /**
   * Top-level and scalar EXTRACT values agree with their metadata classes.
   *
   * @param expression
   *          extraction expression, optionally in a scalar subquery
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "EXTRACT(MONTH FROM DATE '2020-01-01')", "(SELECT EXTRACT(YEAR FROM DATE '2020-01-01'))",
      "EXTRACT(DAY FROM INTERVAL '2' DAY)", "EXTRACT(SECOND FROM TIME '00:00:01')"
  })
  void extractProjectionClasses(String expression) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String projection : List.of("SELECT " + expression + " AS k",
          "SELECT k FROM (SELECT " + expression + " AS k) d")) {
        try (var result = statement.executeQuery(projection)) {
          assertTrue(result.next(), projection);
          Object value = result.getObject(1);
          assertEquals(value.getClass().getName(), result.getMetaData().getColumnClassName(1), projection);
          assertEquals(expression.contains("SECOND") ? BigDecimal.class : Long.class, value.getClass(), projection);
          assertFalse(result.next(), projection);
        }
      }
    }
  }

  /**
   * Merged USING columns and correlated row constructors retain numeric keys.
   *
   * @param scalar
   *          scalar subquery returning one
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT MAX(cyl) / 8 FROM mtcars a JOIN mtcars b USING (cyl)",
      "SELECT e.w FROM (VALUES (0)) q(x) JOIN (VALUES (t.id)) e(w) ON 1 = 1",
      "SELECT e.w FROM (VALUES (1)) q(x), LATERAL (VALUES (q.x)) e(w)"
  })
  void correlatedAndMergedInputKeys(String scalar) throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT DISTINCT CASE WHEN id = 1 THEN (" + scalar + ") ELSE 1.0 END AS k" + INPUT)) {
      assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1), scalar);
      assertTrue(result.next(), scalar);
      assertEquals(0, BigDecimal.ONE.compareTo(assertInstanceOf(BigDecimal.class, result.getObject(1))), scalar);
      assertFalse(result.next(), scalar);
    }
  }

  /**
   * NULL probes retain the numeric declarations of correlated VALUES operands.
   *
   * @throws Exception
   *           if querying fails
   */
  @Test
  void lateralValuesRetainNumericRepresentation() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT e.w FROM (VALUES (1)) q(x), LATERAL (VALUES (q.x)) e(w)")) {
      assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
      assertTrue(result.next());
      assertEquals(1, result.getObject(1));
      assertEquals(Integer.class.getName(), result.getMetaData().getColumnClassName(1));
      assertFalse(result.next());
    }
  }

  /**
   * Optional VALUES declarations cannot reject rows whose values are all NULL.
   *
   * @throws Exception
   *           if querying fails
   */
  @Test
  void unresolvedNullValueDeclarationsRemainOptional() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT v.x FROM (VALUES (CAST(NULL AS DATE)), (CAST(NULL AS TIMESTAMP))) v(x)")) {
      for (int row = 0; row < 2; row++) {
        assertTrue(result.next());
        assertNull(result.getObject(1));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Window arithmetic exposes the computed numeric family outside CASE (#557).
   *
   * @param function
   *          ranking or distribution window
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "RANK", "PERCENT_RANK"
  })
  void windowArithmeticDeclarations(String function) throws Exception {
    boolean distribution = "PERCENT_RANK".equals(function);
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + function + "() OVER (ORDER BY id) + 1 AS k" + INPUT)) {
      assertEquals(distribution ? Types.DOUBLE : Types.BIGINT, result.getMetaData().getColumnType(1));
      assertTrue(result.next());
      Object value = result.getObject(1);
      assertEquals(distribution ? Double.class : Long.class, value.getClass());
      assertEquals(value.getClass().getName(), result.getMetaData().getColumnClassName(1));
      assertEquals(distribution ? 1.0 : 2.0, result.getDouble(1));
    }
  }

  /**
   * Returns the database containing the standard mtcars fixture.
   *
   * @return fixture JDBC URL
   */
  private static String url() {
    return "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
  }
}
