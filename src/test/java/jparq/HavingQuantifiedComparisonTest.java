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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Regression coverage for quantified comparisons in HAVING (issue 214). */
class HavingQuantifiedComparisonTest {

  /**
   * Verifies grouped operands, both operand orders, and correlated subqueries.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void evaluatesGroupedComparisons(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      assertEquals(List.of(4), groups(connection, "COUNT(*) = ANY(SELECT 11)"));
      assertEquals(List.of(4, 6), groups(connection, "COUNT(*) = ANY(ARRAY[11,7])"));
      assertEquals(List.of(6), groups(connection, "COUNT(*) < ALL(ARRAY[8,14])"));
      assertEquals(List.of(8), groups(connection, "COUNT(*) > ALL(SELECT 7 UNION ALL SELECT 11)"));
      assertEquals(List.of(4, 6), groups(connection, "COUNT(*) <= ALL(ARRAY[11,14])"));
      assertEquals(List.of(4, 8), groups(connection, "COUNT(*) >= ALL(ARRAY[7,11])"));
      assertEquals(List.of(4, 8), groups(connection, "COUNT(*) <> ALL(ARRAY[7,32])"));
      assertEquals(List.of(4, 8), groups(connection, "COUNT(*) != ALL(ARRAY[7,32])"));
      for (String quantifier : List.of("ANY", "SOME", "ALL")) {
        for (String operator : List.of("=", "<", ">")) {
          for (String values : List.of("ARRAY[7,11]", "SELECT 7 UNION ALL SELECT 11",
              "ARRAY(SELECT 7 UNION ALL SELECT 11)")) {
            String quantified = quantifier + "(" + values + ")";
            String connective = "ALL".equals(quantifier) ? " AND " : " OR ";
            String rightPredicate = "COUNT(*) " + operator + " " + quantified;
            String leftPredicate = quantified + " " + operator + " COUNT(*)";
            assertEquals(
                groups(connection, "COUNT(*) " + operator + " 7" + connective + "COUNT(*) " + operator + " 11"),
                groups(connection, rightPredicate), rightPredicate);
            assertEquals(
                groups(connection, "7 " + operator + " COUNT(*)" + connective + "11 " + operator + " COUNT(*)"),
                groups(connection, leftPredicate), leftPredicate);
          }
        }
        assertEquals(List.of(4, 6, 8), groups(connection, "COUNT(*) = " + quantifier + "(ARRAY[COUNT(*), COUNT(*)])"));
        assertEquals(List.of(4, 6, 8),
            groups(connection, "COUNT(*) = " + quantifier + "(SELECT COUNT(*) FROM mtcars i WHERE i.cyl = m.cyl)"));
        assertEquals(List.of(4, 6, 8), groups(connection, "cyl = " + quantifier + "(ARRAY(SELECT m.cyl))"));
        assertEquals(List.of(4, 6, 8), groups(connection, "cyl = " + quantifier + "(ARRAY[cyl])"));
        assertEquals(List.of(4, 6, 8), groups(connection, "n = " + quantifier + "(ARRAY[n])"));
      }
      assertEquals(List.of(4, 6),
          integers(connection, "SELECT cyl FROM mtcars GROUP BY cyl HAVING COUNT(*) = ANY(ARRAY[11,7]) ORDER BY cyl"));
      assertEquals(List.of(0),
          integers(connection, "SELECT COUNT(*) FROM mtcars WHERE cyl = 0 HAVING COUNT(*) = ANY(SELECT 0)"));
      try (var statement = connection.prepareStatement(
          "SELECT cyl, COUNT(*) FROM mtcars GROUP BY cyl HAVING COUNT(*) = ANY(ARRAY[?,?]) ORDER BY cyl")) {
        for (int count : List.of(11, 7, 14)) {
          statement.setInt(1, count);
          statement.setInt(2, count);
          try (var result = statement.executeQuery()) {
            assertTrue(result.next());
            assertEquals(count == 11 ? 4 : count == 7 ? 6 : 8, result.getInt(1));
            assertEquals(count, result.getInt(2));
            assertFalse(result.next());
          }
        }
      }
      for (String wrapper : List.of("SELECT cyl FROM (%s) t ORDER BY cyl",
          "WITH t AS (%s) SELECT cyl FROM t ORDER BY cyl")) {
        assertEquals(List.of(4, 6), integers(connection,
            wrapper.formatted("SELECT cyl, COUNT(*) FROM mtcars GROUP BY cyl HAVING COUNT(*) = SOME(ARRAY[11,7])")));
      }
    }
  }

  /**
   * Checks TRUE, FALSE, and UNKNOWN explicitly, including empty and NULL inputs.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void preservesThreeValuedLogic(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String quantifier : List.of("ANY", "SOME", "ALL")) {
        boolean all = "ALL".equals(quantifier);
        for (String[] test : List.of(new String[]{
            "ARRAY[]", all ? "TRUE" : "FALSE"
        }, new String[]{
            "SELECT 1 WHERE 1 = 0", all ? "TRUE" : "FALSE"
        }, new String[]{
            "ARRAY[NULL]", "UNKNOWN"
        }, new String[]{
            "SELECT NULL", "UNKNOWN"
        }, new String[]{
            "NULL", "UNKNOWN"
        }, new String[]{
            "ARRAY[NULL,32]", all ? "UNKNOWN" : "TRUE"
        }, new String[]{
            "SELECT NULL UNION ALL SELECT 32", all ? "UNKNOWN" : "TRUE"
        }, new String[]{
            "ARRAY[NULL,1]", all ? "FALSE" : "UNKNOWN"
        }, new String[]{
            "SELECT NULL UNION ALL SELECT 1", all ? "FALSE" : "UNKNOWN"
        })) {
          String predicate = "COUNT(*) = " + quantifier + "(" + test[0] + ")";
          assertEquals(List.of(32),
              integers(connection, "SELECT COUNT(*) FROM mtcars HAVING (" + predicate + ") IS " + test[1]), predicate);
          String negated = "TRUE".equals(test[1]) ? "FALSE" : "FALSE".equals(test[1]) ? "TRUE" : "UNKNOWN";
          assertEquals(List.of(32),
              integers(connection, "SELECT COUNT(*) FROM mtcars HAVING (NOT (" + predicate + ")) IS " + negated),
              predicate);
        }
        assertEquals(List.of(32), integers(connection,
            "SELECT COUNT(*) FROM mtcars HAVING (NULL = " + quantifier + "(ARRAY[32])) IS UNKNOWN"));
        assertEquals(List.of(32), integers(connection, "SELECT COUNT(*) FROM mtcars HAVING " + "(NULL = " + quantifier
            + "(ARRAY[])) IS " + (all ? "TRUE" : "FALSE")));
      }
      assertEquals(List.of(4, 6, 8), groups(connection, "TRUE OR COUNT(*) = ANY(1)"));
      assertEquals(List.of(), groups(connection, "FALSE AND COUNT(*) = ANY(1)"));
    }
  }

  /**
   * Invalid operands must produce an error instead of silently discarding groups.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void rejectsInvalidOperands(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String[] test : List.of(new String[]{
          "ANY(ARRAY[1]) = ALL(ARRAY[1])", "requires a scalar operand"
      }, new String[]{
          "COUNT(*) = ANY()", "requires exactly one array operand"
      }, new String[]{
          "COUNT(*) = SOME(ARRAY[1],ARRAY[2])", "requires exactly one array operand"
      }, new String[]{
          "COUNT(*) = ALL(1)", "requires an array operand"
      }, new String[]{
          "COUNT(*) = ANY(ARRAY[[32]])", "requires scalar array elements"
      }, new String[]{
          "COUNT(*) = ANY(ARRAY(SELECT 1, 2))", "ARRAY subquery must return exactly one column"
      })) {
        SQLException exception = assertThrows(SQLException.class, () -> groups(connection, test[0]), test[0]);
        Throwable cause = exception;
        while (cause.getCause() != null) {
          cause = cause.getCause();
        }
        assertTrue(cause.getMessage().contains(test[1]), cause.getMessage());
      }
    }
  }

  /**
   * Structured subquery values must fail in WHERE and HAVING before filtering.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void rejectsStructuredSubqueryValues(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String quantifier : List.of("ANY", "SOME", "ALL")) {
        for (String sql : List.of("SELECT cyl FROM mtcars WHERE cyl = " + quantifier + "(SELECT ARRAY[11])",
            "SELECT COUNT(*) FROM mtcars HAVING COUNT(*) = " + quantifier + "(SELECT ARRAY[11])")) {
          SQLException exception = assertThrows(SQLException.class, () -> integers(connection, sql), sql);
          Throwable cause = exception;
          while (cause.getCause() != null) {
            cause = cause.getCause();
          }
          assertEquals("ANY/SOME/ALL requires scalar array elements", cause.getMessage(), sql);
        }
      }
    }
  }

  /**
   * Opens the fixture database with the requested pushdown settings.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @return an open connection
   * @throws Exception
   *           if connection setup fails
   */
  private Connection connect(boolean pushdown) throws Exception {
    Path root = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    return DriverManager
        .getConnection("jdbc:jparq:" + root + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
  }

  /**
   * Returns cylinder groups surviving a HAVING predicate.
   *
   * @param connection
   *          the fixture connection
   * @param predicate
   *          the HAVING condition
   * @return surviving cylinder values
   * @throws SQLException
   *           if querying fails
   */
  private List<Integer> groups(Connection connection, String predicate) throws SQLException {
    return integers(connection,
        "SELECT cyl, COUNT(*) AS n FROM mtcars m GROUP BY cyl HAVING " + predicate + " ORDER BY cyl");
  }

  /**
   * Reads the first result column as integers.
   *
   * @param connection
   *          the fixture connection
   * @param sql
   *          the query
   * @return query values in result order
   * @throws SQLException
   *           if querying fails
   */
  private List<Integer> integers(Connection connection, String sql) throws SQLException {
    List<Integer> values = new ArrayList<>();
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        values.add(result.getInt(1));
      }
    }
    return values;
  }
}
