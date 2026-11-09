package jparq;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeIntegerFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.JSqlParserSupport;
import se.alipsa.jparq.engine.SqlParser;

/** Regression coverage for SELECT aliases used inside ORDER BY expressions. */
class OrderByAliasExpressionTest {

  /**
   * Checks computed and aggregate aliases through physical, derived, and joined
   * inputs.
   *
   * @param prepared
   *          whether to use prepared statements
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void resolvesAliasesInsideExpressions(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    for (String options : List.of("", "?projectionPushdown=false&predicatePushdown=false")) {
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + options)) {
        for (String source : List.of("o t", "(SELECT n FROM o) t", "o t JOIN (VALUES (1)) v(x) ON TRUE")) {
          for (String[] example : List.of(new String[]{
              "n * 2 AS s", "s * 2", "(n * 2) * 2"
          }, new String[]{
              "n * 2 AS s", "-s", "-(n * 2)"
          }, new String[]{
              "UPPER(CAST(n AS VARCHAR)) AS s", "LOWER(s)", "LOWER(UPPER(CAST(n AS VARCHAR)))"
          }, new String[]{
              "-n AS n", "n + 1", "t.n + 1"
          }, new String[]{
              "-n AS n", "t.n + 1", "t.n + 1"
          }, new String[]{
              "-n AS n", "\"n\" + 1", "t.n + 1"
          }, new String[]{
              "-n AS n, n + 1 AS n", "n + 1", "t.n + 1"
          }, new String[]{
              "n + 1 AS a, -n AS b", "a * b", "(n + 1) * (-n)"
          }, new String[]{
              "n + 1 AS renamed", "\"renamed\" * 2", "(n + 1) * 2"
          })) {
            String query = "SELECT " + example[0] + " FROM " + source + " ORDER BY ";
            assertEquals(rows(connection, query + example[2] + " DESC NULLS LAST", prepared),
                rows(connection, query + example[1] + " DESC NULLS LAST", prepared), query + example[1]);
          }
          for (String[] example : List.of(new String[]{
              "SUM(n) AS s", "s * 2", "SUM(n) * 2"
          }, new String[]{
              "COUNT(*) AS c", "COALESCE(c, 0)", "COALESCE(COUNT(*), 0)"
          }, new String[]{
              "SUM(n) + 1 AS s", "-s", "-(SUM(n) + 1)"
          })) {
            String query = "SELECT n, " + example[0] + " FROM " + source + " GROUP BY n ORDER BY ";
            assertEquals(rows(connection, query + example[2] + " DESC NULLS LAST, n", prepared),
                rows(connection, query + example[1] + " DESC NULLS LAST, n", prepared), query + example[1]);
          }
        }
        String wildcard = "SELECT t.*, -n AS s FROM o t ORDER BY ";
        assertEquals(rows(connection, wildcard + "(-n) + 1 NULLS LAST", prepared),
            rows(connection, wildcard + "s + 1 NULLS LAST", prepared));
        String scope = "SELECT -n AS s FROM o ORDER BY ";
        assertEquals(rows(connection, scope + "(-n) + (SELECT s FROM (VALUES (10)) v(s)) NULLS LAST", prepared),
            rows(connection, scope + "s + (SELECT s FROM (VALUES (10)) v(s)) NULLS LAST", prepared));
      }
    }
  }

  /**
   * Preserves input precedence for swapped aliases and ambiguous join inputs.
   *
   * @param prepared
   *          whether to use prepared statements
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void preservesInputBindings(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      String swapped = "SELECT n AS x, x AS n FROM (SELECT n, 10 - n AS x FROM o) t ORDER BY ";
      assertEquals(rows(connection, swapped + "t.n * 10 + t.x NULLS LAST", prepared),
          rows(connection, swapped + "n * 10 + x NULLS LAST", prepared));
      for (String filter : List.of("", " WHERE 1 = 0")) {
        String ambiguous = "SELECT -a.n AS n FROM o a JOIN o b ON TRUE" + filter + " ORDER BY n + 1";
        SQLException error = assertThrows(SQLException.class, () -> rows(connection, ambiguous, prepared));
        assertEquals("42702", error.getSQLState(), ambiguous);
      }
      for (String source : List.of("o t", "o t JOIN (VALUES (1)) v(x) ON TRUE")) {
        try (var statement = connection.prepareStatement("SELECT -n AS n FROM " + source + " ORDER BY n + 1")) {
          var expected = rows(connection, "SELECT -n FROM " + source + " ORDER BY t.n + 1", false);
          for (int execution = 0; execution < 3; execution++) {
            try (var result = statement.executeQuery()) {
              for (var row : expected) {
                assertTrue(result.next());
                assertEquals(row.getFirst(), result.getObject(1));
              }
              assertFalse(result.next());
            }
          }
        }
      }
    }
  }

  /**
   * Rejects alias expansion that cannot preserve a computed projection value.
   *
   * @param prepared
   *          whether to use prepared statements
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void rejectsUnsafeAliasExpansion(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String selected : List.of("ROW_NUMBER() OVER (ORDER BY n)", "SUM(n) OVER (PARTITION BY n)",
          "ROW_NUMBER() OVER (ORDER BY n) + 1", "RAND()", "ABS(RANDOM())", "(SELECT MAX(n) FROM o)")) {
        for (String source : List.of("o", "(SELECT n FROM o) t", "o JOIN (VALUES (1)) v(x) ON TRUE")) {
          for (String filter : List.of("", " WHERE 1 = 0")) {
            String sql = "SELECT " + selected + " AS s FROM " + source + filter + " ORDER BY s * -1";
            SQLException error = assertThrows(SQLException.class, () -> rows(connection, sql, prepared));
            assertEquals("42000", error.getSQLState(), sql);
            assertTrue(error.getMessage().contains("alias 's' requires its computed projection value"),
                error.getMessage());
            if (prepared && source.equals("o")) {
              SQLException preparation = assertThrows(SQLException.class, () -> connection.prepareStatement(sql));
              assertEquals("42000", preparation.getSQLState(), sql);
            }
          }
        }
      }
      // Materializing the projection in a derived table provides an input value.
      assertEquals(
          rows(connection, "SELECT rn FROM (SELECT ROW_NUMBER() OVER (ORDER BY n) AS rn FROM o) t ORDER BY rn DESC",
              prepared),
          rows(connection, "SELECT rn FROM (SELECT ROW_NUMBER() OVER (ORDER BY n) AS rn FROM o) t ORDER BY rn * -1",
              prepared));
      // An input column wins even when an unsafe alias has the same name.
      assertEquals(
          rows(connection, "SELECT n FROM o ORDER BY n NULLS LAST", prepared).stream().map(List::getFirst).toList(),
          rows(connection, "SELECT n AS original, RAND() AS n FROM o ORDER BY n + 1 NULLS LAST", prepared).stream()
              .map(List::getFirst).toList());
    }
  }

  /**
   * Checks that schema-aware expansion can be repeated without freezing a key.
   */
  @Test
  void expressionBindingIsIdempotent() {
    var schema = SchemaBuilder.record("Row").fields().optionalInt("n").endRecord();
    var query = SqlParser.parseSelect("SELECT -n AS n, n + 1 AS s FROM o ORDER BY s * 2");
    var bound = SqlParser.resolveInputOrdering(query, schema, null);
    var rebound = SqlParser.resolveInputOrdering(bound, schema, null);
    assertEquals("(n + 1) * 2", bound.orderBy().getFirst().expression().toString());
    assertEquals(bound.orderBy(), rebound.orderBy());
  }

  /**
   * Preserves both the source and generated SQL in an internal expansion failure.
   *
   * @throws Exception
   *           if mocking expression parsing fails
   */
  @Test
  void expansionFailureIncludesGeneratedSql() throws Exception {
    var schema = SchemaBuilder.record("Row").fields().optionalInt("n").endRecord();
    var query = SqlParser.parseSelect("SELECT n + 1 AS s FROM o ORDER BY s * 2");
    try (var parser = mockStatic(JSqlParserSupport.class)) {
      parser.when(() -> JSqlParserSupport.parseExpression(anyString()))
          .thenThrow(new JSQLParserException("forced parser failure"));
      IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> SqlParser.resolveInputOrdering(query, schema, null));
      assertTrue(failure.getMessage().contains("Internal error resolving ORDER BY aliases in s * 2"));
      assertTrue(failure.getMessage().contains("generated SQL: (n + 1) * 2"));
      assertEquals("forced parser failure", failure.getCause().getMessage());
    }
  }

  /**
   * Checks ambiguity and quoted-name diagnostics even when the input is empty.
   *
   * @param prepared
   *          whether to use prepared statements
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void validatesExpressionAliasNames(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String source : List.of("o", "o JOIN (VALUES (1)) v(x) ON TRUE")) {
        for (String filter : List.of("", " WHERE 1 = 0")) {
          String query = "SELECT n + 1 AS s, -n AS s FROM " + source + filter + " ORDER BY s * 2";
          SQLException ambiguous = assertThrows(SQLException.class, () -> rows(connection, query, prepared));
          assertEquals("42702", ambiguous.getSQLState(), query);
          String repeated = "SELECT n + 1 AS s, n + 1 AS s FROM " + source + filter + " ORDER BY ";
          assertEquals(rows(connection, repeated + "(n + 1) * 2", prepared),
              rows(connection, repeated + "s * 2", prepared));
          String quoted = "SELECT -n AS renamed FROM " + source + filter + " ORDER BY \"Renamed\" * 2";
          SQLException missing = assertThrows(SQLException.class, () -> rows(connection, quoted, prepared));
          assertEquals("42000", missing.getSQLState(), quoted);
        }
      }
    }
  }
}
