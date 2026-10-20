package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.SqlParser;

/** Verifies ordinal column alias binding at derived-table boundaries. */
class DerivedColumnAliasTest {

  /**
   * Checks computed and wildcard inputs, join placement, metadata, and clauses
   * that consume the renamed columns.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void aliasesRenameMaterializedOutputs(boolean pushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown=" + pushdown
        + "&projectionPushdown=" + pushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String body : List.of("SELECT id, id + 1 AS next_id FROM optimized WHERE id <= 2",
          "SELECT id AS b, id + 1 AS a FROM optimized WHERE id <= 2", "SELECT id, id FROM optimized WHERE id <= 2",
          "SELECT * FROM (VALUES (1, 2), (2, 3)) v(id, next_id)",
          "SELECT v.* FROM (VALUES (1, 2), (2, 3)) v(id, next_id)",
          "SELECT id, id AS original, id + 1 AS next_id FROM optimized WHERE id <= 2")) {
        boolean threeColumns = body.contains("AS original");
        String derived = "(" + body + ") x(" + (threeColumns ? "a, extra, b" : "a, b") + ")";
        for (String source : List.of(derived, derived + " JOIN (VALUES (1)) s(z) ON x.a >= s.z",
            "(VALUES (1)) s(z) JOIN " + derived + " ON x.a >= s.z")) {
          for (String projection : List.of("x.a, x.b", "a, b")) {
            String sql = "SELECT " + projection + " FROM " + source + " WHERE x.b >= 1 ORDER BY x.a";
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              assertEquals("a", result.getMetaData().getColumnLabel(1), sql);
              assertEquals("b", result.getMetaData().getColumnLabel(2), sql);
              for (int id = 1; id <= 2; id++) {
                assertTrue(result.next(), sql);
                assertEquals(id, ((Number) result.getObject(1)).intValue(), sql);
                assertEquals(body.startsWith("SELECT id, id FROM") ? id : id + 1,
                    ((Number) result.getObject(2)).intValue(), sql);
              }
              assertFalse(result.next(), sql);
            }
          }
        }
      }
      for (String projection : List.of("*", "x.*", "x.b, x.a")) {
        String sql = "SELECT " + projection
            + " FROM (SELECT id, id + 1 AS next_id FROM optimized WHERE id <= 2) x(a, b) ORDER BY a";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(2, result.getMetaData().getColumnCount());
          boolean reversed = projection.equals("x.b, x.a");
          assertEquals(reversed ? "b" : "a", result.getMetaData().getColumnLabel(1));
          assertTrue(result.next());
          assertEquals(reversed ? 2 : 1, ((Number) result.getObject(1)).intValue());
          assertEquals(reversed ? 1 : 2, ((Number) result.getObject(2)).intValue());
        }
      }
    }
  }

  /**
   * Checks per-row LATERAL binding and schema preservation for empty results.
   *
   * @param empty
   *          whether the derived input returns no rows
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void lateralAndEmptyInputsRetainAliases(boolean empty) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources/hf"))) {
      for (String body : List.of("SELECT id, id + 1 AS next_id FROM optimized",
          "SELECT v.* FROM (VALUES (1, 2), (2, 3)) v(id, next_id)")) {
        String sql = "SELECT x.a, x.b FROM (VALUES (1), (2)) s(z) LEFT JOIN LATERAL (" + body + " WHERE id = s.z"
            + (empty ? " AND id < 0" : "") + ") x(a, b) ON TRUE ORDER BY s.z";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals("a", result.getMetaData().getColumnLabel(1));
          for (int id = 1; id <= 2; id++) {
            assertTrue(result.next(), sql);
            assertEquals(empty ? null : Integer.toString(id), result.getString(1), sql);
            assertEquals(empty ? null : Integer.toString(id + 1), result.getString(2), sql);
          }
          assertFalse(result.next());
        }
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT x.* FROM (SELECT id FROM optimized WHERE id < 0) x(a)")) {
        assertEquals("a", result.getMetaData().getColumnLabel(1));
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Checks quoted names and aliases attached outside extra parentheses.
   *
   * @param source
   *          the parenthesized derived input
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "(SELECT id, id + 1 AS n FROM optimized WHERE id <= 2)",
      "((SELECT id, id + 1 AS n FROM optimized WHERE id <= 2) inner_alias)"
  })
  void quotedAliasesBindByPosition(String source) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources/hf"));
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT x.\"First\" AS first_output, x.\"Second\" AS second_output FROM "
            + source + " x(\"First\", \"Second\") ORDER BY x.\"First\"")) {
      assertEquals("first_output", result.getMetaData().getColumnLabel(1));
      assertEquals("second_output", result.getMetaData().getColumnLabel(2));
      for (int id = 1; id <= 2; id++) {
        assertTrue(result.next());
        assertEquals(id, ((Number) result.getObject(1)).intValue());
        assertEquals(id + 1, ((Number) result.getObject(2)).intValue());
      }
      assertFalse(result.next());
    }
  }

  /**
   * Rejects too few and too many aliases after wildcard expansion, even without
   * rows.
   *
   * @param aliases
   *          the invalid column alias list
   * @throws Exception
   *           if the connection cannot be opened
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "a", "a, b, c"
  })
  void aliasCountsMustMatchOutputWidth(String aliases) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources/hf"))) {
      for (String body : List.of("SELECT id, id + 1 FROM optimized WHERE id < 0",
          "SELECT * FROM (VALUES (1, 2)) v(i, j)")) {
        String derived = "(" + body + ") x(" + aliases + ")";
        for (String source : List.of(derived, "(VALUES (1)) s(z) JOIN " + derived + " ON TRUE",
            "(VALUES (1)) s(z) JOIN LATERAL " + derived + " ON TRUE")) {
          try (var statement = connection.createStatement()) {
            SQLException error = assertThrows(SQLException.class,
                () -> statement.executeQuery("SELECT * FROM " + source));
            assertEquals("42000", error.getSQLState(), error.getMessage());
            assertTrue(error.getMessage().contains("Derived table alias declares"), error.getMessage());
            assertTrue(error.getMessage().contains("Failed to execute derived table 'x':"), error.getMessage());
          }
        }
      }
    }
  }

  /**
   * Checks positional alias mapping for parenthesized CTE references.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void parenthesizedCteAliasesBindByPosition(boolean pushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown=" + pushdown
        + "&projectionPushdown=" + pushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String source : List.of("(c) x(a, b)", "((c)) x(a, b)")) {
        String sql = "WITH c AS (SELECT id, id + 1 AS n FROM optimized WHERE id <= 2) " + "SELECT x.a, x.b FROM "
            + source + " ORDER BY x.a";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          for (int id = 1; id <= 2; id++) {
            assertTrue(result.next(), sql);
            assertEquals(Integer.toString(id), result.getString(1), sql);
            assertEquals(Integer.toString(id + 1), result.getString(2), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Rejects duplicate output aliases during parsing, before input execution.
   *
   * @param aliases
   *          duplicate column alias names
   * @throws Exception
   *           if the connection cannot be opened
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "a, a", "a, A", "\"Same\", \"Same\""
  })
  void duplicateAliasesHaveSqlDiagnostics(String aliases) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources/hf"))) {
      for (String body : List.of("SELECT id, id + 1 FROM optimized WHERE id <= 2",
          "SELECT id, id FROM optimized WHERE id < 0")) {
        String derived = "(" + body + ") x(" + aliases + ")";
        for (String source : List.of(derived, "(VALUES (1)) s(z) JOIN " + derived + " ON TRUE",
            "(VALUES (1)) s(z) JOIN LATERAL " + derived + " ON TRUE", "(SELECT * FROM " + derived + ") nested",
            "((" + body + ") inner_alias) x(" + aliases + ")")) {
          try (var statement = connection.createStatement()) {
            String sql = "SELECT * FROM " + source;
            IllegalArgumentException parseError = assertThrows(IllegalArgumentException.class,
                () -> SqlParser.parseSelect(sql));
            assertEquals("42000", SqlExceptions.findSqlStateFailure(parseError).getSQLState());
            SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql));
            assertEquals("42000", error.getSQLState(), error.getMessage());
            assertTrue(error.getMessage().contains("Duplicate derived table column alias:"), error.getMessage());
          }
        }
      }
    }
  }

  /**
   * Identifies the failing join input and preserves its context through nesting.
   *
   * @throws Exception
   *           if the connection cannot be opened
   */
  @Test
  void countErrorsIdentifyDerivedTable() throws Exception {
    String joined = "SELECT * FROM (SELECT id FROM optimized WHERE id <= 2) x(a) "
        + "JOIN (SELECT id, id FROM optimized WHERE id <= 2) y(b) ON TRUE";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources/hf"))) {
      for (String sql : List.of(joined, "SELECT * FROM (" + joined + ") outer_input")) {
        try (var statement = connection.createStatement()) {
          SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql));
          assertEquals("42000", error.getSQLState(), error.getMessage());
          String context = "Failed to execute derived table '";
          assertTrue(error.getMessage().contains(context + "y': Derived table alias declares"), error.getMessage());
          assertEquals(error.getMessage().indexOf(context), error.getMessage().lastIndexOf(context),
              error.getMessage());
        }
      }
    }
  }

}
