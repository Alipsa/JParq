package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** JDBC regression coverage for SQL/JSON path member names and whitespace. */
class JsonPathTest {
  @TempDir
  Path directory;

  /**
   * Skips whitespace around quoted and unquoted steps in SQL literals.
   *
   * @throws SQLException
   *           if querying fails
   */
  @Test
  void evaluatesWhitespaceInSqlLiterals() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT JSON_QUERY('{"a":{"b":1}}', '$.a .b'),
                   JSON_QUERY('{"a":[1,2]}', '$.a [1]'),
                   JSON_QUERY('{"a":1}', '$. "a"'),
                   JSON_VALUE('{"a":{"b":1}}', '$.a .b'),
                   JSON_VALUE('{"a":[1,2]}', '$.a [1]'),
                   JSON_VALUE('{"a":1}', '$. "a"')
            """)) {
      assertTrue(result.next());
      String[] expected = {
          "1", "2", "1", "1", "2", "1"
      };
      for (int column = 0; column < expected.length; column++) {
        assertEquals(expected[column], result.getString(column + 1));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Skips tabs and line breaks in bound paths while preserving quoted spaces.
   *
   * @param function
   *          the extraction function
   * @throws SQLException
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "JSON_VALUE", "JSON_QUERY"
  })
  void evaluatesWhitespaceInBoundPaths(String function) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT " + function + "(?, ?)")) {
      statement.setString(1, "{\" a \":[{\"b\":7}]}");
      statement.setString(2, "\ufeff\u00a0$ \t.\n\" a \"\r[\u2028 0 \u2029]\t.\n b \r");
      try (var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals("7", result.getString(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Reports missing member names as JDBC errors instead of matching empty keys.
   *
   * @param function
   *          the extraction function
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "JSON_VALUE", "JSON_QUERY"
  })
  void rejectsEmptyMembersInBoundPaths(String function) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT " + function + "(?, ?)")) {
      statement.setString(1, "{\"\":{\"a\":1},\" \":2}");
      for (String path : new String[]{
          "$.", "$. ", "$. .a", "$. [0]", "$.missing. [0]"
      }) {
        statement.setString(2, path);
        SQLException failure = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery()) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        }, path);
        assertTrue(failure.getMessage().contains("Missing member name after dot in JSON path"), failure::getMessage);
      }
    }
  }

  /**
   * Extracts quoted names after SQL literal decoding, including nested steps.
   *
   * @throws SQLException
   *           if querying fails
   */
  @Test
  void evaluatesQuotedMembersInSqlLiterals() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT JSON_VALUE('{"a b":1}', '$."a b"'),
                   JSON_VALUE('{"it''s":1}', '$."it''s"'),
                   JSON_VALUE('{"a":1}', '$."a"'),
                   JSON_VALUE('{"a.b":[{"c[d]":7}]}', '$."a.b"[0]."c[d]"'),
                   JSON_QUERY('{"a b":{"it''s":[1,2]}}', '$."a b"."it''s"'),
                   JSON_VALUE('{"a\\"b":2}', '$."a\\"b"'),
                   JSON_QUERY('{"a b":1}', '$."missing"'),
                   JSON_VALUE('{"a":[3,7]}', '$."a" [ 1 ]'),
                   JSON_QUERY('{"a":{"b":9}}', '$."a" .b')
            """)) {
      assertTrue(result.next());
      assertEquals(1, result.getInt(1));
      assertEquals(1, result.getInt(2));
      assertEquals(1, result.getInt(3));
      assertEquals(7, result.getInt(4));
      assertEquals("[1,2]", result.getString(5));
      assertEquals(2, result.getInt(6));
      assertNull(result.getObject(7));
      assertTrue(result.wasNull());
      assertEquals(7, result.getInt(8));
      assertEquals("9", result.getString(9));
      assertFalse(result.next());
    }
  }

  /**
   * Preserves escaped quotes, backslashes, and apostrophes in bound paths.
   *
   * @param function
   *          the extraction function
   * @throws SQLException
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "JSON_VALUE", "JSON_QUERY"
  })
  void evaluatesBoundQuotedMembers(String function) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT " + function + "(?, ?)")) {
      statement.setString(1, "{\"a\\\"b\\\\c's\":[{\"x.y\":9}]}");
      statement.setString(2, "$.\"a\\\"b\\\\c's\"[0].\"x.y\"");
      try (var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals("9", result.getString(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Returns SQL NULL for negative indexes and selects zero-based positive
   * indexes.
   *
   * @param function
   *          the extraction function
   * @throws SQLException
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "JSON_VALUE", "JSON_QUERY"
  })
  void resolvesSignedArrayIndexes(String function) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + function + """
            ('{"a":{"c":[5,6]}}', '$."a".c[-1]'),
            """ + function + """
            ('{"a":{"c":[5,6]}}', '$."a".c[+1]')
            """)) {
      assertTrue(result.next());
      assertNull(result.getObject(1));
      assertTrue(result.wasNull());
      assertEquals("6", result.getString(2));
      assertFalse(result.next());
    }
  }

  /**
   * Reports invalid paths as JDBC errors with path-specific diagnostics.
   *
   * @param function
   *          the extraction function
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "JSON_VALUE", "JSON_QUERY"
  })
  void rejectsInvalidPaths(String function) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      Map<String, String> invalidPaths = Map.of("$.missing.\"bad\\q\"", "Invalid quoted member in JSON path",
          "$.missing[*]", "Unsupported array subscript in JSON path: [*]", "$.missing[last]",
          "Unsupported array subscript in JSON path: [last]", "$\"a\"",
          "Quoted member in JSON path must be preceded by a dot", "$.\"a\" b",
          "Unexpected character after quoted member in JSON path", "$.\"a\".\"b\" x",
          "Unexpected character after quoted member in JSON path", "$.missing[]",
          "Unsupported array subscript in JSON path: []", "$.missing[ ]",
          "Unsupported array subscript in JSON path: [ ]", "$.missing . a b",
          "Unexpected character after member in JSON path", "$.a\tb", "Unexpected character after member in JSON path");
      for (var entry : invalidPaths.entrySet()) {
        SQLException failure = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery("SELECT " + function + "('{}', '" + entry.getKey() + "')")) {
            while (result.next()) {
              result.getObject(1);
            }
          }
        });
        assertTrue(failure.getMessage().contains(entry.getValue()), failure::getMessage);
      }
    }
  }
}
