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

/** JDBC regression coverage for quoted SQL/JSON path member names. */
class JsonPathTest {
  @TempDir
  Path directory;

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
          "$.missing[*]", "Unsupported array subscript in JSON path: *", "$.missing[last]",
          "Unsupported array subscript in JSON path: last", "$\"a\"",
          "Quoted member in JSON path must be preceded by a dot");
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
