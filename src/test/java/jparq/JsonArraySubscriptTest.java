package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.function.JsonFunctions;

/** Regression coverage for SQL/JSON array selections from issue 303. */
class JsonArraySubscriptTest {
  @TempDir
  Path directory;

  /** Selects final elements, relative indexes, ranges, and lists. */
  @Test
  void selectsArrayElements() {
    String json = "{\"a\":[5,6,7]}";
    for (String subscript : List.of("last", "last-0", "last - 0", "2 to 2", "2 to last", "2,9", "9,2", "-1 to 0,9",
        "last-2 to 0")) {
      String expected = subscript.contains("-1 to 0") || subscript.equals("last-2 to 0") ? "5" : "7";
      assertEquals(expected, JsonFunctions.jsonValue(json, "$.a[" + subscript + "]").toString(), subscript);
      assertEquals(expected, JsonFunctions.jsonQuery(json, "$.a[" + subscript + "]"), subscript);
    }
    assertEquals(6, JsonFunctions.jsonValue(json, "$.a[last-1]"));
    assertEquals(7, JsonFunctions.jsonValue(json, "$.a[last\u00a0-\u2028 0]"));
    assertEquals(7, JsonFunctions.jsonValue(json, "$.a[2 to 2147483647]"));
    assertEquals(5, JsonFunctions.jsonValue(json, "$.a[-2147483648 to 0]"));
    assertEquals("[1,2]", JsonFunctions.jsonQuery("[[1,2]]", "$[*]"));
  }

  /**
   * Applies later steps to the entire sequence, with last relative to each array.
   */
  @Test
  void continuesAfterMultipleMatches() {
    String json = "{\"a\":[{\"b\":[1,2]}, {}, {\"b\":[3,4,5]}]}";
    assertEquals(3, JsonFunctions.jsonValue(json, "$.a[*].b[last-2]"));
    assertEquals("3", JsonFunctions.jsonQuery(json, "$.a[0 to 2].b[last-2]"));
    assertNull(JsonFunctions.jsonValue(json, "$.a[last,0].b[1].missing[*]"));
    assertEquals(3, JsonFunctions.jsonValue(json, "$.a[0,last].b[last-2 to last-2]"));
    assertNull(JsonFunctions.jsonQuery(json, "$.a[*].b[last]"));
    assertEquals(2, JsonFunctions.jsonValue("[[1],[2,3,5]]", "$[*][last-2]"));
  }

  /** Returns NULL for empty or multiple selections without an array wrapper. */
  @Test
  void returnsNullForNonSingletonSelections() {
    for (String document : List.of("{\"a\":[1,2,3]}", "{\"a\":[]}", "{\"a\":null}", "{\"a\":1}", "{}")) {
      for (String subscript : List.of("*", "0 to 2", "0,last", "last+1", "last-2147483648", "3 to 0", "2147483647",
          "last+2147483647", "-1")) {
        String path = "$.a[" + subscript + "]";
        assertNull(JsonFunctions.jsonValue(document, path), document + path);
        assertNull(JsonFunctions.jsonQuery(document, path), document + path);
      }
    }
    assertNull(JsonFunctions.jsonValue("[7]", "$[0,0]"));
    assertNull(JsonFunctions.jsonQuery("[null]", "$[*]"));
    assertEquals(7, JsonFunctions.jsonValue("[7]", "$[*]"));
    assertEquals("7", JsonFunctions.jsonQuery("[7]", "$[0 to 2]"));
  }

  /** Validates later steps even when earlier selections are empty or multiple. */
  @Test
  void validatesTheCompletePath() {
    for (String prefix : List.of("$.missing[*]", "$.a[*]", "$.a[2 to 0]")) {
      for (String suffix : List.of("[last-]", "[0 to]", "[0,]", "[last+2147483648]", "[", ".\"bad\\q\"")) {
        String path = prefix + suffix;
        assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonValue("{\"a\":[1,2]}", path), path);
        assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonQuery("{\"a\":[1,2]}", path), path);
      }
    }
  }

  /**
   * Evaluates standard subscripts through SQL literals and bound parameters.
   *
   * @param function
   *          extraction function
   * @throws SQLException
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "JSON_VALUE", "JSON_QUERY"
  })
  void evaluatesSqlArraySelections(String function) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT " + function + "(?, ?)");
        var literal = connection.createStatement()) {
      statement.setString(1, "{\"a\":[1,2,3]}");
      for (String path : List.of("$.a[last]", "$.a[last - 1]", "$.a[*]", "$.a[0 to 2]", "$.missing[*]")) {
        statement.setString(2, path);
        String expected = path.equals("$.a[last]") ? "3" : path.equals("$.a[last - 1]") ? "2" : null;
        try (var result = statement.executeQuery();
            var literalResult = literal.executeQuery("SELECT " + function + "('{\"a\":[1,2,3]}', '" + path + "')")) {
          assertTrue(result.next());
          assertTrue(literalResult.next());
          assertEquals(expected, result.getString(1));
          assertEquals(expected, literalResult.getString(1));
          assertFalse(result.next());
        }
      }
    }
  }
}
