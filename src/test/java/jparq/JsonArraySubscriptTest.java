package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.function.JsonFunctions;

/** Regression coverage for SQL/JSON array selections from issue 303. */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class JsonArraySubscriptTest {
  @TempDir
  Path directory;

  /** Selects final elements, relative indexes, ranges, and lists. */
  @Test
  void selectsArrayElements() {
    String json = "{\"a\":[5,6,7]}";
    Map<String, String> selections = Map.of("last", "7", "last-0", "7", "last - 0", "7", "2 to 2", "7", "2 to last",
        "7", "2,9", "7", "9,2", "7", "-1 to 0,9", "5", "last-2 to 0", "5");
    for (var entry : selections.entrySet()) {
      String path = "$.a[" + entry.getKey() + "]";
      assertEquals(entry.getValue(), JsonFunctions.jsonValue(json, path).toString(), path);
      assertEquals(entry.getValue(), JsonFunctions.jsonQuery(json, path), path);
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
    for (String document : List.of("{\"a\":[1,2,3]}", "{\"a\":[]}", "{}")) {
      for (String subscript : List.of("*", "0 to 2", "0,last", "last+1", "last-2147483648", "3 to 0", "2147483647",
          "last+2147483647", "-1")) {
        String path = "$.a[" + subscript + "]";
        assertNull(JsonFunctions.jsonValue(document, path), document + path);
        assertNull(JsonFunctions.jsonQuery(document, path), document + path);
      }
    }
    assertNull(JsonFunctions.jsonValue("[7]", "$[0,0]"));
    assertEquals(7, JsonFunctions.jsonValue("[7]", "$[*]"));
    assertEquals("7", JsonFunctions.jsonQuery("[7]", "$[0 to 2]"));
  }

  /** Validates later steps even when earlier selections are empty or multiple. */
  @Test
  void validatesTheCompletePath() {
    for (String prefix : List.of("$.missing[*]", "$.a[*]", "$.a[2 to 0]")) {
      for (String suffix : List.of("[last-]", "[0 to]", "[0,]", "[last+oops]", "[", ".\"bad\\q\"")) {
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
      Map<String, String> paths = new HashMap<>();
      paths.put("$.a[last]", "3");
      paths.put("$.a[last - 1]", "2");
      paths.put("$.a[*]", null);
      paths.put("$.a[0 to 2]", null);
      paths.put("$.missing[*]", null);
      for (var entry : paths.entrySet()) {
        String path = entry.getKey();
        statement.setString(2, path);
        String expected = entry.getValue();
        try (var result = statement.executeQuery();
            var literalResult = literal.executeQuery("SELECT " + function + "('{\"a\":[1,2,3]}', '" + path + "')")) {
          assertTrue(result.next());
          assertTrue(literalResult.next());
          assertEquals(expected, result.getString(1));
          assertEquals(expected, literalResult.getString(1));
          assertFalse(result.next());
          assertFalse(literalResult.next());
        }
      }
    }
  }
  /** Wraps scalars and objects as singleton arrays for lax array access. */
  @Test
  void wrapsNonArrays() {
    for (String subscript : List.of("*", "0", "last", "0 to 2", "last-0", "-9 to last", "99,0")) {
      String path = "$.a[" + subscript + "]";
      assertEquals(1, JsonFunctions.jsonValue("{\"a\":1}", path), path);
      assertEquals("1", JsonFunctions.jsonQuery("{\"a\":1}", path), path);
      assertEquals("{\"b\":1}", JsonFunctions.jsonQuery("{\"a\":{\"b\":1}}", path), path);
      assertEquals(1, JsonFunctions.jsonValue("{\"a\":{\"b\":1}}", path + ".b"), path);
    }
    assertNull(JsonFunctions.jsonValue("{\"a\":1}", "$.a[0,last]"));
    assertNull(JsonFunctions.jsonQuery("{\"a\":1}", "$.a[0,last]"));
  }

  /**
   * Applies lax singleton wrapping to bound SQL/JSON paths.
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
  void wrapsNonArraysThroughJdbc(String function) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT " + function + "(?, ?)")) {
      statement.setString(1, "{\"a\":1}");
      for (String subscript : List.of("*", "0", "last", "0 to 2")) {
        statement.setString(2, "$.a[" + subscript + "]");
        try (var result = statement.executeQuery()) {
          assertTrue(result.next());
          assertEquals("1", result.getString(1), subscript);
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Clips arbitrary-precision endpoints and relative indexes without overflow.
   */
  @Test
  void clipsLargeIndexes() {
    String huge = "9999999999999999999999999999999999999999";
    Map<String, String> ranges = Map.of("2 to 9999999999", "3", "2 to " + huge, "3", "-" + huge + " to 0", "1",
        "last-" + huge + " to 0", "1", "2 to last+" + huge, "3");
    for (var entry : ranges.entrySet()) {
      assertEquals(entry.getValue(), JsonFunctions.jsonQuery("[1,2,3]", "$[" + entry.getKey() + "]"), entry.getKey());
    }
    for (String subscript : List.of(huge, "-" + huge, "last+" + huge, "last-" + huge)) {
      assertNull(JsonFunctions.jsonValue("[1,2,3]", "$[" + subscript + "]"), subscript);
    }
  }

  /**
   * Bounds duplicate growth, cumulative ranges, and whole ranges before
   * expansion.
   */
  @Test
  void boundsIntermediateSelections() {
    withSelectionLimit("64", () -> {
      String repeated = "[" + String.join(",", Collections.nCopies(40, "0")) + "]";
      String explosivePath = "$" + repeated.repeat(6);
      var failure = assertThrows(IllegalArgumentException.class,
          () -> JsonFunctions.jsonValue("[[[[[[1]]]]]]", explosivePath));
      assertTrue(failure.getMessage().contains("jparq.json.maxIntermediateItems limit of 64"));
      assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonQuery("[[[[[[1]]]]]]", explosivePath));
    });
    withSelectionLimit("2", () -> {
      for (String path : List.of("$[0 to last].b", "$[0 to 1,0].b", "$[*][0,0].b")) {
        assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonValue("[{\"b\":1},{},{}]", path), path);
      }
      assertEquals(1, JsonFunctions.jsonValue("[{\"b\":1},{}]", "$[0 to last].b"));
      assertNull(JsonFunctions.jsonQuery("[1,2,3]", "$[*]   "));
      assertNull(JsonFunctions.jsonValue("[1,2,3]", "$[0 to last,0 to last]"));
      // A terminal shortcut must not skip validation of a later malformed step.
      assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonValue("[1,2]", "$[*][bad]"));
    });
  }

  /** Rejects invalid limit settings instead of silently removing the bound. */
  @Test
  void validatesSelectionLimit() {
    for (String setting : List.of("0", "-1", "oops", "2147483648")) {
      withSelectionLimit(setting, () -> {
        var failure = assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonValue("[1]", "$[0].a"));
        assertEquals("jparq.json.maxIntermediateItems must be a positive integer", failure.getMessage());
        assertEquals("54000", SqlExceptions.findSqlStateFailure(failure).getSQLState());
      });
    }
  }

  /**
   * Reports bound-parameter resource failures as implementation limits through
   * JDBC.
   *
   * @param function
   *          extraction function
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "JSON_VALUE", "JSON_QUERY"
  })
  void reportsSelectionLimitThroughJdbc(String function) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT " + function + "(?, ?)")) {
      statement.setString(1, "[{\"b\":1},{},{}]");
      statement.setString(2, "$[0 to last].b");
      Map<String, String> settings = Map.of("2", "jparq.json.maxIntermediateItems limit of 2", "oops",
          "jparq.json.maxIntermediateItems must be a positive integer");
      for (var entry : settings.entrySet()) {
        withSelectionLimit(entry.getKey(), () -> {
          var failure = assertThrows(SQLException.class, () -> {
            try (var result = statement.executeQuery()) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          });
          assertEquals("54000", failure.getSQLState());
          assertTrue(failure.getMessage().contains(entry.getValue()), failure::getMessage);
        });
      }
    }
  }

  /** Keeps JSON null items distinct from an empty or multiple-item selection. */
  @Test
  void selectsNullItems() {
    for (String subscript : List.of("*", "0", "last", "0 to 2")) {
      assertNull(JsonFunctions.jsonValue("{\"a\":null}", "$.a[" + subscript + "]"));
      assertEquals("[null]", JsonFunctions.jsonQuery("[[null]]", "$[" + subscript + "]"));
    }
  }

  /**
   * Avoids consulting array limits for paths without an intermediate array step.
   */
  @Test
  void ignoresUnusedLimitSettings() {
    withSelectionLimit("oops", () -> {
      for (String path : List.of("$.a", "$.a[0]", "$.a[last]", "$.a[*]", "$.a[0 to 2]")) {
        assertEquals(1, JsonFunctions.jsonValue("{\"a\":1}", path), path);
        assertEquals("1", JsonFunctions.jsonQuery("{\"a\":1}", path), path);
      }
    });
  }

  /** Bounds item/range work even when every range selects nothing. */
  @Test
  void boundsEmptyRangeWork() {
    var document = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.arrayNode();
    for (int i = 0; i < 64; i++) {
      document.add(0);
    }
    withSelectionLimit("64", () -> {
      String ranges = "[" + String.join(",", Collections.nCopies(65, "5")) + "]";
      for (String path : List.of("$[*]" + ranges + ".x", "$[*]" + ranges)) {
        var failure = assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonValue(document, path), path);
        assertTrue(failure.getMessage().contains("array work"), failure::getMessage);
        assertEquals("54000", SqlExceptions.findSqlStateFailure(failure).getSQLState());
      }
    });
  }

  /**
   * Parses oversized numeric literals in linear time without large-integer
   * arithmetic.
   */
  @Test
  void boundsHugeLiteralParsingCost() {
    String huge = "9".repeat(300_000);
    assertTimeout(Duration.ofSeconds(10), () -> {
      for (int row = 0; row < 10; row++) {
        assertNull(JsonFunctions.jsonValue("[1,2,3]", "$[" + huge + "]"));
        assertEquals("3", JsonFunctions.jsonQuery("[1,2,3]", "$[2 to last+" + huge + "]"));
      }
    });
  }

  /**
   * Runs a check with a limit setting and restores the previous process setting.
   *
   * @param setting
   *          property value
   * @param check
   *          test operation
   */
  private static void withSelectionLimit(String setting, Runnable check) {
    String property = "jparq.json.maxIntermediateItems";
    String previous = System.setProperty(property, setting);
    try {
      check.run();
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }
}
