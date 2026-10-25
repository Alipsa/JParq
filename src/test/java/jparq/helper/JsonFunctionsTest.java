package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.function.JsonFunctions;

/** Tests for {@link JsonFunctions}. */
class JsonFunctionsTest {

  @Test
  void extractsValuesAndFragmentsFromJson() {
    String json = """
        { "name": "Ada", "age": 12, "active": true, "nested": { "value": 7 }, "items": [1, 2, 3] }
        """;

    assertEquals("Ada", JsonFunctions.jsonValue(json, "$.name"));
    assertEquals(12, JsonFunctions.jsonValue(json, "$.age"));
    assertEquals(Boolean.TRUE, JsonFunctions.jsonValue(json, "$.active"));
    assertEquals("7", JsonFunctions.jsonValue(json, "$.nested.value").toString());
    assertEquals("[1,2,3]", JsonFunctions.jsonQuery(json, "$.items"));
    assertNull(JsonFunctions.jsonValue(json, "$.missing"));
  }

  /**
   * Decodes quoted member names without interpreting their contents as path
   * steps.
   */
  @Test
  void extractsQuotedMembers() {
    Map<String, String> paths = Map.ofEntries(Map.entry("a", "$.\"a\""), Map.entry("a b", "$.\"a b\""),
        Map.entry("it's", "$.\"it's\""), Map.entry("a.b[0]", "$.\"a.b[0]\""), Map.entry("", "$.\"\""),
        Map.entry("a\"b", "$.\"a\\\"b\""), Map.entry("a\\b", "$.\"a\\\\b\""), Map.entry("a\nb", "$.\"a\\nb\""),
        Map.entry("a\tb", "$.\"a\\tb\""), Map.entry("é", "$.\"\\u00e9\""), Map.entry("a/b", "$.\"a\\/b\""));
    for (var entry : paths.entrySet()) {
      JsonNode node = JsonNodeFactory.instance.objectNode().put(entry.getKey(), 1);
      assertEquals(1, JsonFunctions.jsonValue(node, entry.getValue()), entry.getValue());
      assertEquals("1", JsonFunctions.jsonQuery(node.toString(), entry.getValue()), entry.getValue());
    }
    String json = "{\"a.b\":{\"items\":[{\"c[d]\":{\"value\":7}}]}}";
    assertEquals(7, JsonFunctions.jsonValue(json, "$.\"a.b\".items[0].\"c[d]\".value"));
    assertEquals("{\"value\":7}", JsonFunctions.jsonQuery(json, "$.\"a.b\".items[0].\"c[d]\""));
    assertNull(JsonFunctions.jsonValue(json, "$.\"missing\".value"));
    assertNull(JsonFunctions.jsonQuery(json, "$.\"a.b\".\"missing\""));
  }

  /** Rejects malformed quoted names even after a member lookup has failed. */
  @Test
  void rejectsMalformedQuotedMembers() {
    for (String path : List.of("$.\"unterminated", "$.\"trailing\\", "$.\"bad\\q\"", "$.\"bad\\u12\"",
        "$.\"line\nbreak\"", "$.\"a\"suffix", "$.\"a\"\"b\"", "$.missing.\"unterminated", "$.\"a\" b",
        "$.\"a\".\"b\" x", "$.\"a\" \"b\"", "$.\"a\"\t.b")) {
      assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonValue("{}", path), path);
      assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonQuery("{}", path), path);
    }
  }

  /** Allows spaces between quoted member steps and trims numeric subscripts. */
  @Test
  void allowsSpacesAfterQuotedMembers() {
    String json = "{\"a\":[3,7],\"obj\":{\"b\":9}}";
    for (String path : List.of("$.\"a\" [1]", "$.\"a\"  [ 1 ]", "$.\"a\"[\t+1\n]")) {
      assertEquals(7, JsonFunctions.jsonValue(json, path), path);
      assertEquals("7", JsonFunctions.jsonQuery(json, path), path);
    }
    assertEquals(9, JsonFunctions.jsonValue(json, "$.\"obj\" .b"));
    assertEquals("9", JsonFunctions.jsonQuery(json, "$.\"obj\" .b"));
    assertEquals(9, JsonFunctions.jsonValue(json, "$.\"obj\".\"b\"  "));
    assertEquals("9", JsonFunctions.jsonQuery(json, "$.\"obj\".\"b\"  "));
  }

  /**
   * Gives unsupported subscripts a path-specific diagnostic even after a miss.
   */
  @Test
  void rejectsUnsupportedArraySubscripts() {
    for (String subscript : List.of("*", "last", "0 to 1", "", " ", "2147483648")) {
      for (String member : List.of("a", "missing")) {
        String path = "$." + member + "[" + subscript + "]";
        String expected = "Unsupported array subscript in JSON path: [" + subscript + "]";
        var valueFailure = assertThrows(IllegalArgumentException.class,
            () -> JsonFunctions.jsonValue("{\"a\":[1]}", path), path);
        var queryFailure = assertThrows(IllegalArgumentException.class,
            () -> JsonFunctions.jsonQuery("{\"a\":[1]}", path), path);
        assertEquals(expected, valueFailure.getMessage());
        assertEquals(expected, queryFailure.getMessage());
        assertInstanceOf(NumberFormatException.class, valueFailure.getCause());
        assertInstanceOf(NumberFormatException.class, queryFailure.getCause());
      }
    }
  }

  /** Requires a dot before quoted members at the root and after array steps. */
  @Test
  void rejectsQuotedMembersWithoutDot() {
    for (String path : List.of("$\"a\"", "$ \"a\"", "$[0]\"a\"", "$.missing[0]\"a\"")) {
      var valueFailure = assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonValue("{\"a\":1}", path),
          path);
      var queryFailure = assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonQuery("{\"a\":1}", path),
          path);
      assertEquals("Quoted member in JSON path must be preceded by a dot", valueFailure.getMessage());
      assertEquals(valueFailure.getMessage(), queryFailure.getMessage());
    }
  }

  /** Distinguishes a member's delimiters from literal quotes within the key. */
  @Test
  void distinguishesKeysContainingLiteralQuotes() {
    JsonNode node = JsonNodeFactory.instance.objectNode().put("a", 1).put("\"a\"", 2);
    assertEquals(1, JsonFunctions.jsonValue(node, "$.\"a\""));
    assertEquals("1", JsonFunctions.jsonQuery(node, "$.\"a\""));
    assertEquals(2, JsonFunctions.jsonValue(node, "$.\"\\\"a\\\"\""));
    assertEquals("2", JsonFunctions.jsonQuery(node, "$.\"\\\"a\\\"\""));
  }

  @Test
  void buildsJsonObjectAndArray() {
    List<Object> objectArgs = Arrays.asList("name", "Alice", "\"age\"", 30, null, "ignored", " active ", true);
    String objectJson = JsonFunctions.jsonObject(objectArgs);
    assertTrue(objectJson.contains("\"name\":\"Alice\""));
    assertTrue(objectJson.contains("\"\\\"age\\\"\":30"));
    assertTrue(objectJson.contains("\" active \":true"));

    List<Object> arrayArgs = List.of("x", 2, "{\"k\": \"v\"}", JsonNodeFactory.instance.booleanNode(false));
    assertEquals("[\"x\",2,{\"k\":\"v\"},false]", JsonFunctions.jsonArray(arrayArgs));
    assertEquals("{}", JsonFunctions.jsonObject(List.of()));
  }

  /** Embeds Avro records as objects, including nested records and SQL NULLs. */
  @Test
  void embedsStructValuesAsObjects() {
    var childSchema = org.apache.avro.SchemaBuilder.record("child").fields().requiredInt("n").endRecord();
    var schema = org.apache.avro.SchemaBuilder.record("parent").fields().requiredString("s").name("child")
        .type(childSchema).noDefault().optionalString("missing").endRecord();
    var child = new org.apache.avro.generic.GenericData.Record(childSchema);
    child.put("n", 7);
    var record = new org.apache.avro.generic.GenericData.Record(schema);
    record.put("s", "it's");
    record.put("child", child);
    record.put("missing", null);
    String json = "{\"s\":\"it's\",\"child\":{\"n\":7},\"missing\":null}";
    assertEquals("{\"k\":" + json + "}", JsonFunctions.jsonObject(List.of("k", record)));
    assertEquals("[" + json + "]", JsonFunctions.jsonArray(List.of(record)));
  }

  @Test
  void validatesInput() {
    assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonObject(List.of("onlyKey")));
    assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonValue("{}", "name"));
    assertThrows(IllegalArgumentException.class, () -> JsonFunctions.jsonValue("not-json", "$.name"));
  }

  @Test
  void resolvesJsonNodesDirectly() {
    JsonNode node = JsonNodeFactory.instance.objectNode().put("flag", true);
    assertEquals(Boolean.TRUE, JsonFunctions.jsonValue(node, "$.flag"));
    assertNull(JsonFunctions.jsonQuery(null, "$.flag"));
  }
}
