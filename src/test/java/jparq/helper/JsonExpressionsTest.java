package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.JsonExpressions;

/** Tests for {@link JsonExpressions}. */
class JsonExpressionsTest {

  @Test
  void extractsValuesAndFragmentsFromJson() {
    String json = """
        { "name": "Ada", "age": 12, "active": true, "nested": { "value": 7 }, "items": [1, 2, 3] }
        """;

    assertEquals("Ada", JsonExpressions.jsonValue(json, "$.name"));
    assertEquals(12, JsonExpressions.jsonValue(json, "$.age"));
    assertEquals(Boolean.TRUE, JsonExpressions.jsonValue(json, "$.active"));
    assertEquals("7", JsonExpressions.jsonValue(json, "$.nested.value").toString());
    assertEquals("[1,2,3]", JsonExpressions.jsonQuery(json, "$.items"));
    assertNull(JsonExpressions.jsonValue(json, "$.missing"));
  }

  @Test
  void buildsJsonObjectAndArray() {
    List<Object> objectArgs = Arrays.asList("name", "Alice", "\"age\"", 30, null, "ignored", " active ", true);
    String objectJson = JsonExpressions.jsonObject(objectArgs);
    assertTrue(objectJson.contains("\"name\":\"Alice\""));
    assertTrue(objectJson.contains("\"age\":30"));
    assertTrue(objectJson.contains("\"active\":true"));

    List<Object> arrayArgs = List.of("x", 2, "{\"k\": \"v\"}", JsonNodeFactory.instance.booleanNode(false));
    assertEquals("[\"x\",2,{\"k\":\"v\"},false]", JsonExpressions.jsonArray(arrayArgs));
    assertEquals("{}", JsonExpressions.jsonObject(List.of()));
  }

  @Test
  void validatesInput() {
    assertThrows(IllegalArgumentException.class, () -> JsonExpressions.jsonObject(List.of("onlyKey")));
    assertThrows(IllegalArgumentException.class, () -> JsonExpressions.jsonValue("{}", "name"));
    assertThrows(IllegalArgumentException.class, () -> JsonExpressions.jsonValue("not-json", "$.name"));
  }

  @Test
  void resolvesJsonNodesDirectly() {
    JsonNode node = JsonNodeFactory.instance.objectNode().put("flag", true);
    assertEquals(Boolean.TRUE, JsonExpressions.jsonValue(node, "$.flag"));
    assertNull(JsonExpressions.jsonQuery(null, "$.flag"));
  }
}
