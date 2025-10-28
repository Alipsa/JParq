package se.alipsa.jparq.helper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;

/** Minimal implementations of SQL JSON_* functions using Jackson. */
public final class JsonExpressions {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonExpressions() {
  }

  /**
   * Evaluate {@code JSON_VALUE(document, path)} and return a scalar value.
   *
   * @param document
   *          JSON document (string or {@link JsonNode})
   * @param path
   *          JSON path expression beginning with {@code $}
   * @return extracted scalar value or {@code null} when the path cannot be resolved
   */
  public static Object jsonValue(Object document, Object path) {
    JsonNode node = resolve(document, path);
    if (node == null || node.isMissingNode()) {
      return null;
    }
    if (node.isTextual()) {
      return node.asText();
    }
    if (node.isNumber()) {
      return node.numberValue();
    }
    if (node.isBoolean()) {
      return node.booleanValue();
    }
    if (node.isNull()) {
      return null;
    }
    return node.toString();
  }

  /**
   * Evaluate {@code JSON_QUERY(document, path)} and return a JSON fragment.
   *
   * @param document
   *          JSON document (string or {@link JsonNode})
   * @param path
   *          JSON path expression beginning with {@code $}
   * @return JSON string or {@code null} when the path cannot be resolved
   */
  public static String jsonQuery(Object document, Object path) {
    JsonNode node = resolve(document, path);
    if (node == null || node.isMissingNode() || node.isNull()) {
      return null;
    }
    return node.toString();
  }

  /**
   * Construct a JSON object from an alternating sequence of keys and values.
   *
   * @param args
   *          alternating key/value arguments
   * @return serialized JSON object
   */
  public static String jsonObject(List<Object> args) {
    if (args == null || args.isEmpty()) {
      return "{}";
    }
    if (args.size() % 2 != 0) {
      throw new IllegalArgumentException("JSON_OBJECT requires an even number of arguments");
    }
    ObjectNode obj = MAPPER.createObjectNode();
    Iterator<Object> it = args.iterator();
    while (it.hasNext()) {
      Object key = it.next();
      Object value = it.next();
      if (key == null) {
        continue;
      }
      String field = normalizeKey(key);
      if (field == null) {
        continue;
      }
      obj.set(field, toNode(value));
    }
    return obj.toString();
  }

  /**
   * Construct a JSON array from the supplied arguments.
   *
   * @param args
   *          values to include in the array
   * @return serialized JSON array
   */
  public static String jsonArray(List<Object> args) {
    ArrayNode array = MAPPER.createArrayNode();
    if (args != null) {
      for (Object value : args) {
        array.add(toNode(value));
      }
    }
    return array.toString();
  }

  private static JsonNode resolve(Object document, Object path) {
    if (document == null || path == null) {
      return null;
    }
    JsonNode node;
    if (document instanceof JsonNode existing) {
      node = existing;
    } else {
      try {
        node = MAPPER.readTree(document.toString());
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Invalid JSON document", e);
      }
    }
    String pathExpr = path.toString();
    if (!pathExpr.startsWith("$")) {
      throw new IllegalArgumentException("JSON path must start with $");
    }
    return navigate(node, pathExpr.substring(1));
  }

  private static JsonNode navigate(JsonNode node, String path) {
    JsonNode current = node;
    int index = 0;
    while (index < path.length() && current != null && !current.isMissingNode()) {
      char ch = path.charAt(index);
      if (ch == '.') {
        index++;
        int next = findNextDelimiter(path, index);
        String prop = path.substring(index, next);
        current = current.get(prop);
        index = next;
      } else if (ch == '[') {
        int close = path.indexOf(']', index);
        if (close < 0) {
          throw new IllegalArgumentException("Unclosed array index in JSON path");
        }
        String idxStr = path.substring(index + 1, close);
        int arrayIndex = Integer.parseInt(idxStr);
        current = current != null && current.isArray() ? current.get(arrayIndex) : null;
        index = close + 1;
      } else if (ch == ' ') {
        index++;
      } else {
        int next = findNextDelimiter(path, index);
        String prop = path.substring(index, next);
        current = current.get(prop);
        index = next;
      }
    }
    return current;
  }

  private static int findNextDelimiter(String path, int start) {
    int i = start;
    while (i < path.length()) {
      char c = path.charAt(i);
      if (c == '.' || c == '[') {
        break;
      }
      i++;
    }
    return i;
  }

  private static JsonNode toNode(Object value) {
    if (value == null) {
      return MAPPER.nullNode();
    }
    if (value instanceof JsonNode node) {
      return node;
    }
    if (value instanceof String str) {
      try {
        String trimmed = str.trim();
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
          return MAPPER.readTree(trimmed);
        }
      } catch (JsonProcessingException ignore) {
        // treat as plain string
      }
      return MAPPER.getNodeFactory().textNode(str);
    }
    if (value instanceof Number num) {
      return numberNode(num);
    }
    if (value instanceof Boolean bool) {
      return MAPPER.getNodeFactory().booleanNode(bool);
    }
    return MAPPER.getNodeFactory().textNode(value.toString());
  }

  private static String normalizeKey(Object key) {
    if (key == null) {
      return null;
    }
    String field = key.toString().trim();
    if (field.isEmpty()) {
      return null;
    }
    if ((field.startsWith("\"") && field.endsWith("\"")) || (field.startsWith("'") && field.endsWith("'"))) {
      field = field.substring(1, field.length() - 1);
    }
    return field;
  }

  private static JsonNode numberNode(Number number) {
    if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
      return MAPPER.getNodeFactory().numberNode(number.intValue());
    }
    if (number instanceof Long) {
      return MAPPER.getNodeFactory().numberNode(number.longValue());
    }
    if (number instanceof BigInteger bigInt) {
      return MAPPER.getNodeFactory().numberNode(bigInt);
    }
    if (number instanceof BigDecimal bigDec) {
      return MAPPER.getNodeFactory().numberNode(bigDec);
    }
    if (number instanceof Float || number instanceof Double) {
      return MAPPER.getNodeFactory().numberNode(number.doubleValue());
    }
    try {
      BigDecimal decimal = new BigDecimal(number.toString());
      return MAPPER.getNodeFactory().numberNode(decimal);
    } catch (NumberFormatException e) {
      return MAPPER.getNodeFactory().textNode(number.toString());
    }
  }
}
