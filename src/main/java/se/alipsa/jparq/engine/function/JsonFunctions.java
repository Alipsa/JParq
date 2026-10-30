package se.alipsa.jparq.engine.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.SqlText;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/** Minimal implementations of SQL JSON_* functions using Jackson. */
public final class JsonFunctions {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonFunctions() {
  }

  /**
   * Evaluate {@code JSON_VALUE(document, path)} and return a scalar value.
   *
   * @param document
   *          JSON document (string or {@link JsonNode})
   * @param path
   *          JSON path expression beginning with {@code $}; double-quoted member
   *          names use JSON string escapes
   * @return extracted scalar value or {@code null} when the path cannot be
   *         resolved
   * @throws IllegalArgumentException
   *           if the document or path is malformed
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
   * Convenience overload for {@code JSON_VALUE} handling positional argument
   * lists.
   *
   * @param args
   *          positional arguments
   * @return extracted value or {@code null} when inputs are missing
   * @throws IllegalArgumentException
   *           if the document or path is malformed
   */
  public static Object jsonValue(List<Object> args) {
    if (args == null || args.size() < 2) {
      return null;
    }
    return jsonValue(args.get(0), args.get(1));
  }

  /**
   * Evaluate {@code JSON_QUERY(document, path)} and return a JSON fragment.
   *
   * @param document
   *          JSON document (string or {@link JsonNode})
   * @param path
   *          JSON path expression beginning with {@code $}; double-quoted member
   *          names use JSON string escapes
   * @return JSON string or {@code null} when the path cannot be resolved
   * @throws IllegalArgumentException
   *           if the document or path is malformed
   */
  public static String jsonQuery(Object document, Object path) {
    JsonNode node = resolve(document, path);
    if (node == null || node.isMissingNode() || node.isNull()) {
      return null;
    }
    return node.toString();
  }

  /**
   * Convenience overload for {@code JSON_QUERY} handling positional argument
   * lists.
   *
   * @param args
   *          positional arguments
   * @return JSON fragment or {@code null} when inputs are missing
   * @throws IllegalArgumentException
   *           if the document or path is malformed
   */
  public static String jsonQuery(List<Object> args) {
    if (args == null || args.size() < 2) {
      return null;
    }
    return jsonQuery(args.get(0), args.get(1));
  }

  /**
   * Construct a JSON object from an alternating sequence of evaluated keys and
   * values. Key text is preserved, including whitespace and literal quotes.
   * Arrays and records are converted recursively, using Avro logical types for
   * record fields. Binary values use base64 strings and NULLs use JSON null.
   * Strings nested in containers remain text; only direct value arguments use the
   * JSON-text parsing heuristic.
   *
   * @param args
   *          alternating key/value arguments
   * @return serialized JSON object
   * @throws IllegalArgumentException
   *           if the argument count is odd
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
      obj.set(SqlText.valueOf(key), toNode(value));
    }
    return obj.toString();
  }

  /**
   * Construct a JSON array from the supplied arguments. Nested arrays and records
   * are converted recursively, using Avro logical types for record fields. Binary
   * values use base64 strings and NULLs use JSON null. Strings nested in
   * containers remain text; only direct arguments use the JSON-text parsing
   * heuristic.
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
        node = MAPPER.readTree(SqlText.valueOf(document));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Invalid JSON document", e);
      }
    }
    String pathExpr = SqlText.valueOf(path);
    if (!pathExpr.startsWith("$")) {
      throw new IllegalArgumentException("JSON path must start with $");
    }
    return navigate(node, pathExpr.substring(1));
  }

  /**
   * Traverses member and array steps, validating the remaining path after missing
   * values.
   *
   * @param node
   *          the document root
   * @param path
   *          the path after the root marker
   * @return the selected node, or null if a step has no match
   * @throws IllegalArgumentException
   *           if a path step is malformed
   */
  private static JsonNode navigate(JsonNode node, String path) {
    JsonNode current = node;
    int index = 0;
    while (index < path.length()) {
      char ch = path.charAt(index);
      if (ch == '[') {
        int close = path.indexOf(']', index);
        if (close < 0) {
          throw new IllegalArgumentException("Unclosed array index in JSON path");
        }
        String idxStr = path.substring(index + 1, close);
        int arrayIndex;
        try {
          arrayIndex = Integer.parseInt(idxStr.trim());
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Unsupported array subscript in JSON path: [" + idxStr + "]", e);
        }
        current = current != null && current.isArray() ? current.get(arrayIndex) : null;
        index = close + 1;
      } else if (ch == ' ') {
        index++;
      } else {
        if (ch == '"') {
          throw new IllegalArgumentException("Quoted member in JSON path must be preceded by a dot");
        }
        int start = ch == '.' ? index + 1 : index;
        int next = findNextDelimiter(path, start);
        String prop = decodeMember(path.substring(start, next));
        current = current == null ? null : current.get(prop);
        index = next;
      }
    }
    return current;
  }

  /**
   * Decodes a quoted member using JSON string rules, preserving unquoted names.
   *
   * @param member
   *          the member token, including any quotes
   * @return the lookup key
   * @throws IllegalArgumentException
   *           if the quoted member has invalid JSON escapes
   */
  private static String decodeMember(String member) {
    if (!member.startsWith("\"")) {
      return member;
    }
    try {
      return MAPPER.readValue(member, String.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid quoted member in JSON path", e);
    }
  }

  /**
   * Finds the end of a member token without splitting inside quoted names.
   *
   * @param path
   *          the path after the root marker
   * @param start
   *          the member's starting offset
   * @return the offset after the member token
   * @throws IllegalArgumentException
   *           if a quoted member is unclosed or has a suffix
   */
  private static int findNextDelimiter(String path, int start) {
    if (start < path.length() && path.charAt(start) == '"') {
      return findQuotedMemberEnd(path, start);
    }
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

  /**
   * Locates the closing quote and validates the next step after any plain spaces.
   *
   * @param path
   *          the path after the root marker
   * @param start
   *          the opening quote's offset
   * @return the offset after the closing quote
   * @throws IllegalArgumentException
   *           if the member is unclosed or has a suffix
   */
  private static int findQuotedMemberEnd(String path, int start) {
    for (int i = start + 1; i < path.length(); i++) {
      char ch = path.charAt(i);
      if (ch == '\\') {
        i++;
      } else if (ch == '"') {
        int next = i + 1;
        int after = next;
        while (after < path.length() && path.charAt(after) == ' ') {
          after++;
        }
        if (after < path.length() && path.charAt(after) != '.' && path.charAt(after) != '[') {
          throw new IllegalArgumentException("Unexpected character after quoted member in JSON path");
        }
        return next;
      }
    }
    throw new IllegalArgumentException("Unclosed quoted member in JSON path");
  }

  /**
   * Converts a Java or Avro value into a JSON node recursively.
   *
   * @param value
   *          evaluated value
   * @return corresponding JSON node
   */
  private static JsonNode toNode(Object value) {
    if (value == null) {
      return MAPPER.nullNode();
    }
    if (value instanceof JsonNode node) {
      return node;
    }
    if (value instanceof GenericRecord record) {
      ObjectNode object = MAPPER.createObjectNode();
      for (Schema.Field field : record.getSchema().getFields()) {
        object.set(field.name(), toNode(record.get(field.pos()), field.schema()));
      }
      return object;
    }
    if (value instanceof GenericArray<?> array) {
      return arrayNode(array, array.getSchema().getElementType());
    }
    if (value instanceof List<?> list) {
      return arrayNode(list, null);
    }
    if (value instanceof Object[] array) {
      return arrayNode(Arrays.asList(array), null);
    }
    if (value instanceof byte[] bytes) {
      return MAPPER.getNodeFactory().binaryNode(bytes);
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer duplicate = buffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return MAPPER.getNodeFactory().binaryNode(bytes);
    }
    if (value instanceof Map<?, ?> map) {
      return mapNode(map, null);
    }
    if (value instanceof String str) {
      String trimmed = str.trim();
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        try {
          return MAPPER.readTree(trimmed);
        } catch (JsonProcessingException e) {
          return MAPPER.getNodeFactory().textNode(str);
        }
      }
      return MAPPER.getNodeFactory().textNode(str);
    }
    if (value instanceof Number num) {
      return numberNode(num);
    }
    if (value instanceof Boolean bool) {
      return MAPPER.getNodeFactory().booleanNode(bool);
    }
    return MAPPER.getNodeFactory().textNode(SqlText.valueOf(value));
  }

  /**
   * Applies an Avro schema before converting a nested value to JSON. Without a
   * schema, nested strings remain text rather than being parsed as JSON.
   *
   * @param value
   *          raw field or element value
   * @param schema
   *          declared Avro schema, or null for an already decoded Java value
   * @return corresponding JSON node
   */
  private static JsonNode toNode(Object value, Schema schema) {
    if (schema == null && value instanceof String text) {
      return MAPPER.getNodeFactory().textNode(text);
    }
    if (value == null || schema == null) {
      return toNode(value);
    }
    Schema effective = JdbcTypeMapper.nonNullSchema(schema);
    return switch (effective.getType()) {
      case ARRAY -> arrayNode(value instanceof Object[] array ? Arrays.asList(array) : (Iterable<?>) value,
          effective.getElementType());
      case MAP -> mapNode((Map<?, ?>) value, effective.getValueType());
      case STRING, ENUM -> MAPPER.getNodeFactory().textNode(SqlText.valueOf(value));
      case BYTES -> toNode(effective.getLogicalType() == null ? value : AvroCoercions.unwrap(value, effective));
      default -> toNode(AvroCoercions.unwrap(value, effective));
    };
  }

  /**
   * Converts array elements recursively, preserving their order and NULLs.
   *
   * @param values
   *          array elements
   * @param elementSchema
   *          element schema, or null for decoded values
   * @return JSON array
   */
  private static ArrayNode arrayNode(Iterable<?> values, Schema elementSchema) {
    ArrayNode array = MAPPER.createArrayNode();
    for (Object value : values) {
      array.add(toNode(value, elementSchema));
    }
    return array;
  }

  /**
   * Converts map values recursively while preserving string keys.
   *
   * @param values
   *          map entries
   * @param valueSchema
   *          value schema, or null for decoded values
   * @return JSON object
   */
  private static ObjectNode mapNode(Map<?, ?> values, Schema valueSchema) {
    ObjectNode object = MAPPER.createObjectNode();
    for (var entry : values.entrySet()) {
      object.set(SqlText.valueOf(entry.getKey()), toNode(entry.getValue(), valueSchema));
    }
    return object;
  }

  /**
   * Preserves the numeric type and precision when constructing a JSON node.
   *
   * @param number
   *          numeric value
   * @return numeric node, or text for an unparseable custom number
   */
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
