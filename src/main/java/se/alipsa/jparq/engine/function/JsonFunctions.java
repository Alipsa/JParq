package se.alipsa.jparq.engine.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.SqlText;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalInterval;
import se.alipsa.jparq.helper.TemporalValueType;

/**
 * SQL JSON_* functions using Jackson. Array access uses lax singleton wrapping.
 * From the first nonterminal array step onward, the default limits are 100,000
 * intermediate items per step and 100,000 item/range evaluations over the
 * entire path. The positive integer system property
 * {@code jparq.json.maxIntermediateItems} overrides both limits when an
 * evaluation first needs an intermediate array step. Temporal constructor
 * values use ISO-8601 text; UTC-adjusted timestamps use UTC and local
 * timestamps retain their wall-clock fields.
 */
public final class JsonFunctions {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter[] TIME_FORMATTERS = timeFormatters();

  private static final long MAX_ARRAY_LITERAL = Integer.MAX_VALUE + 1L;
  private static final String LAST_INDEX = "last";
  private static final String MAX_ITEMS_PROPERTY = "jparq.json.maxIntermediateItems";
  private static final int DEFAULT_MAX_ITEMS = 100_000;

  private static final String ARRAY_INDEX = "([+-]?[0-9]+|last(?: *[+-] *[0-9]+)?)";
  private static final Pattern ARRAY_RANGE = Pattern.compile(ARRAY_INDEX + "(?: +to +" + ARRAY_INDEX + ")?");

  private JsonFunctions() {
  }

  /**
   * Evaluate {@code JSON_VALUE(document, path)} and return a scalar value.
   *
   * @param document
   *          JSON document (string or {@link JsonNode})
   * @param path
   *          JSON path expression rooted at {@code $}; all member steps require a
   *          leading dot; double-quoted names use JSON string escapes; leading
   *          and trailing whitespace and whitespace between tokens are ignored;
   *          array steps accept integer indexes, {@code *}, {@code last},
   *          relative indexes such as {@code last-1}, inclusive ranges, and
   *          comma-separated selections
   * @return extracted scalar value or {@code null} when the path cannot be
   *         resolved to exactly one item
   * @throws IllegalArgumentException
   *           if the document or path is malformed, or the configured
   *           intermediate selection limit is invalid or exceeded
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
   *           if the document or path is malformed, or the configured
   *           intermediate selection limit is invalid or exceeded
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
   *          JSON path expression rooted at {@code $}; all member steps require a
   *          leading dot; double-quoted names use JSON string escapes; leading
   *          and trailing whitespace and whitespace between tokens are ignored;
   *          array steps accept integer indexes, {@code *}, {@code last},
   *          relative indexes such as {@code last-1}, inclusive ranges, and
   *          comma-separated selections
   * @return JSON string or {@code null} when the path does not select exactly one
   *         item
   * @throws IllegalArgumentException
   *           if the document or path is malformed, or the configured
   *           intermediate selection limit is invalid or exceeded
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
   *           if the document or path is malformed, or the configured
   *           intermediate selection limit is invalid or exceeded
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

  /**
   * Converts a constructor argument while retaining its temporal declaration.
   * Direct character arguments retain the JSON-text parsing heuristic.
   *
   * @param value
   *          evaluated argument or raw Avro column value
   * @param schema
   *          argument declaration, or null when unknown
   * @return corresponding JSON node
   * @throws IllegalArgumentException
   *           if a value is invalid for its declared logical type
   */
  public static JsonNode constructorValue(Object value, Schema schema) {
    if (schema == null) {
      return toNode(value);
    }
    Schema effective = JdbcTypeMapper.nonNullSchema(schema);
    if (effective != null && effective.getType() == Schema.Type.MAP) {
      // Direct MAP decoding is tracked separately in issue #313.
      return toNode(value);
    }
    if (effective != null && effective.getType() == Schema.Type.STRING
        && TemporalValueType.fromSchema(effective) == null) {
      return toNode(value == null ? null : value.toString());
    }
    return toNode(value, schema);
  }

  /**
   * Parses the document and evaluates a path rooted at {@code $}, ignoring
   * leading whitespace.
   *
   * @param document
   *          the JSON document (string or {@link JsonNode})
   * @param path
   *          the SQL/JSON path expression
   * @return the selected node, or null if the document, path, or match is missing
   * @throws IllegalArgumentException
   *           if the document or path is malformed, or the configured
   *           intermediate selection limit is invalid or exceeded
   */
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
    int root = skipWhitespace(pathExpr, 0);
    if (!pathExpr.startsWith("$", root)) {
      throw new IllegalArgumentException("JSON path must start with $");
    }
    return navigate(node, pathExpr.substring(root + 1));
  }

  /**
   * Traverses member and array steps, validating the remaining path after missing
   * values. All member steps require a leading dot. Whitespace separates tokens
   * and is preserved only in quoted names.
   *
   * @param node
   *          the document root
   * @param path
   *          the path after the root marker
   * @return the selected node, or null unless exactly one item matches
   * @throws IllegalArgumentException
   *           if a path step is malformed
   */
  private static JsonNode navigate(JsonNode node, String path) {
    // Without an intermediate array step, a terminal step has at most one input
    // item, so its work is linear in the path text and needs no intermediate
    // budget.
    ArrayBudget budget = null;
    List<JsonNode> current = node == null ? List.of() : List.of(node);
    int index = 0;
    while (index < path.length()) {
      char ch = path.charAt(index);
      if (ch == '[') {
        int close = path.indexOf(']', index);
        if (close < 0) {
          throw new IllegalArgumentException("Unclosed array index in JSON path");
        }
        List<ArrayRange> ranges = parseArraySubscript(path.substring(index + 1, close));
        boolean terminal = skipWhitespace(path, close + 1) == path.length();
        if (!terminal && budget == null) {
          budget = new ArrayBudget(maxIntermediateItems());
        }
        current = selectArrayItems(current, ranges, budget, terminal);
        index = close + 1;
      } else if (isPathWhitespace(ch)) {
        index = skipWhitespace(path, index);
      } else {
        if (ch == '"') {
          throw new IllegalArgumentException("Quoted member in JSON path must be preceded by a dot");
        }
        if (ch != '.') {
          int codePoint = path.codePointAt(index);
          throw new IllegalArgumentException(Character.isUnicodeIdentifierStart(codePoint) || ch == '$' || ch == '_'
              ? "Member in JSON path must be preceded by a dot"
              : String.format("Unexpected character '%s' (U+%04X) in JSON path", Character.toString(codePoint),
                  codePoint));
        }
        int start = skipWhitespace(path, index + 1);
        if (start == path.length() || isStepDelimiter(path.charAt(start))) {
          throw new IllegalArgumentException("Missing member name after dot in JSON path");
        }
        int next = findNextDelimiter(path, start);
        String prop = decodeMember(path.substring(start, next));
        List<JsonNode> selected = new ArrayList<>();
        for (JsonNode item : current) {
          JsonNode value = item.get(prop);
          if (value != null) {
            selected.add(value);
          }
        }
        current = selected;
        index = next;
      }
    }
    return current.size() == 1 ? current.getFirst() : null;
  }

  /**
   * Reads the per-evaluation bound on intermediate selection sizes.
   *
   * @return configured positive limit, or 100,000 by default
   * @throws IllegalArgumentException
   *           if the configured limit is not a positive integer
   */
  private static int maxIntermediateItems() {
    String configured = System.getProperty(MAX_ITEMS_PROPERTY);
    if (configured == null) {
      return DEFAULT_MAX_ITEMS;
    }
    try {
      int limit = Integer.parseInt(configured);
      if (limit > 0) {
        return limit;
      }
    } catch (NumberFormatException e) {
      throw SqlExceptions.invalidAttribute(MAX_ITEMS_PROPERTY + " must be a positive integer", e);
    }
    throw SqlExceptions.invalidAttribute(MAX_ITEMS_PROPERTY + " must be a positive integer");
  }

  /**
   * Selects array items, treating non-arrays as singleton arrays in lax mode.
   * Checks each complete range before allocating its results. Terminal steps
   * retain at most two items because multiple results always produce NULL.
   *
   * @param current
   *          input sequence
   * @param ranges
   *          ordered selections, including duplicates
   * @param budget
   *          shared evaluation budget, or null for a terminal step with no
   *          preceding intermediate array step
   * @param terminal
   *          whether no path steps remain
   * @return selected items, or at most two items for a terminal step
   * @throws IllegalArgumentException
   *           if an intermediate selection or array accessor work exceeds the
   *           limit
   */
  private static List<JsonNode> selectArrayItems(List<JsonNode> current, List<ArrayRange> ranges, ArrayBudget budget,
      boolean terminal) {
    if (!terminal) {
      budget.consume(current.size() * (long) ranges.size());
    }
    List<JsonNode> selected = new ArrayList<>();
    for (JsonNode item : current) {
      int size = item.isArray() ? item.size() : 1;
      for (ArrayRange range : ranges) {
        if (terminal && budget != null) {
          budget.consume(1);
        }
        int first = (int) Math.min(size, Math.max(0, range.first().resolve(size)));
        int last = (int) Math.max(-1, Math.min(size - 1L, range.last().resolve(size)));
        long count = Math.max(0L, last - (long) first + 1);
        if (!terminal && count > budget.limit - (long) selected.size()) {
          throw SqlExceptions.implementationLimit("JSON path intermediate selection exceeds " + MAX_ITEMS_PROPERTY
              + " limit of " + budget.limit + " items");
        }
        for (int position = first; position <= last; position++) {
          selected.add(item.isArray() ? item.get(position) : item);
          if (terminal && selected.size() == 2) {
            return selected;
          }
        }
      }
    }
    return selected;
  }

  /**
   * Creates a classified failure for excessive array accessor work.
   *
   * @param limit
   *          maximum item/range evaluations over the complete path
   * @return failure carrying SQLState 54000
   */
  private static IllegalArgumentException arrayWorkLimit(int limit) {
    return SqlExceptions.implementationLimit("JSON path array work exceeds " + MAX_ITEMS_PROPERTY + " limit of " + limit
        + " item/range evaluations for one path");
  }

  /** Shared memory limit and remaining accessor work for one path evaluation. */
  private static final class ArrayBudget {
    private final int limit;
    private long remainingWork;

    /**
     * Creates an evaluation budget without reading process settings again.
     *
     * @param limit
     *          maximum intermediate items and total item/range evaluations
     */
    private ArrayBudget(int limit) {
      this.limit = limit;
      remainingWork = limit;
    }

    /**
     * Reserves work before iterating, including ranges with no matches.
     *
     * @param work
     *          number of item/range evaluations
     * @throws IllegalArgumentException
     *           if the cumulative work exceeds the limit
     */
    private void consume(long work) {
      if (work > remainingWork) {
        throw arrayWorkLimit(limit);
      }
      remainingWork -= work;
    }
  }

  /**
   * Parses array selections before evaluating them, including after a missing
   * step.
   *
   * @param subscript
   *          bracket contents
   * @return inclusive index ranges in selection order
   * @throws IllegalArgumentException
   *           if the subscript is malformed
   */
  private static List<ArrayRange> parseArraySubscript(String subscript) {
    StringBuilder normalized = new StringBuilder();
    for (int i = 0; i < subscript.length(); i++) {
      char ch = subscript.charAt(i);
      normalized.append(isPathWhitespace(ch) ? ' ' : ch);
    }
    String text = trimPathWhitespace(normalized.toString());
    if ("*".equals(text)) {
      return List.of(new ArrayRange(new ArrayIndex(false, 0), new ArrayIndex(true, 0)));
    }
    List<ArrayRange> ranges = new ArrayList<>();
    for (String selection : text.split(",", -1)) {
      var matcher = ARRAY_RANGE.matcher(trimPathWhitespace(selection));
      if (!matcher.matches()) {
        throw new IllegalArgumentException("Unsupported array subscript in JSON path: [" + subscript + "]");
      }
      ArrayIndex first = parseArrayIndex(matcher.group(1));
      ArrayIndex last = matcher.group(2) == null ? first : parseArrayIndex(matcher.group(2));
      ranges.add(new ArrayRange(first, last));
    }
    return ranges;
  }

  /**
   * Parses an integer index or an offset from the last array element.
   *
   * @param text
   *          validated index expression
   * @return index independent of the current array's size
   */
  private static ArrayIndex parseArrayIndex(String text) {
    if (!text.startsWith(LAST_INDEX)) {
      return new ArrayIndex(false, parseBoundedIndex(text));
    }
    String offset = text.substring(LAST_INDEX.length()).replace(" ", "");
    return new ArrayIndex(true, offset.isEmpty() ? 0 : parseBoundedIndex(offset));
  }

  /**
   * Parses a validated integer with saturation just beyond any possible array.
   * Once saturated, the remaining digits cannot affect selection or clipping.
   *
   * @param text
   *          signed ASCII integer literal
   * @return signed magnitude capped at Integer.MAX_VALUE + 1
   */
  private static long parseBoundedIndex(String text) {
    boolean negative = text.charAt(0) == '-';
    int start = negative || text.charAt(0) == '+' ? 1 : 0;
    long magnitude = 0;
    for (int i = start; i < text.length(); i++) {
      magnitude = Math.min(MAX_ARRAY_LITERAL, magnitude * 10 + text.charAt(i) - '0');
      if (magnitude == MAX_ARRAY_LITERAL) {
        break;
      }
    }
    return negative ? -magnitude : magnitude;
  }

  /**
   * An array index, optionally relative to the final element.
   *
   * @param fromLast
   *          whether to add the final element's index
   * @param offset
   *          signed index or relative offset
   */
  private record ArrayIndex(boolean fromLast, long offset) {
    /**
     * Resolves the index without overflowing when adding a relative offset.
     *
     * @param size
     *          array size
     * @return zero-based index, possibly outside the array bounds
     */
    private long resolve(int size) {
      return fromLast ? size - 1L + offset : offset;
    }
  }

  /**
   * An inclusive range; a single index has identical endpoints.
   *
   * @param first
   *          lower endpoint
   * @param last
   *          upper endpoint
   */
  private record ArrayRange(ArrayIndex first, ArrayIndex last) {
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
   *           if a quoted member is unclosed or a member has a suffix
   */
  private static int findNextDelimiter(String path, int start) {
    if (start < path.length() && path.charAt(start) == '"') {
      return findQuotedMemberEnd(path, start);
    }
    int i = start;
    while (i < path.length()) {
      char c = path.charAt(i);
      if (isStepDelimiter(c) || isPathWhitespace(c)) {
        break;
      }
      i++;
    }
    validateMemberEnd(path, i, false);
    return i;
  }

  /**
   * Locates the closing quote and validates the next step after whitespace.
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
        validateMemberEnd(path, next, true);
        return next;
      }
    }
    throw new IllegalArgumentException("Unclosed quoted member in JSON path");
  }

  /**
   * Requires a step delimiter or the end of the path after a member token.
   *
   * @param path
   *          the path after the root marker
   * @param end
   *          the offset after the member token
   * @param quoted
   *          whether the member was quoted, for the error diagnostic
   * @throws IllegalArgumentException
   *           if a member has an unexpected suffix
   */
  private static void validateMemberEnd(String path, int end, boolean quoted) {
    int after = skipWhitespace(path, end);
    if (after < path.length() && !isStepDelimiter(path.charAt(after))) {
      throw new IllegalArgumentException(
          "Unexpected character after " + (quoted ? "quoted member" : "member") + " in JSON path");
    }
  }

  /**
   * Recognizes the delimiters that begin member and array steps.
   *
   * @param ch
   *          the character to classify
   * @return whether the character begins a path step
   */
  private static boolean isStepDelimiter(char ch) {
    return ch == '.' || ch == '[';
  }

  /**
   * Removes leading and trailing path whitespace from subscript text.
   *
   * @param text
   *          the subscript text
   * @return the text without surrounding path whitespace
   */
  private static String trimPathWhitespace(String text) {
    int start = skipWhitespace(text, 0);
    int end = text.length();
    while (end > start && isPathWhitespace(text.charAt(end - 1))) {
      end--;
    }
    return text.substring(start, end);
  }

  /**
   * Skips whitespace between path tokens without modifying quoted contents.
   *
   * @param path
   *          the path or subscript text
   * @param start
   *          the offset from which to skip whitespace
   * @return the offset of the next non-whitespace character or the path length
   */
  private static int skipWhitespace(String path, int start) {
    int index = start;
    while (index < path.length() && isPathWhitespace(path.charAt(index))) {
      index++;
    }
    return index;
  }

  /**
   * Recognizes ECMAScript WhiteSpace and LineTerminator characters, including
   * Unicode space separators and the byte order mark, but excluding other control
   * characters.
   *
   * @param ch
   *          the character to classify
   * @return whether the character separates path tokens
   */
  private static boolean isPathWhitespace(char ch) {
    return Character.getType(ch) == Character.SPACE_SEPARATOR || ch == '\t' || ch == '\u000b' || ch == '\f'
        || ch == '\n' || ch == '\r' || ch == '\u2028' || ch == '\u2029' || ch == '\ufeff';
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
    if (value instanceof Time time) {
      return toNode(DateTimeFunctions.toLocalTime(time));
    }
    if (value instanceof LocalTime time) {
      return MAPPER.getNodeFactory().textNode(DateTimeFormatter.ISO_LOCAL_TIME.format(time));
    }
    if (value instanceof Timestamp timestamp) {
      return toNode(timestamp.toLocalDateTime());
    }
    if (value instanceof LocalDateTime timestamp) {
      return MAPPER.getNodeFactory().textNode(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(timestamp));
    }
    if (value instanceof OffsetTime time) {
      return MAPPER.getNodeFactory().textNode(DateTimeFormatter.ISO_OFFSET_TIME.format(time));
    }
    if (value instanceof OffsetDateTime timestamp) {
      return MAPPER.getNodeFactory().textNode(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp));
    }
    if (value instanceof TemporalInterval interval) {
      return MAPPER.getNodeFactory().textNode(interval.toString());
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
    if (value instanceof Time time) {
      return timeNode(DateTimeFunctions.toLocalTime(time), effective);
    }
    if (value instanceof LocalTime time) {
      return timeNode(time, effective);
    }
    if (value instanceof LocalDateTime || value instanceof OffsetDateTime || value instanceof OffsetTime) {
      return toNode(value);
    }
    TemporalValueType temporal = TemporalValueType.fromSchema(effective);
    if (temporal != null) {
      if (temporal == TemporalValueType.UTC_TIMESTAMP) {
        return MAPPER.getNodeFactory().textNode(temporal.encode(value));
      }
      return toNode(temporal.normalize(value));
    }
    if (AvroCoercions.isLocalTimestamp(effective)) {
      if (value instanceof Number number) {
        return toNode(
            LocalDateTime.ofInstant(AvroCoercions.timestampInstant(number.longValue(), effective), ZoneOffset.UTC));
      }
      return toNode(value instanceof Timestamp timestamp ? timestamp.toLocalDateTime() : value);
    }
    if (AvroCoercions.isTimestamp(effective)) {
      Timestamp timestamp = (Timestamp) AvroCoercions.unwrap(value, effective);
      return MAPPER.getNodeFactory().textNode(timestamp.toInstant().toString());
    }
    if (IntervalType.matches(effective)) {
      return toNode(AvroCoercions.unwrap(value, effective));
    }
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
   * Formats TIME with an explicit SQL fractional precision when available.
   * Storage resolution alone does not impose fractional padding.
   *
   * @param time
   *          decoded wall-clock time
   * @param schema
   *          value declaration
   * @return ISO time text preserving explicitly declared fractional digits
   */
  private static JsonNode timeNode(LocalTime time, Schema schema) {
    String precision = schema.getProp("jparq.temporal.precision");
    DateTimeFormatter formatter = precision == null
        ? DateTimeFormatter.ISO_LOCAL_TIME
        : TIME_FORMATTERS[Integer.parseInt(precision)];
    return MAPPER.getNodeFactory().textNode(formatter.format(time));
  }

  /**
   * Builds immutable TIME formatters once for supported SQL precisions.
   *
   * @return formatters indexed by fractional-second precision
   */
  private static DateTimeFormatter[] timeFormatters() {
    DateTimeFormatter[] formatters = new DateTimeFormatter[10];
    for (int digits = 0; digits < formatters.length; digits++) {
      var builder = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss");
      if (digits > 0) {
        builder.appendFraction(ChronoField.NANO_OF_SECOND, digits, digits, true);
      }
      formatters[digits] = builder.toFormatter();
    }
    return formatters;
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
   * Preserves the numeric type and precision when constructing a JSON node,
   * keeping floats at single precision to avoid widening artifacts.
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
    if (number instanceof Float) {
      return MAPPER.getNodeFactory().numberNode(number.floatValue());
    }
    if (number instanceof Double) {
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
