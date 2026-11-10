package se.alipsa.jparq.engine;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import se.alipsa.jparq.engine.function.ArrayFunctions;
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.engine.function.JsonFunctions;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalValueType;

/**
 * Retains evaluated temporal values together with their source declarations
 * until common type promotion, JSON serialization, or engine evaluation needs
 * them. Extracted from the value evaluator so the promotion rules can be
 * reviewed and tested on their own.
 */
public final class TemporalOperands {

  /** Prevents utility class instantiation. */
  private TemporalOperands() {
  }

  /**
   * Retains an evaluated value's declaration until temporal promotion is applied.
   *
   * @param value
   *          decoded value or already serialized array
   * @param declaration
   *          source declaration, or null when unknown
   */
  public record Operand(Object value, Schema declaration) {

    /**
     * Serializes the source value without reinterpreting an existing node.
     *
     * @return corresponding JSON node
     */
    public JsonNode node() {
      if (value instanceof Elements array) {
        List<JsonNode> nodes = new ArrayList<>();
        for (Operand element : array.elements()) {
          nodes.add(element.node());
        }
        return JsonFunctions.constructorValue(nodes, null);
      }
      return value instanceof JsonNode node ? node : JsonFunctions.constructorValue(value, declaration);
    }

    /**
     * Returns decoded values for shared ARRAY validation and temporal promotion.
     *
     * @return value retaining its UTC instant or local fields
     */
    public Object decoded() {
      if (value instanceof Elements array) {
        return array.elements().stream().map(Operand::decoded).toList();
      }
      if (value instanceof Timestamp timestamp) {
        return AvroCoercions.utcTimestamp(declaration)
            ? timestamp.toInstant().atOffset(ZoneOffset.UTC)
            : timestamp.toLocalDateTime();
      }
      return value;
    }

    /**
     * Decodes nested stored ARRAY sources before engine offset promotion.
     *
     * @return decoded elements retaining each UTC source's instant
     */
    public Object engineDecoded() {
      if (value instanceof Elements array) {
        return array.elements().stream().map(Operand::engineDecoded).toList();
      }
      Schema schema = JdbcTypeMapper.nonNullSchema(declaration);
      if (value != null && schema != null && schema.getType() == Schema.Type.ARRAY) {
        Iterable<?> elements = elementsOf(value);
        if (elements == null) {
          return decoded();
        }
        List<Object> decoded = new ArrayList<>();
        for (Object element : elements) {
          decoded.add(new Operand(element, schema.getElementType()).engineDecoded());
        }
        return decoded;
      }
      return decoded();
    }

    /**
     * Returns a value in the common engine representation, validating arrays once.
     *
     * @return JDBC or java.time value matching the common declaration
     */
    public Object engineValue() {
      if (value == null) {
        return null;
      }
      if (value instanceof Elements array) {
        List<Object> values = new ArrayList<>();
        for (Operand element : array.elements()) {
          values.add(element.engineDecoded());
        }
        return ArrayFunctions.homogenizeArrayValues(values, declaration);
      }
      Schema effective = JdbcTypeMapper.nonNullSchema(declaration);
      if (effective != null && effective.getType() == Schema.Type.ARRAY) {
        List<Object> values = new ArrayList<>();
        Iterable<?> elements = elementsOf(value);
        if (elements != null) {
          elements.forEach(values::add);
        }
        return ArrayFunctions.homogenizeArrayValues(values);
      }
      TemporalValueType temporal = TemporalValueType.fromSchema(effective);
      if (temporal != null) {
        return temporal.normalize(value);
      }
      return value instanceof LocalDateTime && effective != null && AvroCoercions.isLocalTimestamp(effective)
          ? DateTimeFunctions.toTimestamp(value)
          : value;
    }
  }

  /**
   * Retains decoded ARRAY elements until validation and promotion are complete.
   *
   * @param elements
   *          evaluated elements with source declarations
   */
  public record Elements(List<Operand> elements) {
  }

  /**
   * Views an array or nested value as elements, or null when it is neither an
   * Object[] nor an Iterable.
   *
   * @param value
   *          evaluated value
   * @return its elements, or null when not an array-like value
   */
  public static Iterable<?> elementsOf(Object value) {
    return value instanceof Object[] array
        ? Arrays.asList(array)
        : value instanceof Iterable<?> iterable ? iterable : null;
  }

  /**
   * Promotes decoded temporal values without formatting or reparsing JSON text.
   *
   * @param source
   *          source value with its original declaration
   * @param target
   *          result declaration, or null when unknown
   * @return promoted value
   */
  public static Operand promote(Operand source, Schema target) {
    if (target == null || source.value() == null) {
      return source;
    }
    Schema effective = JdbcTypeMapper.nonNullSchema(target);
    if (source.value() instanceof Elements array && effective.getType() == Schema.Type.ARRAY) {
      List<Operand> elements = array.elements().stream().map(element -> promote(element, effective.getElementType()))
          .toList();
      return new Operand(new Elements(elements), target);
    }
    if (effective.getType() == Schema.Type.ARRAY && source.declaration() != null
        && !sameArrayType(effective, source.declaration())) {
      Iterable<?> values = elementsOf(source.value());
      if (values == null) {
        return source;
      }
      Schema sourceElement = JdbcTypeMapper.nonNullSchema(source.declaration()).getElementType();
      List<Operand> elements = new ArrayList<>();
      for (Object value : values) {
        elements.add(promote(new Operand(value, sourceElement), effective.getElementType()));
      }
      return new Operand(new Elements(elements), target);
    }
    TemporalValueType temporal = TemporalValueType.fromSchema(effective);
    if (temporal == TemporalValueType.OFFSET_TIMESTAMP || temporal == TemporalValueType.OFFSET_TIME) {
      return new Operand(temporal.normalize(source.decoded()), effective);
    }
    if (temporal == TemporalValueType.JDBC_LOCAL_TIMESTAMP || temporal == TemporalValueType.LOCAL_TIMESTAMP) {
      // A JDBC Timestamp is already the engine's wall-clock view; keep it intact
      // so UTC instants survive grouping and set operations across DST overlaps.
      // JSON and materialization convert to fields only where they need them.
      return new Operand(source.value(), effective);
    }
    if (temporal == null && JdbcTypeMapper.mapSchemaToJdbcType(effective) == Types.TIME) {
      // Adopt the common TIME declaration (including its fractional precision)
      // so JSON serialization pads every row to the same width.
      return new Operand(source.value(), effective);
    }
    return source;
  }

  /**
   * Identifies arrays whose datetime leaves need their source declarations.
   *
   * @param schema
   *          array or leaf declaration
   * @return whether the innermost array elements are datetime values
   */
  public static boolean temporalElements(Schema schema) {
    Schema element = JdbcTypeMapper.innermostElementType(schema);
    int type = JdbcTypeMapper.mapSchemaToJdbcType(element);
    return type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP || type == Types.TIME_WITH_TIMEZONE
        || type == Types.TIMESTAMP_WITH_TIMEZONE;
  }

  /**
   * Identifies ARRAY declarations requiring offset-aware source conversion.
   *
   * @param schema
   *          ARRAY declaration
   * @return whether its leaves carry an explicit time zone
   */
  public static boolean offsetArray(Schema schema) {
    Schema leaf = JdbcTypeMapper.innermostElementType(schema);
    int type = JdbcTypeMapper.mapSchemaToJdbcType(leaf);
    return type == Types.TIMESTAMP_WITH_TIMEZONE || type == Types.TIME_WITH_TIMEZONE;
  }

  /**
   * Identifies explicit offset sources, including NULL and nested ARRAY values.
   *
   * @param operand
   *          source operand
   * @return whether a source has an instant or explicit offset
   */
  public static boolean offsetOperand(Operand operand) {
    if (operand.value() instanceof OffsetDateTime || operand.value() instanceof OffsetTime) {
      return true;
    }
    if (operand.value() instanceof Elements array
        && array.elements().stream().anyMatch(TemporalOperands::offsetOperand)) {
      return true;
    }
    if (operand.declaration() == null) {
      return false;
    }
    Schema declaration = JdbcTypeMapper.innermostElementType(operand.declaration());
    int type = JdbcTypeMapper.mapSchemaToJdbcType(declaration);
    return type == Types.TIMESTAMP_WITH_TIMEZONE || type == Types.TIME_WITH_TIMEZONE;
  }

  /**
   * Retains the common ARRAY declaration, including runtime offset sources whose
   * declarations were unknown. Local-only arrays keep wall-clock values.
   *
   * @param declaration
   *          common ARRAY or leaf declaration
   * @param values
   *          source elements at any nesting depth
   * @return ARRAY or leaf serialization declaration
   */
  public static Schema arrayTarget(Schema declaration, List<Operand> values) {
    Schema effective = JdbcTypeMapper.nonNullSchema(declaration);
    if (effective.getType() == Schema.Type.ARRAY) {
      return Schema.createArray(arrayTarget(effective.getElementType(), values));
    }
    boolean zoned = values.stream().anyMatch(TemporalOperands::offsetOperand);
    int type = JdbcTypeMapper.mapSchemaToJdbcType(effective);
    if (zoned && type == Types.TIMESTAMP) {
      return TemporalValueType.OFFSET_TIMESTAMP.schema();
    }
    if (zoned && type == Types.TIME) {
      return TemporalValueType.OFFSET_TIME.schema();
    }
    return effective;
  }

  /**
   * Identifies timestamp combinations and datetime ARRAY results.
   *
   * @param schema
   *          conditional result declaration
   * @return whether source-aware engine evaluation is required
   */
  public static boolean timestampResult(Schema schema) {
    Schema effective = JdbcTypeMapper.nonNullSchema(schema);
    int type = JdbcTypeMapper.mapSchemaToJdbcType(effective);
    return type == Types.TIMESTAMP || type == Types.TIMESTAMP_WITH_TIMEZONE
        || effective.getType() == Schema.Type.ARRAY && offsetArray(effective);
  }

  /**
   * Compares ARRAY declarations without treating optional elements as promotions.
   *
   * @param first
   *          target declaration
   * @param second
   *          source declaration
   * @return whether their non-null element types agree at every depth
   */
  private static boolean sameArrayType(Schema first, Schema second) {
    Schema left = JdbcTypeMapper.nonNullSchema(first);
    Schema right = JdbcTypeMapper.nonNullSchema(second);
    if (left.getType() == Schema.Type.ARRAY && right.getType() == Schema.Type.ARRAY) {
      return sameArrayType(left.getElementType(), right.getElementType());
    }
    return left.equals(right);
  }
}
