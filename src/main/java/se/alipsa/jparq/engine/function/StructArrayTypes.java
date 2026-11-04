package se.alipsa.jparq.engine.function;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import se.alipsa.jparq.NumericType;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/**
 * Resolves SQL field types and converts STRUCT array elements without mutating
 * input records.
 */
final class StructArrayTypes {
  /** Prevents utility class construction. */
  private StructArrayTypes() {
  }

  /**
   * Combines compatible SQL field types, ignoring Avro names and storage
   * properties.
   *
   * @param left
   *          first STRUCT declaration
   * @param right
   *          next STRUCT declaration
   * @return common STRUCT declaration
   * @throws IllegalArgumentException
   *           if a field is incompatible, carrying SQLState 42000
   */
  static Schema common(Schema left, Schema right) {
    return common(left, right, "", new HashMap<>());
  }

  /**
   * Resolves one field or container recursively, retaining optional constraints.
   *
   * @param first
   *          first declaration
   * @param second
   *          second declaration
   * @param path
   *          field path for diagnostics
   * @param visited
   *          already combined schema pairs, including recursive records
   * @return common declaration
   * @throws IllegalArgumentException
   *           if declarations are incompatible
   */
  private static Schema common(Schema first, Schema second, String path, Map<List<Schema>, Schema> visited) {
    if (first.equals(second)) {
      return first;
    }
    Schema left = optionalValue(first);
    Schema right = optionalValue(second);
    List<Schema> pair = List.of(left, right);
    Schema result = visited.get(pair);
    if (result == null) {
      if (left.getType() == Schema.Type.UNION || right.getType() == Schema.Type.UNION) {
        throw mismatch(path, "UNION", "incompatible UNION");
      }
      int leftType = JdbcTypeMapper.mapSchemaToJdbcType(left);
      int rightType = JdbcTypeMapper.mapSchemaToJdbcType(right);
      NumericType a = NumericType.of(leftType, left);
      NumericType b = NumericType.of(rightType, right);
      if (a != null && b != null) {
        result = a.common(b).schema();
      } else if (left.getType() == Schema.Type.RECORD && right.getType() == Schema.Type.RECORD) {
        result = Schema.createRecord(left.getName(), null, left.getNamespace(), false);
        visited.put(pair, result);
        if (left.getFields().size() != right.getFields().size()) {
          throw mismatch(path, "STRUCT with " + left.getFields().size() + " fields",
              "STRUCT with " + right.getFields().size() + " fields");
        }
        List<Schema.Field> fields = new ArrayList<>();
        for (int index = 0; index < left.getFields().size(); index++) {
          Schema.Field x = left.getFields().get(index);
          Schema.Field y = right.getFields().get(index);
          String fieldPath = path.isEmpty() ? x.name() : path + "." + x.name();
          if (!x.name().equals(y.name())) {
            throw mismatch(fieldPath, "field '" + x.name() + "'", "field '" + y.name() + "'");
          }
          fields.add(new Schema.Field(x.name(), common(x.schema(), y.schema(), fieldPath, visited)));
        }
        result.setFields(fields);
      } else if (left.getType() == Schema.Type.ARRAY && right.getType() == Schema.Type.ARRAY) {
        result = Schema.createArray(common(left.getElementType(), right.getElementType(), path + "[]", visited));
      } else if (left.getType() == Schema.Type.MAP && right.getType() == Schema.Type.MAP) {
        result = Schema.createMap(common(left.getValueType(), right.getValueType(), path + "{}", visited));
      } else if (leftType == rightType && leftType != Types.OTHER && leftType != Types.STRUCT
          && leftType != Types.ARRAY) {
        result = switch (leftType) {
          case Types.VARCHAR -> Schema.create(Schema.Type.STRING);
          case Types.BINARY -> {
            Schema binary = Schema.create(Schema.Type.BYTES);
            binary.addProp("jparq.binary", true);
            yield binary;
          }
          case Types.TIMESTAMP -> !AvroCoercions.isTimestamp(left) || !AvroCoercions.isTimestamp(right)
              || AvroCoercions.timestampUnits(left) >= AvroCoercions.timestampUnits(right) ? left : right;
          default -> left;
        };
      } else {
        throw mismatch(path, typeName(left), typeName(right));
      }
      visited.put(pair, result);
    }
    return JdbcTypeMapper.isNullable(first) || JdbcTypeMapper.isNullable(second)
        ? Schema.createUnion(Schema.create(Schema.Type.NULL), result)
        : result;
  }

  /**
   * Unwraps only a genuine optional union; heterogeneous unions retain their
   * declaration.
   *
   * @param schema
   *          field declaration
   * @return its value declaration
   */
  private static Schema optionalValue(Schema schema) {
    return schema.getType() == Schema.Type.UNION && schema.getTypes().size() == 2 && JdbcTypeMapper.isNullable(schema)
        ? JdbcTypeMapper.nonNullSchema(schema)
        : schema;
  }

  /**
   * Formats a compact first-field mismatch.
   *
   * @param path
   *          differing field path
   * @param left
   *          first SQL type
   * @param right
   *          second SQL type
   * @return exception carrying SQLState 42000
   */
  private static IllegalArgumentException mismatch(String path, String left, String right) {
    return SqlExceptions.incompatibleArrayElements("STRUCT field '" + path + "': " + left, right);
  }

  /**
   * Names a field by its SQL type rather than its physical Avro representation.
   *
   * @param schema
   *          field declaration
   * @return SQL type name
   */
  private static String typeName(Schema schema) {
    return schema.getType() == Schema.Type.MAP
        ? "MAP"
        : JDBCType.valueOf(JdbcTypeMapper.mapSchemaToJdbcType(schema)).getName();
  }

  /**
   * Converts raw Avro field values into the common declaration, retaining
   * unchanged records.
   *
   * @param value
   *          input field or record value
   * @param source
   *          original declaration
   * @param target
   *          common declaration
   * @return value encoded for the common declaration
   * @throws IllegalArgumentException
   *           if a numeric conversion fails
   */
  static Object convert(Object value, Schema source, Schema target) {
    if (value == null || source.equals(target)) {
      return value;
    }
    Schema from = optionalValue(source);
    Schema to = optionalValue(target);
    if (from.equals(to)) {
      return value;
    }
    if (value instanceof IndexedRecord record && to.getType() == Schema.Type.RECORD) {
      boolean unchanged = true;
      for (Schema.Field field : to.getFields()) {
        unchanged &= field.schema().equals(from.getFields().get(field.pos()).schema());
      }
      if (unchanged) {
        return value;
      }
      var result = new GenericData.Record(to);
      for (Schema.Field field : to.getFields()) {
        result.put(field.pos(),
            convert(record.get(field.pos()), from.getFields().get(field.pos()).schema(), field.schema()));
      }
      return result;
    }
    if (to.getType() == Schema.Type.ARRAY) {
      List<Object> result = new ArrayList<>();
      Iterable<?> elements = value instanceof Object[] array ? java.util.Arrays.asList(array) : (Iterable<?>) value;
      for (Object item : elements) {
        result.add(convert(item, from.getElementType(), to.getElementType()));
      }
      return result;
    }
    if (to.getType() == Schema.Type.MAP) {
      Map<Object, Object> result = new LinkedHashMap<>();
      for (var entry : ((Map<?, ?>) value).entrySet()) {
        result.put(entry.getKey(), convert(entry.getValue(), from.getValueType(), to.getValueType()));
      }
      return result;
    }
    Object decoded = AvroCoercions.unwrap(value, from);
    NumericType numeric = NumericType.of(JdbcTypeMapper.mapSchemaToJdbcType(to), to);
    if (numeric != null) {
      try {
        Object converted = numeric.convert(decoded);
        if (to.getLogicalType() instanceof LogicalTypes.Decimal decimal) {
          return new Conversions.DecimalConversion().toBytes((BigDecimal) converted, to, decimal);
        }
        return converted;
      } catch (SQLException exception) {
        throw new IllegalArgumentException(exception.getMessage(), exception);
      }
    }
    if (to.getLogicalType() instanceof LogicalTypes.Date && decoded instanceof java.sql.Date date) {
      return Math.toIntExact(date.toLocalDate().toEpochDay());
    }
    if (decoded instanceof java.sql.Time time) {
      long nanos = DateTimeFunctions.toLocalTime(time).toNanoOfDay();
      if (to.getLogicalType() instanceof LogicalTypes.TimeMillis) {
        return Math.toIntExact(nanos / 1_000_000L);
      }
      if (to.getLogicalType() instanceof LogicalTypes.TimeMicros) {
        return nanos / 1000L;
      }
    }
    if (AvroCoercions.isTimestamp(to)) {
      Timestamp timestamp = (Timestamp) decoded;
      var instant = AvroCoercions.isLocalTimestamp(to)
          ? timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC)
          : timestamp.toInstant();
      long units = AvroCoercions.timestampUnits(to);
      return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), units),
          instant.getNano() / (1_000_000_000L / units));
    }
    if (to.getType() == Schema.Type.BYTES && decoded instanceof byte[] bytes) {
      return ByteBuffer.wrap(bytes);
    }
    return decoded;
  }
}
