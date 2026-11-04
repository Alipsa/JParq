package se.alipsa.jparq.engine.function;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import se.alipsa.jparq.NumericType;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalValueType;

/**
 * Resolves SQL field types and converts STRUCT array elements without mutating
 * input records.
 */
public final class StructArrayTypes {
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
  public static Schema common(Schema left, Schema right) {
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
        throw mismatch(path, typeName(left), typeName(right));
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
          throw mismatch(path, structName(left), structName(right));
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
          case Types.TIMESTAMP -> timestampSchema(left, right);
          case Types.TIME -> right.getLogicalType() instanceof LogicalTypes.TimeMicros ? right : left;
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
   * Retains full datetime range and precision when mixing nanosecond and coarser
   * storage. UTC-adjusted fields use UTC datetime text; local fields retain their
   * wall-clock fields.
   *
   * @param left
   *          first timestamp declaration
   * @param right
   *          second timestamp declaration
   * @return lossless internal declaration, or the finer compatible physical unit
   */
  private static Schema timestampSchema(Schema left, Schema right) {
    if (TemporalValueType.fromSchema(left) == TemporalValueType.LOCAL_TIMESTAMP
        || TemporalValueType.fromSchema(right) == TemporalValueType.LOCAL_TIMESTAMP) {
      return TemporalValueType.LOCAL_TIMESTAMP.schema();
    }
    if (!AvroCoercions.isTimestamp(left) || !AvroCoercions.isTimestamp(right)) {
      return left;
    }
    long first = AvroCoercions.timestampUnits(left);
    long second = AvroCoercions.timestampUnits(right);
    if (first != second && Math.max(first, second) == 1_000_000_000L) {
      return TemporalValueType.LOCAL_TIMESTAMP.schema();
    }
    return first >= second ? left : right;
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
    return SqlExceptions.incompatibleArrayElements(path.isEmpty() ? left : "STRUCT field '" + path + "': " + left,
        right);
  }

  /**
   * Names a field by its SQL type rather than its physical Avro representation.
   *
   * @param schema
   *          field declaration
   * @return SQL type name
   */
  private static String typeName(Schema schema) {
    if (schema.getType() == Schema.Type.NULL) {
      return "NULL";
    }
    if (schema.getType() == Schema.Type.UNION) {
      return "UNION(" + String.join(", ", schema.getTypes().stream().map(StructArrayTypes::typeName).toList()) + ")";
    }
    return schema.getType() == Schema.Type.MAP
        ? "MAP"
        : JDBCType.valueOf(JdbcTypeMapper.mapSchemaToJdbcType(schema)).getName();
  }

  /**
   * Describes a STRUCT degree with singular or plural field wording.
   *
   * @param schema
   *          STRUCT declaration
   * @return compact degree description
   */
  private static String structName(Schema schema) {
    int count = schema.getFields().size();
    return "STRUCT with " + count + (count == 1 ? " field" : " fields");
  }

  /**
   * Compiles conversions once for each source and common schema pair.
   *
   * @param source
   *          original declaration
   * @param target
   *          common declaration
   * @return conversion retaining unchanged values
   */
  static Function<Object, Object> converter(Schema source, Schema target) {
    return converter(source, target, new IdentityHashMap<>());
  }

  /**
   * Compiles recursive containers without comparing schemas during row
   * conversion.
   *
   * @param source
   *          original declaration
   * @param target
   *          common declaration
   * @param compiled
   *          conversion plans indexed by source and target identity
   * @return reusable conversion
   */
  private static Function<Object, Object> converter(Schema source, Schema target,
      Map<Schema, Map<Schema, Function<Object, Object>>> compiled) {
    var targets = compiled.computeIfAbsent(source, ignored -> new IdentityHashMap<>());
    Function<Object, Object> cached = targets.get(target);
    if (cached != null) {
      return cached;
    }
    Schema from = optionalValue(source);
    Schema to = optionalValue(target);
    if (from.equals(to)) {
      return Function.identity();
    }
    targets.put(target, value -> targets.get(target).apply(value));
    Function<Object, Object> conversion;
    if (to.getType() == Schema.Type.RECORD) {
      boolean unchanged = true;
      List<Function<Object, Object>> fields = new ArrayList<>();
      for (Schema.Field field : to.getFields()) {
        Schema original = from.getFields().get(field.pos()).schema();
        unchanged &= field.schema().equals(original);
        fields.add(converter(original, field.schema(), compiled));
      }
      conversion = unchanged ? Function.identity() : value -> {
        var record = (IndexedRecord) value;
        var result = new GenericData.Record(to);
        for (int index = 0; index < fields.size(); index++) {
          result.put(index, fields.get(index).apply(record.get(index)));
        }
        return result;
      };
    } else if (to.getType() == Schema.Type.ARRAY) {
      var element = converter(from.getElementType(), to.getElementType(), compiled);
      conversion = value -> {
        List<Object> result = new ArrayList<>();
        Iterable<?> elements = value instanceof Object[] array ? java.util.Arrays.asList(array) : (Iterable<?>) value;
        for (Object item : elements) {
          result.add(element.apply(item));
        }
        return result;
      };
    } else if (to.getType() == Schema.Type.MAP) {
      var element = converter(from.getValueType(), to.getValueType(), compiled);
      conversion = value -> {
        Map<Object, Object> result = new LinkedHashMap<>();
        for (var entry : ((Map<?, ?>) value).entrySet()) {
          result.put(entry.getKey(), element.apply(entry.getValue()));
        }
        return result;
      };
    } else {
      conversion = value -> convertScalar(value, from, to);
    }
    Function<Object, Object> nullable = value -> value == null ? null : conversion.apply(value);
    targets.put(target, nullable);
    return nullable;
  }

  /**
   * Converts a scalar Avro field into the common declaration.
   *
   * @param value
   *          input field or record value
   * @param from
   *          original declaration
   * @param to
   *          common declaration
   * @return value encoded for the common declaration
   * @throws IllegalArgumentException
   *           if a numeric or datetime conversion fails, carrying SQLState 22003
   *           or 22008 respectively
   */
  private static Object convertScalar(Object value, Schema from, Schema to) {
    if (value instanceof Number number
        && (from.getLogicalType() instanceof LogicalTypes.TimeMillis
            || from.getLogicalType() instanceof LogicalTypes.TimeMicros)
        && (to.getLogicalType() instanceof LogicalTypes.TimeMillis
            || to.getLogicalType() instanceof LogicalTypes.TimeMicros)) {
      // Validate the stored clock range before scaling; keep the raw value to retain
      // microseconds.
      AvroCoercions.unwrap(value, from);
      long sourceUnits = from.getLogicalType() instanceof LogicalTypes.TimeMicros ? 1_000_000L : 1000L;
      long targetUnits = to.getLogicalType() instanceof LogicalTypes.TimeMicros ? 1_000_000L : 1000L;
      long converted = number.longValue() * targetUnits / sourceUnits;
      if (to.getType() == Schema.Type.INT) {
        return Math.toIntExact(converted);
      }
      return converted;
    }
    Object decoded = AvroCoercions.unwrap(value, from);
    TemporalValueType temporal = TemporalValueType.fromSchema(to);
    if (temporal != null) {
      if (temporal == TemporalValueType.LOCAL_TIMESTAMP && decoded instanceof Timestamp timestamp) {
        return (AvroCoercions.isLocalTimestamp(from)
            ? timestamp.toLocalDateTime()
            : LocalDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.UTC)).toString();
      }
      return temporal.normalize(decoded).toString();
    }
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
      try {
        long seconds = instant.getEpochSecond();
        long fraction = instant.getNano() / (1_000_000_000L / units);
        if (seconds < 0 && fraction > 0) {
          return Math.addExact(Math.multiplyExact(seconds + 1, units), fraction - units);
        }
        return Math.addExact(Math.multiplyExact(seconds, units), fraction);
      } catch (ArithmeticException exception) {
        String message = "Timestamp cannot be represented by the common type: "
            + (AvroCoercions.isLocalTimestamp(from) ? timestamp.toLocalDateTime() : timestamp.toInstant());
        throw new IllegalArgumentException(message, new SQLDataException(message, "22008", exception));
      }
    }
    if (to.getType() == Schema.Type.BYTES && decoded instanceof byte[] bytes) {
      return ByteBuffer.wrap(bytes);
    }
    return decoded;
  }
}
