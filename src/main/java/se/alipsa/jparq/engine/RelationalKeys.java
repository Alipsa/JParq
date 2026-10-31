package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.helper.TemporalInterval;

/** Builds content keys without changing projected JDBC values. */
public final class RelationalKeys {

  private RelationalKeys() {
  }

  /**
   * Normalizes a row for DISTINCT, grouping, set operations, or window
   * partitions. Rows whose values need no conversion reuse the supplied list;
   * callers must not modify that list while its key is in use. A converted value
   * triggers a copy of the row.
   *
   * @param values
   *          row values, including SQL NULLs
   * @return the supplied list if unchanged, otherwise an unmodifiable key list
   */
  public static List<Object> row(List<Object> values) {
    return row(values, true);
  }

  /**
   * Builds a content key, optionally normalizing scalar whole-number types.
   * Homogeneously typed scalar columns can retain their original objects and
   * list. Nested content is always normalized because its leaves may mix types.
   *
   * @param values
   *          row values, including SQL NULLs
   * @param normalizeWholeNumbers
   *          whether scalar whole-number types may differ
   * @return the supplied list if unchanged, otherwise an unmodifiable key list
   */
  public static List<Object> row(List<Object> values, boolean normalizeWholeNumbers) {
    return row(values, normalizeWholeNumbers, null);
  }

  /**
   * Builds a content key using the scalar normalization policy for each column.
   * Known scalar types can retain their original objects while unknown columns
   * normalize whole-number representations. Nested content is always normalized.
   *
   * @param values
   *          row values, including SQL NULLs
   * @param normalizeColumns
   *          normalization flags in column order, matching the row width
   * @return the supplied list if unchanged, otherwise an unmodifiable key list
   */
  public static List<Object> row(List<Object> values, boolean[] normalizeColumns) {
    return row(values, false, normalizeColumns);
  }

  /**
   * Shares copy-on-change row construction across uniform and per-column
   * policies.
   *
   * @param values
   *          row values
   * @param normalizeWholeNumbers
   *          uniform scalar policy when column flags are absent
   * @param normalizeColumns
   *          per-column flags, or null for the uniform policy
   * @return the original list or an unmodifiable normalized copy
   */
  private static List<Object> row(List<Object> values, boolean normalizeWholeNumbers, boolean[] normalizeColumns) {
    List<Object> keys = null;
    int index = 0;
    for (Object original : values) {
      Object key = value(original, normalizeColumns == null ? normalizeWholeNumbers : normalizeColumns[index]);
      if (keys == null && key != original) {
        keys = new ArrayList<>(values);
      }
      if (keys != null) {
        keys.set(index, key);
      }
      index++;
    }
    return keys == null ? values : Collections.unmodifiableList(keys);
  }

  /**
   * Copies nested containers even when every element is already scalar.
   *
   * @param values
   *          nested values to snapshot
   * @return an unmodifiable list of content keys
   */
  private static List<Object> snapshot(Collection<?> values) {
    List<Object> keys = new ArrayList<>(values.size());
    for (Object value : values) {
      keys.add(value(value));
    }
    return Collections.unmodifiableList(keys);
  }

  /**
   * Snapshots structured values by content, ignoring Avro record names, and
   * normalizes whole-number representations and decimal scales for equality.
   *
   * @param value
   *          a scalar, record, array, collection, or map
   * @return content key, or the original scalar
   */
  public static Object value(Object value) {
    return value(value, true);
  }

  /**
   * Builds a value key without boxing already compatible scalar integers again.
   * Decimal keys always ignore scale. Structured values still snapshot and
   * normalize their nested contents.
   *
   * @param value
   *          the scalar or structured value
   * @param normalizeWholeNumbers
   *          whether scalar whole-number types may differ
   * @return the original scalar when compatible, or a normalized content key
   */
  public static Object value(Object value, boolean normalizeWholeNumbers) {
    if (value instanceof TemporalInterval interval) {
      return List.of(interval.period().toTotalMonths(), interval.duration().plusDays(interval.period().getDays()));
    }
    if (value instanceof BigDecimal decimal) {
      // An odd unscaled value cannot end in a zero at any scale.
      if (decimal.unscaledValue().testBit(0)) {
        return decimal;
      }
      BigDecimal normalized = decimal.stripTrailingZeros();
      return decimal.equals(normalized) ? decimal : normalized;
    }
    if (value instanceof Number number) {
      if (normalizeWholeNumbers && (value instanceof Byte || value instanceof Short || value instanceof Integer)) {
        return number.longValue();
      }
      return value;
    }
    if (value instanceof GenericRecord record) {
      Map<String, Object> fields = new LinkedHashMap<>();
      for (Schema.Field field : record.getSchema().getFields()) {
        fields.put(field.name(), value(record.get(field.pos())));
      }
      return Collections.unmodifiableMap(fields);
    }
    if (value instanceof Object[] array) {
      return snapshot(Arrays.asList(array));
    }
    if (value instanceof Collection<?> collection) {
      return snapshot(collection);
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> fields = new LinkedHashMap<>();
      map.forEach((key, item) -> fields.put(value(key), value(item)));
      return Collections.unmodifiableMap(fields);
    }
    if (value instanceof EnumSymbol symbol) {
      return symbol.toString();
    }
    if (value instanceof Fixed fixed) {
      return value(fixed.bytes());
    }
    if (value instanceof byte[] bytes) {
      return ByteBuffer.wrap(bytes.clone()).asReadOnlyBuffer();
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer copy = buffer.duplicate();
      byte[] bytes = new byte[copy.remaining()];
      copy.get(bytes);
      return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }
    return value instanceof CharSequence text ? text.toString() : value;
  }
}
