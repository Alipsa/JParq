package se.alipsa.jparq.engine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** Builds content keys without changing projected JDBC values. */
public final class RelationalKeys {

  private RelationalKeys() {
  }

  /**
   * Snapshots a row for DISTINCT, grouping, or set-operation membership.
   *
   * @param values
   *          row values, including SQL NULLs
   * @return keys in an unmodifiable list in projection order
   */
  public static List<Object> row(Collection<?> values) {
    List<Object> keys = new ArrayList<>(values.size());
    for (Object value : values) {
      keys.add(value(value));
    }
    return Collections.unmodifiableList(keys);
  }

  /**
   * Snapshots structured values by content, ignoring Avro record names.
   *
   * @param value
   *          a scalar, record, array, collection, or map
   * @return content key, or the original scalar
   */
  public static Object value(Object value) {
    if (value instanceof GenericRecord record) {
      Map<String, Object> fields = new LinkedHashMap<>();
      for (Schema.Field field : record.getSchema().getFields()) {
        fields.put(field.name(), value(record.get(field.pos())));
      }
      return Collections.unmodifiableMap(fields);
    }
    if (value instanceof Object[] array) {
      return row(java.util.Arrays.asList(array));
    }
    if (value instanceof Collection<?> collection) {
      return row(collection);
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> fields = new LinkedHashMap<>();
      map.forEach((key, item) -> fields.put(value(key), value(item)));
      return Collections.unmodifiableMap(fields);
    }
    if (value instanceof org.apache.avro.generic.GenericData.EnumSymbol symbol) {
      return symbol.toString();
    }
    if (value instanceof org.apache.avro.generic.GenericData.Fixed fixed) {
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
