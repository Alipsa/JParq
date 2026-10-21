package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.RelationalKeys;

/**
 * Checks scalar-row reuse and snapshots for nested structured values.
 */
class RelationalKeysTest {

  /** Scalar rows reuse their input list, including NULL and decimal values. */
  @Test
  void reusesScalarRows() {
    List<Object> values = Arrays.asList("text", 1, 2L, new BigDecimal("1.50"), true, null);
    assertSame(values, RelationalKeys.row(values));
    List<Object> empty = List.of();
    assertSame(empty, RelationalKeys.row(empty));
    List<Object> immutable = List.of("text", 1);
    assertSame(immutable, RelationalKeys.row(immutable));
  }

  /** Converted text and nested scalar lists remain independent of mutations. */
  @Test
  void snapshotsRowsRequiringNormalization() {
    Utf8 text = new Utf8("same");
    List<Object> nested = new ArrayList<>(Arrays.asList(1, "two", null));
    List<Object> values = new ArrayList<>(Arrays.asList(7, "prefix", text, nested));
    List<Object> key = RelationalKeys.row(values);
    assertNotSame(values, key);
    values.set(1, "changed");
    text.set("changed");
    nested.set(0, 9);
    assertEquals(Arrays.asList(7, "prefix", "same", Arrays.asList(1, "two", null)), key);
    assertThrows(UnsupportedOperationException.class, () -> key.add("extra"));
  }

  /** Record names and array containers do not affect equal field contents. */
  @Test
  void snapshotsNestedRecordsAndArrays() {
    var firstSchema = SchemaBuilder.record("First").fields().requiredString("s").name("a").type().array().items()
        .nullable().intType().noDefault().endRecord();
    var secondSchema = SchemaBuilder.record("Second").fields().requiredString("s").name("a").type().array().items()
        .nullable().intType().noDefault().endRecord();
    var first = new GenericData.Record(firstSchema);
    first.put("s", new Utf8("same"));
    first.put("a", Arrays.asList(1, null, 2));
    var second = new GenericData.Record(secondSchema);
    second.put("s", "same");
    second.put("a", new Object[]{
        1, null, 2
    });
    Object key = RelationalKeys.value(first);
    Object otherKey = RelationalKeys.value(second);
    assertEquals(key, otherKey);
    assertEquals(key.hashCode(), otherKey.hashCode());
    assertEquals(RelationalKeys.row(Arrays.asList(first, null)), RelationalKeys.row(Arrays.asList(second, null)));
    second.put("s", "changed");
    assertEquals(key, otherKey);
    assertNotEquals(key, RelationalKeys.value(second));
    assertEquals(RelationalKeys.value(new Object[]{
        new Object[]{
            first
        }
    }), RelationalKeys.value(List.of(List.of(first))));
  }

  /** Binary keys copy bytes and leave source buffer positions intact. */
  @Test
  void snapshotsBinaryValues() {
    byte[] bytes = {
        1, 2
    };
    ByteBuffer buffer = ByteBuffer.wrap(new byte[]{
        0, 1, 2
    });
    buffer.position(1);
    Object key = RelationalKeys.value(bytes);
    assertEquals(key, RelationalKeys.value(buffer));
    assertEquals(1, buffer.position());
    bytes[0] = 9;
    buffer.put(1, (byte) 8);
    assertEquals(key, RelationalKeys.value(new byte[]{
        1, 2
    }));
    assertNotEquals(key, RelationalKeys.value(bytes));
  }
}
