package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.RelationalKeys;

/**
 * Checks immutable keys for nested structured values independently of schemas.
 */
class RelationalKeysTest {

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
