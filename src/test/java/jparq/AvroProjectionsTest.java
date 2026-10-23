package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.AvroProjections;

/**
 * Checks projection field-name extraction used by projection pushdown.
 */
class AvroProjectionsTest {

  /**
   * Field names follow the record declaration order and include only top-level
   * fields.
   */
  @Test
  void extractsFieldNamesInDeclarationOrder() {
    Schema nested = SchemaBuilder.record("nested").fields().requiredInt("inner").endRecord();
    Schema schema = SchemaBuilder.record("sample").fields().requiredInt("zeta").requiredString("alpha").name("nested")
        .type(nested).noDefault().endRecord();
    Set<String> names = AvroProjections.fieldNames(schema);
    assertEquals(List.of("zeta", "alpha", "nested"), List.copyOf(names));
  }

  /**
   * A record without fields yields an empty name set.
   */
  @Test
  void extractsNoNamesFromRecordWithoutFields() {
    Schema schema = SchemaBuilder.record("empty").fields().endRecord();
    assertEquals(Set.of(), AvroProjections.fieldNames(schema));
  }
}
