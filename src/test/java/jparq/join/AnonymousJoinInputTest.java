package jparq.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.JoinRecordReader;
import se.alipsa.jparq.engine.SqlParser.JoinType;

/**
 * Verifies that anonymous join inputs expose columns without SQL qualifiers.
 */
class AnonymousJoinInputTest {

  /**
   * Keeps storage prefixes out of qualifier mappings even with duplicate labels.
   *
   * @throws IOException
   *           if closing the reader fails
   */
  @Test
  void anonymousInputsHaveNoQualifierMappings() throws IOException {
    Schema schema = Schema.createRecord("derived", null, null, false);
    schema.setFields(List.of(new Schema.Field("a", Schema.create(Schema.Type.INT))));
    var first = new JoinRecordReader.JoinTable(null, null, schema, List.of(), JoinType.BASE, null, List.of(), null);
    var second = new JoinRecordReader.JoinTable(null, null, schema, List.of(), JoinType.CROSS, null, List.of(), null);
    try (var reader = new JoinRecordReader(List.of(first, second))) {
      assertTrue(reader.qualifierColumnMapping().isEmpty());
      assertEquals(2, reader.columnNames().stream().distinct().count());
      assertEquals(List.of("a", "a"), reader.columnNames().stream().map(reader.canonicalLabels()::get).toList());
    }
  }
}
