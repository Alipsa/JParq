package jparq.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
   * Reuses immutable mappings so repeated qualified lookups do not copy them.
   *
   * @throws IOException
   *           if closing the reader fails
   */
  @Test
  void qualifierMappingsAreSharedAndDeeplyImmutable() throws IOException {
    Schema schema = Schema.createRecord("input", null, null, false);
    schema.setFields(List.of(new Schema.Field("a", Schema.create(Schema.Type.INT))));
    var table = new JoinRecordReader.JoinTable("input", "t", schema, List.of(), JoinType.BASE, null, List.of(), null);
    try (var reader = new JoinRecordReader(List.of(table))) {
      var mapping = reader.qualifierColumnMapping();
      assertSame(mapping, reader.qualifierColumnMapping());
      assertEquals("a", mapping.get("U:t").get("U:a"));
      assertThrows(UnsupportedOperationException.class, mapping::clear);
      assertThrows(UnsupportedOperationException.class, () -> mapping.get("U:t").clear());
    }
  }

  /**
   * Falls back to the table name when an input has a blank alias.
   *
   * @throws IOException
   *           if closing the reader fails
   */
  @Test
  void blankAliasesRetainTableQualifiers() throws IOException {
    Schema schema = Schema.createRecord("input", null, null, false);
    schema.setFields(List.of(new Schema.Field("a", Schema.create(Schema.Type.INT))));
    for (String alias : List.of("", " ", "\t")) {
      var table = new JoinRecordReader.JoinTable("input", alias, schema, List.of(), JoinType.BASE, null, List.of(),
          null);
      assertEquals("input", table.visibleName());
      try (var reader = new JoinRecordReader(List.of(table))) {
        assertEquals("a", reader.qualifierColumnMapping().get("U:input").get("U:a"));
      }
    }
  }

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
