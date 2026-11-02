package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.InMemoryRecordReader;
import se.alipsa.jparq.engine.RenamingRecordReader;

/** Verifies positional record renaming by physical source name. */
class RenamingRecordReaderTest {

  private static final Schema SOURCE = SchemaBuilder.record("t").fields().requiredInt("id").requiredString("name")
      .endRecord();
  private static final Schema RENAMED = SchemaBuilder.record("t").fields().requiredInt("a").requiredString("b")
      .endRecord();

  /**
   * Maps values by name when the record schema reorders the source fields.
   *
   * @throws Exception
   *           if reading fails
   */
  @Test
  void reorderedFieldsMapByName() throws Exception {
    Schema reordered = SchemaBuilder.record("t").fields().requiredString("name").requiredInt("id").endRecord();
    var record = new GenericData.Record(reordered);
    record.put("name", "Ada");
    record.put("id", 7);
    var reader = new RenamingRecordReader(new InMemoryRecordReader(List.of(record)), SOURCE, RENAMED);
    var renamed = reader.read();
    assertEquals(7, renamed.get("a"));
    assertEquals("Ada", renamed.get("b"));
    assertNull(reader.read());
  }

  /**
   * Leaves renamed fields null when the record schema is a narrowed projection
   * that drops unreferenced source fields.
   *
   * @throws Exception
   *           if reading fails
   */
  @Test
  void narrowedProjectionLeavesDroppedFieldsNull() throws Exception {
    Schema narrowed = SchemaBuilder.record("t").fields().requiredString("name").endRecord();
    var record = new GenericData.Record(narrowed);
    record.put("name", "Ada");
    var reader = new RenamingRecordReader(new InMemoryRecordReader(List.of(record)), SOURCE, RENAMED);
    var renamed = reader.read();
    assertNull(renamed.get("a"));
    assertEquals("Ada", renamed.get("b"));
  }

  /**
   * Rejects construction when the schemas are not positionally equivalent.
   */
  @Test
  void rejectsFieldCountMismatch() {
    Schema wider = SchemaBuilder.record("t").fields().requiredInt("a").requiredString("b").requiredString("c")
        .endRecord();
    var record = new GenericData.Record(SOURCE);
    var delegate = new InMemoryRecordReader(List.of(record));
    assertThrows(IllegalArgumentException.class, () -> new RenamingRecordReader(delegate, SOURCE, wider));
  }

  /**
   * Exposes the renamed schema and passes exhaustion and close through to the
   * delegate.
   *
   * @throws Exception
   *           if closing fails
   */
  @Test
  void exposesRenamedSchemaAndExhausts() throws Exception {
    var record = new GenericData.Record(SOURCE);
    record.put("id", 1);
    record.put("name", "Ada");
    var reader = new RenamingRecordReader(new InMemoryRecordReader(List.of(record)), SOURCE, RENAMED);
    assertSame(RENAMED, reader.schema());
    assertTrue(reader.read() != null);
    assertNull(reader.read());
    reader.close();
  }
}
