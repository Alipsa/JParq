package jparq.hf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.AvroProjections;
import se.alipsa.jparq.engine.ParquetRecordReaderAdapter;
import se.alipsa.jparq.engine.ParquetSchemas;

/** Checks that schema enrichment copies only records that need annotations. */
class ParquetRecordReaderAdapterTest {

  /**
   * Real PyArrow numeric and projected string records avoid schema-copy
   * allocations.
   */
  @Test
  void pyArrowRowsWithoutMissingAnnotationsAreNotCopied() throws Exception {
    for (String name : List.of("float_special", "optimized")) {
      Path path = new Path(getClass().getResource("/hf/" + name + ".parquet").toURI());
      Configuration configuration = new Configuration(false);
      Schema schema = ParquetSchemas.readAvroSchema(path, configuration);
      if ("optimized".equals(name)) {
        Schema projection = AvroProjections.project(schema, Set.of("id", "text"));
        AvroReadSupport.setRequestedProjection(configuration, projection);
        AvroReadSupport.setAvroReadSchema(configuration, projection);
      }
      try (var parquet = ParquetReader.<GenericRecord>builder(new AvroReadSupport<>(), path).withConf(configuration)
          .build()) {
        GenericRecord first = parquet.read();
        GenericRecord second = parquet.read();
        try (var reader = new ParquetRecordReaderAdapter(delegate(first, second), schema)) {
          assertSame(first, reader.read(), name);
          assertSame(second, reader.read(), name);
        }
      }
    }
  }

  /**
   * Ordinary projected rows retain their identity even if other fields need
   * enrichment.
   */
  @Test
  void ordinaryProjectedRowsAreNotCopied() throws Exception {
    Schema rawSchema = SchemaBuilder.record("raw").fields().requiredLong("id").endRecord();
    Schema fileSchema = Schema.createRecord("file", null, null, false);
    fileSchema.setFields(List.of(new Schema.Field("id", Schema.create(Schema.Type.LONG)),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG))));
    fileSchema.getField("ts").schema().addProp("logicalType", "timestamp-nanos");
    GenericRecord first = new GenericData.Record(rawSchema);
    GenericRecord second = new GenericData.Record(rawSchema);
    first.put("id", 1L);
    second.put("id", 2L);
    try (var reader = new ParquetRecordReaderAdapter(delegate(first, second), fileSchema)) {
      assertSame(first, reader.read());
      assertSame(second, reader.read());
      assertNull(reader.read());
    }
  }

  /** A projected timestamp receives its missing annotation on every row. */
  @Test
  void enrichedRowsRetainAnnotationsAndValues() throws Exception {
    Schema rawSchema = SchemaBuilder.record("raw").fields().requiredLong("ts").endRecord();
    Schema fileSchema = SchemaBuilder.record("file").fields().requiredLong("ts").endRecord();
    fileSchema.getField("ts").schema().addProp("logicalType", "timestamp-nanos");
    GenericRecord first = new GenericData.Record(rawSchema);
    GenericRecord second = new GenericData.Record(rawSchema);
    first.put("ts", 1L);
    second.put("ts", 2L);
    try (var reader = new ParquetRecordReaderAdapter(delegate(first, second), fileSchema)) {
      GenericRecord enriched = reader.read();
      assertNotSame(first, enriched);
      assertEquals("timestamp-nanos", enriched.getSchema().getField("ts").schema().getProp("logicalType"));
      assertEquals(1L, enriched.get("ts"));
      enriched = reader.read();
      assertNotSame(second, enriched);
      assertEquals(2L, enriched.get("ts"));
      assertEquals("timestamp-nanos", enriched.getSchema().getField("ts").schema().getProp("logicalType"));
      assertNull(reader.read());
    }
  }

  /**
   * Enriches records inside containers while retaining raw binary scalar values
   * for later decoding.
   *
   * @throws Exception
   *           if the reader cannot be stubbed or read
   */
  @Test
  void enrichedContainersRetainRawScalars() throws Exception {
    Schema child = SchemaBuilder.record("child").fields().requiredLong("ts").endRecord();
    Schema enrichedChild = new Schema.Parser().parse(child.toString());
    enrichedChild.getField("ts").schema().addProp("logicalType", "timestamp-nanos");
    Schema source = SchemaBuilder.record("source").fields().name("child").type(child).noDefault().name("array")
        .type(Schema.createArray(child)).noDefault().name("map").type(Schema.createMap(child)).noDefault()
        .requiredBytes("text").endRecord();
    final Schema target = SchemaBuilder.record("target").fields().name("child").type(enrichedChild).noDefault()
        .name("array").type(Schema.createArray(enrichedChild)).noDefault().name("map")
        .type(Schema.createMap(enrichedChild)).noDefault().requiredString("text").endRecord();
    var nested = new GenericData.Record(child);
    nested.put("ts", 3L);
    var raw = new GenericData.Record(source);
    raw.put("child", nested);
    raw.put("array", List.of(nested));
    raw.put("map", java.util.Map.of("k", nested));
    var bytes = java.nio.ByteBuffer.wrap("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    raw.put("text", bytes);
    try (var reader = new ParquetRecordReaderAdapter(delegate(raw, raw), target)) {
      for (int index = 0; index < 2; index++) {
        var enriched = reader.read();
        assertSame(bytes, enriched.get("text"));
        assertEquals("hello", se.alipsa.jparq.engine.AvroCoercions.unwrap(enriched.get("text"),
            enriched.getSchema().getField("text").schema()));
        for (Object value : List.of(enriched.get("child"), ((List<?>) enriched.get("array")).getFirst(),
            ((java.util.Map<?, ?>) enriched.get("map")).get("k"))) {
          var record = (GenericRecord) value;
          assertNotSame(nested, record);
          assertEquals("timestamp-nanos", record.getSchema().getField("ts").schema().getProp("logicalType"));
          assertEquals(3L, record.get("ts"));
        }
        assertNull(nested.getSchema().getField("ts").schema().getProp("logicalType"));
      }
    }
  }

  /**
   * Creates a reader returning two raw records followed by end of input.
   *
   * @param first
   *          the first record
   * @param second
   *          the second record
   * @return the stubbed Parquet reader
   * @throws Exception
   *           if the reader cannot be stubbed
   */
  @SuppressWarnings("unchecked")
  private ParquetReader<GenericRecord> delegate(GenericRecord first, GenericRecord second) throws Exception {
    ParquetReader<GenericRecord> reader = mock(ParquetReader.class);
    when(reader.read()).thenReturn(first, second, null);
    return reader;
  }
}
