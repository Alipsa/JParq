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
