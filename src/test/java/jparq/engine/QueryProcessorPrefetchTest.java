package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.InMemoryRecordReader;
import se.alipsa.jparq.engine.QueryProcessor;
import se.alipsa.jparq.engine.SqlParser;

/** Verifies the public processor options for buffered, prefetched input. */
class QueryProcessorPrefetchTest {
  /**
   * A buffering processor owns emission counts, including the prefetched row.
   *
   * @throws Exception
   *           if processing fails
   */
  @Test
  void bufferingDoesNotCountPrefetchedInputAsEmitted() throws Exception {
    var schema = SchemaBuilder.record("rows").fields().requiredInt("id").endRecord();
    List<GenericRecord> records = List.of(new GenericData.Record(schema), new GenericData.Record(schema),
        new GenericData.Record(schema));
    for (int i = 0; i < records.size(); i++) {
      records.get(i).put("id", i + 1);
    }
    for (boolean staged : List.of(true, false)) {
      try (var reader = new InMemoryRecordReader(records)) {
        var options = QueryProcessor.Options.builder().schema(schema).initialEmitted(1).firstAlreadyRead(reader.read());
        if (staged) {
          options.preLimit(2);
        } else {
          options.orderBy(List.of(new SqlParser.OrderKey("id", true, null, null, null)));
        }
        try (var processor = new QueryProcessor(reader, List.of("id"), null, 2, options)) {
          assertEquals(1, processor.nextMatching().get("id"));
          assertEquals(2, processor.nextMatching().get("id"));
          assertNull(processor.nextMatching());
        }
      }
    }
  }
}
