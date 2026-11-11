package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.engine.CorrelationMappings;
import se.alipsa.jparq.engine.InMemoryRecordReader;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.function.AggregateFunctions;

/**
 * Verifies execution reuse for selected and compared grouped scalar subqueries.
 */
class GroupedScalarReuseTest {
  /**
   * Shares one typed result per group across a selected scalar and a comparison,
   * while preserving different outer bindings for the next group.
   *
   * @throws Exception
   *           if planning, evaluation, or JDBC access fails
   */
  @Test
  void executesSelectedAndComparedScalarOncePerGroup() throws Exception {
    Schema timestamp = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema = SchemaBuilder.record("GroupedSource").fields().name("o").type(timestamp).noDefault().endRecord();
    List<GenericRecord> records = new ArrayList<>();
    for (int hour : List.of(10, 11)) {
      var record = new GenericData.Record(schema);
      record.put("o", Instant.parse("2024-07-01T" + hour + ":00:00Z").getEpochSecond() * 1_000_000L);
      records.add(record);
    }
    var reader = new InMemoryRecordReader(records, schema, "t");
    var mappings = CorrelationMappings.of(List.of("t"), reader.qualifierColumnMapping(),
        reader.unqualifiedColumnMapping());
    var select = SqlParser
        .parseSelect("SELECT o,(SELECT t.o)," + "CAST(o AS TIMESTAMP WITH TIME ZONE) = (SELECT t.o) FROM t GROUP BY o");
    var plan = AggregateFunctions.plan(select, schema, mappings);
    try (var connection = (JParqConnection) DriverManager.getConnection("jdbc:jparq:.");
        var executor = spy(new SubqueryExecutor(connection))) {
      var result = AggregateFunctions.evaluate(reader, plan, null, null, List.of(), executor,
          mappings.outerQualifiers(), mappings.qualifierColumnMapping(), mappings.unqualifiedColumnMapping());
      assertEquals(2, result.rows().size());
      for (int index = 0; index < 2; index++) {
        assertEquals(Instant.parse("2024-07-01T" + (10 + index) + ":00:00Z"),
            ((Timestamp) result.rows().get(index).get(1)).toInstant());
        assertEquals(Boolean.TRUE, result.rows().get(index).get(2));
      }
      verify(executor, times(2)).executeCorrelated(any(), any(), any(), any(), any());
    }
  }
}
