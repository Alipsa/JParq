package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqPreparedStatement;
import se.alipsa.jparq.engine.CorrelatedBindings;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.IntervalExpressionTypes;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SubqueryCorrelatedFiltersIsolator;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.helper.TemporalValueType;

/**
 * Counts analysis work rather than relying on timing thresholds for IN
 * performance.
 */
class SubqueryCorrelationCachingTest {
  /**
   * Keeps typed numeric bindings distinct from datetime projection declarations.
   */
  @Test
  void correlatedMetadataResolvesOnlyTemporalDeclarations() {
    for (Schema schema : List.of(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.STRING), Schema.createArray(Schema.create(Schema.Type.INT)))) {
      try (var bindings = new CorrelatedBindings("numeric-projection", Collections.singletonList(null),
          List.of(schema))) {
        var reference = new CorrelatedBindings.Reference("numeric-projection", 0).expression();
        assertNull(IntervalExpressionTypes.resolveTemporal(reference, column -> null, select -> null));
      }
    }
    for (Schema schema : List.of(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
        LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)),
        TemporalValueType.OFFSET_TIMESTAMP.schema(), TemporalValueType.OFFSET_TIME.schema())) {
      try (var bindings = new CorrelatedBindings("temporal-projection", Collections.singletonList(null),
          List.of(schema))) {
        var reference = new CorrelatedBindings.Reference("temporal-projection", 0).expression();
        assertSame(schema, IntervalExpressionTypes.resolveTemporal(reference, column -> null, select -> null));
      }
    }
  }

  /**
   * Avoids rendering and preparing projection SQL once its Select node is cached.
   *
   * @throws Exception
   *           if metadata lookup fails
   */
  @Test
  void projectionMetadataIsResolvedOncePerSelectNode() throws Exception {
    var renders = new AtomicInteger();
    var prepares = new AtomicInteger();
    Select select = mock(Select.class);
    when(select.toString()).thenAnswer(invocation -> {
      renders.incrementAndGet();
      return "SELECT CURRENT_TIMESTAMP";
    });
    var prepared = mock(JParqPreparedStatement.class);
    Schema expected = TemporalValueType.OFFSET_TIMESTAMP.schema();
    when(prepared.declaredProjectionSchema()).thenReturn(expected);
    try (var executor = new SubqueryExecutor(mock(JParqConnection.class), sql -> {
      prepares.incrementAndGet();
      return prepared;
    })) {
      for (int row = 0; row < 1000; row++) {
        assertSame(expected, executor.projectionSchema(select));
      }
      assertEquals(1, renders.get());
      assertEquals(1, prepares.get());
    }
  }

  @Test
  void uncorrelatedInIsAnalyzedOncePerNodeRatherThanOncePerOuterRow() {
    Schema schema = new Schema.Parser().parse("""
        {"type":"record","name":"Input","fields":[{"name":"id","type":"int"}]}
        """);
    var schemaLookups = new AtomicInteger();
    var executor = spy(new SubqueryExecutor(mock(JParqConnection.class), null, table -> {
      schemaLookups.incrementAndGet();
      return java.util.Set.of("id");
    }));
    doReturn(new SubqueryExecutor.SubqueryResult(List.of("id"), List.of(List.of(1)))).when(executor).execute(any());
    var evaluator = new ExpressionEvaluator(schema, executor, List.of("big", "b"), Map.of(), Map.of());
    var where = SqlParser.parseSelect("SELECT id FROM big b WHERE id IN (SELECT id FROM small)").where();
    var analyses = new AtomicInteger();
    try (var isolator = mockStatic(SubqueryCorrelatedFiltersIsolator.class, CALLS_REAL_METHODS)) {
      isolator.when(() -> SubqueryCorrelatedFiltersIsolator.isolate(any(), any(), any(), any(), any(), any()))
          .thenAnswer(invocation -> {
            analyses.incrementAndGet();
            return invocation.callRealMethod();
          });
      for (int id = 0; id < 1000; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        assertEquals(id == 1, evaluator.eval(where, row));
      }
      assertEquals(1, analyses.get());
      assertEquals(1, schemaLookups.get());
      verify(executor, never()).executeRaw(any());
    }
  }
}
