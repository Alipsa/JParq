package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SubqueryCorrelatedFiltersIsolator;
import se.alipsa.jparq.engine.SubqueryExecutor;

/**
 * Counts analysis work rather than relying on timing thresholds for IN
 * performance.
 */
class SubqueryCorrelationCachingTest {
  @Test
  void uncorrelatedInIsAnalyzedOncePerNodeRatherThanOncePerOuterRow() {
    Schema schema = new Schema.Parser().parse("""
        {"type":"record","name":"Input","fields":[{"name":"id","type":"int"}]}
        """);
    var executor = mock(SubqueryExecutor.class);
    when(executor.execute(any())).thenReturn(new SubqueryExecutor.SubqueryResult(List.of("id"), List.of(List.of(1))));
    var evaluator = new ExpressionEvaluator(schema, executor, List.of("big", "b"), Map.of(), Map.of());
    var where = SqlParser.parseSelect("SELECT id FROM big b WHERE id IN (SELECT id FROM small)").where();
    var analyses = new AtomicInteger();
    try (var isolator = mockStatic(SubqueryCorrelatedFiltersIsolator.class, CALLS_REAL_METHODS)) {
      isolator.when(() -> SubqueryCorrelatedFiltersIsolator.isolate(any(), any(), any(), any(), any()))
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
      verify(executor, never()).executeRaw(any());
    }
  }
}
