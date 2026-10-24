package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.CorrelatedBindings;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.helper.LiteralConverter;

/** Checks binding ownership, typed values, and nested frame cleanup. */
class CorrelatedBindingsTest {
  /** Preserves values without SQL formatting, coercion, or loss of precision. */
  @Test
  void typedSlotsAndNullsArePreserved() {
    List<Object> values = Arrays.asList(null, 7, "an apostrophe ' and a question ?",
        new BigDecimal("9007199254740993.25"), Date.valueOf("2026-10-20"),
        Timestamp.valueOf("2026-10-20 12:34:56.123456789"), new byte[]{
            0, 1, 2
        });
    try (var bindings = new CorrelatedBindings("shape", values)) {
      for (int index = 0; index < values.size(); index++) {
        var expression = new CorrelatedBindings.Reference("shape", index).expression();
        assertSame(values.get(index), LiteralConverter.toLiteral(expression));
      }
    }
    assertThrows(IllegalArgumentException.class,
        () -> LiteralConverter.toLiteral(new CorrelatedBindings.Reference("shape", 0).expression()));
  }

  /** Source schemas survive null values and are restored with nested frames. */
  @Test
  void sourceSchemasFollowBindingLifetime() {
    var schema = SchemaBuilder.array().items().intType();
    var expression = new CorrelatedBindings.Reference("typed", 0).expression();
    try (var parent = new CorrelatedBindings("typed", Arrays.asList((Object) null), List.of(schema))) {
      assertSame(schema, CorrelatedBindings.schema(expression));
      assertNull(CorrelatedBindings.value(expression));
      try (var child = new CorrelatedBindings("typed", List.of(1))) {
        assertNull(CorrelatedBindings.schema(expression));
      }
      assertSame(schema, CorrelatedBindings.schema(expression));
    }
    assertThrows(IllegalArgumentException.class, () -> CorrelatedBindings.schema(expression));
  }

  /** Rejects malformed, inactive, and out-of-range private calls consistently. */
  @Test
  void invalidPlaceholderCallsHaveOneClearError() throws Exception {
    for (String sql : List.of("__JPARQ_CORRELATED_VALUE()", "__JPARQ_CORRELATED_VALUE(1, 2)",
        "__JPARQ_CORRELATED_VALUE('shape')", "__JPARQ_CORRELATED_VALUE('shape', 'slot')",
        "__JPARQ_CORRELATED_VALUE('shape', 0, 1)", "__JPARQ_CORRELATED_VALUE('missing', 0)")) {
      var expression = net.sf.jsqlparser.parser.CCJSqlParserUtil.parseExpression(sql);
      var failure = assertThrows(IllegalArgumentException.class, () -> LiteralConverter.toLiteral(expression));
      assertEquals("Invalid correlated binding: reserved for internal query execution", failure.getMessage());
    }
    try (var bindings = new CorrelatedBindings("shape", List.of(1))) {
      for (String index : List.of("-1", "1", "2147483648", "9223372036854775808")) {
        var expression = net.sf.jsqlparser.parser.CCJSqlParserUtil
            .parseExpression("__JPARQ_CORRELATED_VALUE('shape', " + index + ")");
        var failure = assertThrows(IllegalArgumentException.class, () -> LiteralConverter.toLiteral(expression));
        assertEquals("Invalid correlated binding: reserved for internal query execution", failure.getMessage());
      }
    }
  }

  /** Restores ancestors after deeper work fails and releases the last frame. */
  @Test
  void nestedFramesRestoreBindingsOnFailure() {
    var outer = new CorrelatedBindings.Reference("outer", 0).expression();
    var inner = new CorrelatedBindings.Reference("inner", 0).expression();
    try (var parent = new CorrelatedBindings("outer", List.of(1))) {
      assertThrows(IllegalArgumentException.class, () -> {
        try (var child = new CorrelatedBindings("inner", List.of(2));
            var shadow = new CorrelatedBindings("outer", List.of(3))) {
          assertEquals(3, LiteralConverter.toLiteral(outer));
          assertEquals(2, LiteralConverter.toLiteral(inner));
          throw new IllegalArgumentException("nested failure");
        }
      });
      assertEquals(1, LiteralConverter.toLiteral(outer));
      assertThrows(IllegalArgumentException.class, () -> LiteralConverter.toLiteral(inner));
    }
    assertThrows(IllegalArgumentException.class, () -> LiteralConverter.toLiteral(outer));
  }
  /** Limits bound predicate translation to the current execution scope. */
  @Test
  void predicateTranslationScopesDoNotLeak() {
    var schema = SchemaBuilder.record("row").fields().requiredInt("id").endRecord();
    var reference = new CorrelatedBindings.Reference("shape", 0).expression();
    var condition = new EqualsTo(new Column("id"), reference);
    for (int id : List.of(1, 2)) {
      try (var bindings = new CorrelatedBindings("shape", List.of(id))) {
        assertTrue(ParquetFilterBuilder.split(schema, condition).predicate().isEmpty());
        var split = ParquetFilterBuilder.withCorrelatedBindings(() -> ParquetFilterBuilder.split(schema, condition));
        assertEquals("eq(id, " + id + ")", split.predicate().orElseThrow().toString());
        assertThrows(IllegalArgumentException.class, () -> ParquetFilterBuilder.withCorrelatedBindings(() -> {
          throw new IllegalArgumentException("translation failed");
        }));
        assertTrue(ParquetFilterBuilder.split(schema, condition).predicate().isEmpty());
      }
    }
  }

}
