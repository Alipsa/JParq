package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.AvroCoercions;
import se.alipsa.jparq.engine.TemporalOperands;
import se.alipsa.jparq.engine.TemporalOperands.Operand;
import se.alipsa.jparq.helper.TemporalValueType;

/** Verifies the shared temporal operand promotion helpers on their own. */
class TemporalOperandsTest {

  /** Classifies physical and declared UTC-adjusted timestamps. */
  @Test
  void recognizesUtcTimestamps() {
    Schema utc = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema local = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    assertTrue(AvroCoercions.utcTimestamp(utc));
    assertFalse(AvroCoercions.utcTimestamp(local));
    assertTrue(AvroCoercions.utcTimestamp(TemporalValueType.UTC_TIMESTAMP.schema()));
    assertFalse(AvroCoercions.utcTimestamp(TemporalValueType.JDBC_LOCAL_TIMESTAMP.schema()));
    assertFalse(AvroCoercions.utcTimestamp(null));
  }

  /** Finds the innermost element type of nested ARRAY declarations. */
  @Test
  void recognizesTemporalArrayElements() {
    Schema utc = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema array = Schema.createArray(Schema.createArray(utc));
    assertTrue(TemporalOperands.temporalElements(array));
    assertTrue(TemporalOperands.temporalElements(Schema.createUnion(Schema.create(Schema.Type.NULL), utc)));
    assertFalse(TemporalOperands.temporalElements(Schema.createArray(Schema.create(Schema.Type.INT))));
    assertFalse(TemporalOperands.temporalElements(null));
  }

  /** Identifies ARRAY declarations whose leaves carry an explicit offset. */
  @Test
  void recognizesOffsetArraysAndTimestampResults() {
    Schema offset = TemporalValueType.OFFSET_TIMESTAMP.schema();
    assertTrue(TemporalOperands.offsetArray(Schema.createArray(offset)));
    assertFalse(TemporalOperands.offsetArray(Schema.createArray(Schema.create(Schema.Type.LONG))));
    assertTrue(TemporalOperands.timestampResult(offset));
    assertTrue(
        TemporalOperands.timestampResult(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
    assertFalse(TemporalOperands.timestampResult(Schema.create(Schema.Type.INT)));
  }

  /** Views Object[] and Iterable values as elements, rejecting other values. */
  @Test
  void viewsArrayLikeValuesAsElements() {
    Object[] array = {
        1, 2
    };
    assertEquals(List.of(1, 2), TemporalOperands.elementsOf(array));
    assertEquals(List.of(1, 2), TemporalOperands.elementsOf(List.of(1, 2)));
    assertNull(TemporalOperands.elementsOf(42));
    assertNull(TemporalOperands.elementsOf(null));
  }

  /** Keeps a JDBC Timestamp intact when the common type is a local timestamp. */
  @Test
  void promoteKeepsTimestampInstantForLocalTargets() {
    Timestamp timestamp = Timestamp.from(Instant.parse("2024-10-27T00:30:00Z"));
    Operand source = new Operand(timestamp,
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));
    for (TemporalValueType target : List.of(TemporalValueType.JDBC_LOCAL_TIMESTAMP,
        TemporalValueType.LOCAL_TIMESTAMP)) {
      Operand promoted = TemporalOperands.promote(source, target.schema());
      assertSame(timestamp, promoted.value(), target.name());
      assertEquals(target.schema(), promoted.declaration());
    }
  }

  /** Attaches the session displacement to local values for offset targets. */
  @Test
  void promoteConvertsUtcSourcesForOffsetTargets() {
    Timestamp timestamp = Timestamp.from(Instant.parse("2024-07-01T12:00:00Z"));
    Operand source = new Operand(timestamp,
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));
    Operand promoted = TemporalOperands.promote(source, TemporalValueType.OFFSET_TIMESTAMP.schema());
    assertEquals(Instant.parse("2024-07-01T12:00:00Z"),
        assertInstanceOf(OffsetDateTime.class, promoted.value()).toInstant());
  }

  /** Adopts the common TIME declaration so JSON serialization keeps its width. */
  @Test
  void promoteAdoptsCommonTimeDeclaration() {
    Schema time = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    time.addProp("jparq.temporal.precision", "6");
    java.sql.Time value = java.sql.Time.valueOf("01:02:03");
    Operand promoted = TemporalOperands.promote(new Operand(value, null), time);
    assertSame(value, promoted.value());
    assertEquals("6", promoted.declaration().getProp("jparq.temporal.precision"));
  }

  /** Leaves a LocalDateTime source to the ordinary decoding paths. */
  @Test
  void promoteLeavesLocalDateTimeSourcesUnchanged() {
    LocalDateTime local = LocalDateTime.parse("2024-10-27T02:30:00");
    Schema utc = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Operand source = new Operand(local, null);
    assertSame(source, TemporalOperands.promote(source, utc));
    assertSame(source, TemporalOperands.promote(source, null));
  }
}
