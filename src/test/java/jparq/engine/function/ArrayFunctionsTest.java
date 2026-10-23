package jparq.engine.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import se.alipsa.jparq.engine.JSqlParserSupport;
import se.alipsa.jparq.engine.SubqueryExecutor.SubqueryResult;
import se.alipsa.jparq.engine.function.ArrayFunctions;

/**
 * Unit tests for {@link ArrayFunctions}.
 */
class ArrayFunctionsTest {

  /**
   * Verifies shared ARRAY subquery shape checks, normalization, and diagnostics.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void normalizesSubqueryResults() throws Exception {
    Select subSelect = (Select) JSqlParserSupport.parseStatement("SELECT 1, 2");
    assertEquals(List.of(), ArrayFunctions.fromSubquery(new SubqueryResult(List.of(), List.of(List.of(1))), subSelect));
    assertEquals(List.of(), ArrayFunctions.fromSubquery(new SubqueryResult(List.of("n"), List.of()), subSelect));
    assertEquals(List.of(1L, 2L),
        ArrayFunctions.fromSubquery(new SubqueryResult(List.of("n"), List.of(List.of(1), List.of(2L))), subSelect));
    var exception = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.fromSubquery(new SubqueryResult(List.of("n", "m"), List.of()), subSelect));
    assertEquals("ARRAY subquery must return exactly one column: " + subSelect, exception.getMessage());
  }

  /**
   * Verify null or empty inputs return an empty, unmodifiable list.
   */
  @Test
  void returnsEmptyUnmodifiableListWhenInputIsNullOrEmpty() {
    List<Object> fromNull = ArrayFunctions.homogenizeArrayValues(null);
    List<Object> fromEmpty = ArrayFunctions.homogenizeArrayValues(List.of());
    assertTrue(fromNull.isEmpty());
    assertTrue(fromEmpty.isEmpty());
    assertThrows(UnsupportedOperationException.class, () -> fromNull.add("x"));
    assertThrows(UnsupportedOperationException.class, () -> fromEmpty.add("y"));
  }

  /**
   * Ensure numeric promotion targets the widest numeric type (BigDecimal) when
   * present.
   */
  @Test
  void promotesNumericValuesToBigDecimalWhenNeeded() {
    List<Object> values = Arrays.asList(1d, new BigDecimal("2.50"), new BigInteger("3"));
    List<Object> homogenized = ArrayFunctions.homogenizeArrayValues(values);
    assertIterableEquals(List.of(BigDecimal.valueOf(1.0d), new BigDecimal("2.50"), new BigDecimal("3")), homogenized);
    homogenized.forEach(item -> assertTrue(item == null || item instanceof BigDecimal));
    assertThrows(UnsupportedOperationException.class, () -> homogenized.add(BigDecimal.ONE));
  }

  /**
   * Confirm promotion to float happens for mixed lower-order numeric values.
   */
  @Test
  void promotesNumericValuesToFloatForMixedLowOrderTypes() {
    List<Object> values = Arrays.asList((byte) 1, 2.0f, 3L);
    List<Object> homogenized = ArrayFunctions.homogenizeArrayValues(values);
    assertIterableEquals(List.of(1.0f, 2.0f, 3.0f), homogenized);
    homogenized.forEach(item -> assertTrue(item == null || item instanceof Float));
  }

  /**
   * Validate that unknown {@link Number} implementations default to double
   * promotion.
   */
  @Test
  void defaultsToDoublePromotionForCustomNumbers() {
    Number custom = new Number() {
      @Override
      public int intValue() {
        return 3;
      }

      @Override
      public long longValue() {
        return 3L;
      }

      @Override
      public float floatValue() {
        return 3.25f;
      }

      @Override
      public double doubleValue() {
        return 3.25d;
      }
    };
    List<Object> homogenized = ArrayFunctions.homogenizeArrayValues(Arrays.asList(custom, 2L));
    assertIterableEquals(List.of(3.25d, 2.0d), homogenized);
    homogenized.forEach(item -> assertTrue(item == null || item instanceof Double));
  }

  /**
   * Ensure character sequences are coerced to plain strings.
   */
  @Test
  void coercesCharacterSequencesToStrings() {
    List<Object> homogenized = ArrayFunctions
        .homogenizeArrayValues(Arrays.asList(new StringBuilder("alpha"), "beta", null));
    assertIterableEquals(Arrays.asList("alpha", "beta", null), homogenized);
    homogenized.forEach(item -> assertTrue(item == null || item instanceof String));
  }

  /**
   * Verify that homogeneous lists preserve their values and remain unmodifiable.
   */
  @Test
  void preservesHomogeneousLists() {
    List<Object> listA = List.of(1);
    List<Object> listB = List.of(2, 3);
    List<Object> homogenized = ArrayFunctions.homogenizeArrayValues(Arrays.asList(listA, listB));
    assertEquals(List.of(listA, listB), homogenized);
    assertThrows(UnsupportedOperationException.class, () -> homogenized.add(List.of(4)));
  }

  /**
   * Rejects incompatible element families before conversion, in both orders and
   * with NULL elements interspersed.
   *
   * @param first
   *          first incompatible element
   * @param second
   *          second incompatible element
   */
  @ParameterizedTest
  @MethodSource("incompatibleElements")
  void rejectsIncompatibleElements(Object first, Object second) {
    var values = new ArrayList<Object>(Arrays.asList(null, first, null, second, null));
    for (int order = 0; order < 2; order++) {
      var failure = assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.homogenizeArrayValues(values));
      assertTrue(failure.getMessage().contains("ARRAY elements must share a common type"), failure.getMessage());
      var sql = assertInstanceOf(SQLSyntaxErrorException.class, failure.getCause());
      assertEquals("42000", sql.getSQLState());
      Collections.reverse(values);
    }
  }

  /**
   * Supplies numeric and nonnumeric mismatches that must fail before conversion.
   *
   * @return incompatible non-NULL element pairs
   */
  private static Stream<Arguments> incompatibleElements() {
    return Stream.of(Arguments.of(4L, List.of(6L)), Arguments.of(BigDecimal.ONE, List.of()), Arguments.of(4L, "6"),
        Arguments.of(4L, true), Arguments.of(4L, new Object[]{
            6L
        }), Arguments.of(List.of(4L), "6"), Arguments.of(List.of(4L), List.of("6")),
        Arguments.of(List.of(4L), List.of(List.of(6L))), Arguments.of(List.of(List.of(4L)), List.of(List.of(true))),
        Arguments.of(4L, java.sql.Date.valueOf("2026-01-01")));
  }

  /** Preserves NULL values while promoting compatible numeric elements. */
  @Test
  void preservesNullsDuringNumericPromotion() {
    assertEquals(Arrays.asList(null, new BigDecimal("4"), new BigDecimal("6.5"), null),
        ArrayFunctions.homogenizeArrayValues(Arrays.asList(null, 4L, new BigDecimal("6.5"), null)));
  }

  /**
   * Promotes numeric leaves across sibling arrays while preserving NULLs and
   * empty arrays.
   */
  @Test
  void promotesNestedNumericValues() {
    var values = Arrays.<Object>asList(null, Arrays.asList(null, 1L), List.of(), List.of(new BigDecimal("2.5")));
    var normalized = ArrayFunctions.homogenizeArrayValues(values);
    assertEquals(
        Arrays.asList(null, Arrays.asList(null, new BigDecimal("1")), List.of(), List.of(new BigDecimal("2.5"))),
        normalized);
    assertThrows(UnsupportedOperationException.class, () -> ((List<?>) normalized.get(1)).clear());
    assertEquals(List.of(List.of(List.of(new BigDecimal("1"))), List.of(List.of(new BigDecimal("2.5")))),
        ArrayFunctions.homogenizeArrayValues(List.of(List.of(List.of(1L)), List.of(List.of(new BigDecimal("2.5"))))));
  }

  /**
   * Normalizes character sequences and preserves all-NULL leaves inside nested
   * arrays.
   */
  @Test
  void normalizesNestedStringsAndNulls() {
    assertEquals(Arrays.asList(Arrays.asList(null, null), List.of("a", "b")),
        ArrayFunctions.homogenizeArrayValues(List.of(Arrays.asList(null, null), List.of(new StringBuilder("a"), "b"))));
    assertEquals(List.of(Arrays.asList(null, null), List.of()),
        ArrayFunctions.homogenizeArrayValues(List.of(Arrays.asList(null, null), List.of())));
  }

  /**
   * Mixing lists with scalars should trigger an error explaining the mismatch.
   */
  @Test
  void throwsWhenMixingListsAndScalars() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(Arrays.asList(List.of(1), "scalar")));
    assertEquals("ARRAY elements must share a common type; found ARRAY and VARCHAR", ex.getMessage());
  }

  /**
   * Mixing incompatible object types (non-numeric, non-list) should be rejected.
   */
  @Test
  void throwsWhenObjectTypesDoNotMatch() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(Arrays.asList("one", new Dummy("two"))));
    assertEquals("ARRAY elements must share a common type; found VARCHAR and Dummy", ex.getMessage());
  }

  /** Uses SQL type names for numeric, temporal, and boolean incompatibilities. */
  @Test
  void usesSqlTypeNamesInDiagnostics() {
    var temporal = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(List.of(4L, java.sql.Date.valueOf("2026-01-01"))));
    assertEquals("ARRAY elements must share a common type; found NUMERIC and DATE", temporal.getMessage());
    var bool = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(List.of("one", true)));
    assertEquals("ARRAY elements must share a common type; found VARCHAR and BOOLEAN", bool.getMessage());
  }

  /**
   * Names modern temporal types and custom values without ambiguous OTHER
   * diagnostics.
   *
   * @param value
   *          value with a known SQL type label
   * @param type
   *          expected SQL type label
   */
  @ParameterizedTest
  @MethodSource("modernTypeMappings")
  void namesModernTemporalTypes(Object value, String type) {
    var failure = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(List.of("x", value)));
    assertEquals("ARRAY elements must share a common type; found VARCHAR and " + type, failure.getMessage());
  }

  /**
   * Supplies temporal and structured values with their SQL type labels.
   *
   * @return independently reported SQL type mappings
   */
  private static Stream<Arguments> modernTypeMappings() {
    return Stream.of(Arguments.of(java.util.Map.of("x", 1), "MAP"), Arguments.of(structValue(), "STRUCT"),
        Arguments.of(java.time.LocalDate.of(2026, 1, 1), "DATE"), Arguments.of(java.time.LocalTime.NOON, "TIME"),
        Arguments.of(java.time.LocalDateTime.of(2026, 1, 1, 12, 0), "TIMESTAMP"),
        Arguments.of(java.time.OffsetTime.parse("12:00:00Z"), "TIME_WITH_TIMEZONE"),
        Arguments.of(java.time.OffsetDateTime.parse("2026-01-01T12:00:00Z"), "TIMESTAMP_WITH_TIMEZONE"),
        Arguments.of(java.time.ZonedDateTime.parse("2026-01-01T12:00:00Z"), "TIMESTAMP_WITH_TIMEZONE"),
        Arguments.of(java.time.Instant.EPOCH, "TIMESTAMP_WITH_TIMEZONE"));
  }

  /**
   * Creates an Avro struct value for SQL type diagnostics.
   *
   * @return record with an integer field
   */
  private static org.apache.avro.generic.GenericData.Record structValue() {
    var schema = org.apache.avro.SchemaBuilder.record("array_struct").fields().requiredInt("value").endRecord();
    var record = new org.apache.avro.generic.GenericData.Record(schema);
    record.put("value", 1);
    return record;
  }

  /** Distinguishes MAP values from Avro STRUCT values in ARRAY diagnostics. */
  @Test
  void distinguishesMapsFromStructs() {
    var failure = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(List.of(java.util.Map.of("value", 1), structValue())));
    assertEquals("ARRAY elements must share a common type; found MAP and STRUCT", failure.getMessage());
    assertEquals("42000", assertInstanceOf(SQLSyntaxErrorException.class, failure.getCause()).getSQLState());
  }

  /**
   * Distinguishes mixed Java representations of the same SQL type.
   *
   * @param first
   *          first representation
   * @param second
   *          second representation
   * @param type
   *          shared SQL label
   */
  @ParameterizedTest
  @MethodSource("sameSqlTypeRepresentations")
  void distinguishesSameSqlTypeRepresentations(Object first, Object second, String type) {
    var values = new ArrayList<Object>(Arrays.asList(null, first, second, null));
    for (int order = 0; order < 2; order++) {
      var failure = assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.homogenizeArrayValues(values));
      assertEquals("ARRAY elements must share a common type; found " + type + " (" + values.get(1).getClass().getName()
          + ") and " + type + " (" + values.get(2).getClass().getName() + ")", failure.getMessage());
      assertEquals("42000", assertInstanceOf(SQLSyntaxErrorException.class, failure.getCause()).getSQLState());
      Collections.reverse(values);
    }
  }

  /**
   * Supplies class pairs whose SQL diagnostic labels would otherwise be
   * identical.
   *
   * @return incompatible representations and their shared label
   */
  private static Stream<Arguments> sameSqlTypeRepresentations() {
    return Stream.of(Arguments.of(java.time.LocalDateTime.of(2026, 1, 1, 12, 0),
        java.sql.Timestamp.valueOf("2026-01-01 12:00:00"), "TIMESTAMP"),
        Arguments.of(java.time.LocalTime.NOON, java.sql.Time.valueOf("12:00:00"), "TIME"));
  }

  /**
   * Accepts different Map implementations as MAP elements, including inside
   * nested arrays.
   */
  @Test
  void acceptsMixedMapImplementations() {
    var first = new java.util.HashMap<>(java.util.Map.of("value", 1));
    var second = new java.util.LinkedHashMap<>(java.util.Map.of("value", 2));
    var values = new ArrayList<Object>(Arrays.asList(null, first, second, null));
    for (int order = 0; order < 2; order++) {
      var normalized = ArrayFunctions.homogenizeArrayValues(values);
      assertEquals(values, normalized);
      assertSame(values.get(1), normalized.get(1));
      assertSame(values.get(2), normalized.get(2));
      assertThrows(UnsupportedOperationException.class, () -> normalized.add(first));
      assertEquals(List.of(List.of(first), List.of(second)),
          ArrayFunctions.homogenizeArrayValues(List.of(List.of(first), List.of(second))));
      Collections.reverse(values);
    }
  }

  /**
   * Treats reference arrays as nested arrays while retaining primitive binary
   * values.
   */
  @Test
  void normalizesReferenceArrays() {
    assertEquals(List.of(List.of(new BigDecimal("1")), List.of(new BigDecimal("2.5"))),
        ArrayFunctions.homogenizeArrayValues(List.of(new Integer[]{
            1
        }, List.of(new BigDecimal("2.5")))));
    var failure = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(List.of(new Object[]{
            1
        }, new Object[]{
            "x"
        })));
    assertEquals("42000", assertInstanceOf(SQLSyntaxErrorException.class, failure.getCause()).getSQLState());
    byte[] binary = {
        1, 2
    };
    assertEquals(List.of(binary), ArrayFunctions.homogenizeArrayValues(List.of(binary)));
  }

  /**
   * Confirm that non-numeric, non-list scalars of the same type are preserved.
   */
  @Test
  void preservesHomogeneousCustomObjects() {
    Dummy dummy1 = new Dummy("a");
    Dummy dummy2 = new Dummy("b");
    List<Object> homogenized = ArrayFunctions.homogenizeArrayValues(Arrays.asList(dummy1, dummy2, null));
    assertEquals(Arrays.asList(dummy1, dummy2, null), homogenized);
    homogenized.forEach(item -> assertTrue(item == null || item instanceof Dummy));
  }

  /**
   * Simple custom class used to verify object handling.
   */
  private static class Dummy {
    private final String value;

    /**
     * Create a dummy instance.
     *
     * @param value
     *          the payload
     */
    Dummy(String value) {
      this.value = value;
    }

    /**
     * Compare two {@link Dummy} instances for equality.
     *
     * @param obj
     *          the object to compare
     * @return {@code true} when {@code obj} has the same payload, otherwise
     *         {@code false}
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      Dummy other = (Dummy) obj;
      return value.equals(other.value);
    }

    /**
     * Calculate the hash code for this instance.
     *
     * @return the hash code of the payload
     */
    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }
}
