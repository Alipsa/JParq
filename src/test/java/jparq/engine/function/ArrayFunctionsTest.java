package jparq.engine.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.function.ArrayFunctions;

/**
 * Unit tests for {@link ArrayFunctions}.
 */
class ArrayFunctionsTest {

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
   * Verify that homogeneous lists are accepted unchanged and remain unmodifiable.
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
   * Mixing lists with scalars should trigger an error explaining the mismatch.
   */
  @Test
  void throwsWhenMixingListsAndScalars() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(Arrays.asList(List.of(1), "scalar")));
    assertTrue(ex.getMessage() != null && ex.getMessage().toLowerCase().contains("list"));
  }

  /**
   * Mixing incompatible object types (non-numeric, non-list) should be rejected.
   */
  @Test
  void throwsWhenObjectTypesDoNotMatch() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.homogenizeArrayValues(Arrays.asList("one", new Dummy("two"))));
    assertTrue(ex.getMessage().contains("java.lang.String"));
    assertTrue(ex.getMessage().contains(Dummy.class.getName()));
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
