package jparq.setops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.NumericType;

/** Verifies shared numeric widening and exact conversion contracts. */
class NumericTypeTest {

  /**
   * Checks decimal digit capacity, approximate precedence, and integer widening.
   *
   * @throws SQLException
   *           if an exact conversion fails
   */
  @Test
  void commonNumericFamily() throws SQLException {
    var integer = NumericType.of(Types.INTEGER, 0, 0);
    var bigint = NumericType.of(Types.BIGINT, 0, 0);
    var decimal = NumericType.of(Types.DECIMAL, 10, 4);
    final var approximate = NumericType.of(Types.DOUBLE, 0, 0);
    assertEquals(bigint, integer.common(bigint));
    assertEquals(bigint, bigint.common(integer));
    assertEquals(NumericType.of(Types.DECIMAL, 23, 4), decimal.common(bigint));
    assertEquals(approximate, decimal.common(approximate));
    assertEquals(approximate, approximate.common(decimal));
    assertEquals(approximate, NumericType.of(Types.FLOAT, 0, 0));
    assertEquals(new BigDecimal("9223372036854775807.0000"), decimal.common(bigint).convert(Long.MAX_VALUE));
    assertEquals(1.25, approximate.convert(new BigDecimal("1.25")));
    assertEquals(3, integer.convert(new BigDecimal("3.0")));
    assertNull(decimal.convert(null));
    assertNull(NumericType.of(Types.VARCHAR, 0, 0));
  }

  /**
   * Retains REAL for matching inputs and widens mixed approximate families.
   *
   * @throws SQLException
   *           if numeric conversion fails
   */
  @Test
  void realPromotion() throws SQLException {
    var real = NumericType.of(Types.REAL, 0, 0);
    assertEquals(real, real.common(real));
    assertEquals(Types.REAL, real.jdbcType());
    assertEquals(Schema.Type.FLOAT, real.schema().getType());
    assertEquals(0.1f, real.convert(0.1f));
    assertEquals(0.1f, real.convert(new BigDecimal("0.1")));
    for (int jdbcType : new int[]{
        Types.INTEGER, Types.BIGINT, Types.DECIMAL, Types.DOUBLE, Types.FLOAT
    }) {
      var other = NumericType.of(jdbcType, 10, 2);
      assertEquals(Types.DOUBLE, real.common(other).jdbcType());
      assertEquals(real.common(other), other.common(real));
      assertEquals((double) 0.1f, real.common(other).convert(0.1f));
    }
  }

  /** Ensures exact numeric conversion rejects overflow and fractional loss. */
  @Test
  void rejectsLossyConversions() {
    for (Object[] example : new Object[][]{
        {
            NumericType.of(Types.INTEGER, 0, 0), Long.MAX_VALUE
        }, {
            NumericType.of(Types.BIGINT, 0, 0), new BigDecimal("9223372036854775808")
        }, {
            NumericType.of(Types.BIGINT, 0, 0), new BigDecimal("1.1")
        }, {
            NumericType.of(Types.DECIMAL, 4, 2), new BigDecimal("1.001")
        }, {
            NumericType.of(Types.DECIMAL, 4, 2), new BigDecimal("100.00")
        }
    }) {
      SQLException error = assertThrows(SQLException.class, () -> ((NumericType) example[0]).convert(example[1]));
      assertEquals("22003", error.getSQLState());
    }
  }
}
