package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Key wrapper for a projected row. Ensures SQL DISTINCT semantics across common
 * types.
 */
final class RowKey {

  private final Object[] row;
  private final int hash;

  private RowKey(Object[] row) {
    this.row = row;
    this.hash = computeHash(row);
  }

  static RowKey of(Object[] row) {
    return new RowKey(canonicalize(row));
  }

  Object[] row() {
    return row;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RowKey other)) {
      return false;
    }
    if (row.length != other.row.length) {
      return false;
    }
    for (int i = 0; i < row.length; i++) {
      if (!valEquals(row[i], other.row[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  private static int computeHash(Object[] r) {
    int h = 1;
    for (Object v : r) {
      h = 31 * h + valHash(v);
    }
    return h;
  }

  private static Object[] canonicalize(Object[] r) {
    Object[] c = new Object[r.length];
    for (int i = 0; i < r.length; i++) {
      c[i] = canonical(r[i]);
    }
    return c;
  }

  private static Object canonical(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof BigDecimal bd) {
      return bd.stripTrailingZeros();
    }
    if (v instanceof Float f) {
      if (f.isNaN()) {
        return Float.intBitsToFloat(0x7fc00000);
      } else {
        return f;
      }
    }
    if (v instanceof Double d) {
      if (d.isNaN()) {
        return Double.longBitsToDouble(0x7ff8000000000000L);
      } else {
        return d;
      }
    }
    if (v instanceof byte[] b) {
      return Arrays.copyOf(b, b.length);
    }
    if (v instanceof ByteBuffer bb) {
      ByteBuffer dup = bb.slice();
      byte[] arr = new byte[dup.remaining()];
      dup.get(arr);
      return arr;
    }
    return v;
  }

  private static boolean valEquals(Object a, Object b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    if (a instanceof byte[] ba && b instanceof byte[] bb) {
      return Arrays.equals(ba, bb);
    }
    return Objects.equals(a, b);
  }

  private static int valHash(Object v) {
    if (v == null) {
      return 0;
    }
    if (v instanceof byte[] ba) {
      return Arrays.hashCode(ba);
    }
    return v.hashCode();
  }
}
