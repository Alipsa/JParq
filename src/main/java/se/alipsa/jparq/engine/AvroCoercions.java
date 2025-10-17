package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public final class AvroCoercions {

  private AvroCoercions() {}

  static Schema nonNull(Schema s) {
    if (s.getType() == Schema.Type.UNION) {
      for (Schema t : s.getTypes()) if (t.getType() != Schema.Type.NULL) return t;
    }
    return s;
  }

  @SuppressWarnings("PMD.EmptyCatchBlock")
  static Object coerceLiteral(Object lit, Schema s) {
    if (lit == null) return null;
    Schema effective = nonNull(s);
    switch (effective.getType()) {
      case STRING, ENUM:
        return lit.toString();

      case INT:
        {
          if (LogicalTypes.date().equals(effective.getLogicalType())) {
            if (lit instanceof java.sql.Date) return lit;
            try {
              return java.sql.Date.valueOf(lit.toString());
            } catch (Exception ignore) {
            }
            try {
              return Integer.parseInt(lit.toString());
            } catch (Exception ignore) {
            }
            return lit.toString();
          }
          try {
            return Integer.parseInt(lit.toString());
          } catch (Exception ignore) {
            return lit;
          }
        }

      case LONG:
        {
          if (effective.getLogicalType() instanceof LogicalTypes.TimestampMillis
              || effective.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
            if (lit instanceof Timestamp) return lit;
            try {
              return Timestamp.valueOf(lit.toString());
            } catch (Exception ignore) {
            }
            try {
              return new Timestamp(Long.parseLong(lit.toString()));
            } catch (Exception ignore) {
            }
            return lit.toString();
          }
          try {
            return Long.parseLong(lit.toString());
          } catch (Exception ignore) {
            return lit;
          }
        }

      case FLOAT, DOUBLE:
        try {
          return new BigDecimal(lit.toString());
        } catch (Exception ignore) {
          return lit;
        }

      case BOOLEAN:
        if (lit instanceof Boolean) return lit;
        return Boolean.parseBoolean(lit.toString());

      default:
        return lit; // bytes/arrays/etc. not coerced for WHERE in this minimal impl
    }
  }

  public static Object unwrap(Object v, Schema s) {
    if (v == null) return null;
    Schema effective = nonNull(s);
    switch (effective.getType()) {
      case STRING:
        return v.toString();
      case INT:
        if (LogicalTypes.date().equals(effective.getLogicalType())) {
          int days = (Integer) v;
          return java.sql.Date.valueOf(LocalDate.ofEpochDay(days));
        }
        return ((Number) v).intValue();
      case LONG:
        if (effective.getLogicalType() instanceof LogicalTypes.TimestampMillis
            || effective.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
          long epoch = ((Number) v).longValue();
          if (effective.getLogicalType() instanceof LogicalTypes.TimestampMicros) epoch /= 1000L;
          return Timestamp.from(Instant.ofEpochMilli(epoch));
        }
        return ((Number) v).longValue();
      case FLOAT:
        return ((Number) v).floatValue();
      case DOUBLE:
        return ((Number) v).doubleValue();
      case BOOLEAN:
        return v;
      case BYTES:
        if (effective.getLogicalType() instanceof LogicalTypes.Decimal dec) {
          ByteBuffer bb = ((ByteBuffer) v).duplicate();
          byte[] bytes = new byte[bb.remaining()];
          bb.get(bytes);
          java.math.BigInteger bi = new java.math.BigInteger(bytes);
          return new BigDecimal(bi, dec.getScale());
        }
        if (v instanceof ByteBuffer) {
          ByteBuffer bb = ((ByteBuffer) v).duplicate();
          byte[] b = new byte[bb.remaining()];
          bb.get(b);
          return b;
        }
        return v;
      case RECORD:
        return v.toString();
      case ENUM:
        return v.toString();
      case ARRAY:
        if (v instanceof GenericData.Array) return ((GenericData.Array<?>) v).toArray();
        return v;
      case FIXED:
        if (effective.getLogicalType() instanceof LogicalTypes.Decimal) {
          LogicalTypes.Decimal dec = (LogicalTypes.Decimal) effective.getLogicalType();
          byte[] bytes = ((GenericData.Fixed) v).bytes();
          java.math.BigInteger bi = new java.math.BigInteger(bytes);
          return new BigDecimal(bi, dec.getScale());
        }
        return ((GenericData.Fixed) v).bytes();
      default:
        return v;
    }
  }
}
