package se.alipsa.jparq;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/**
 * Describes a numeric SQL type and its exact decimal bounds. Common types use
 * approximate arithmetic when either input is approximate, then decimal, then
 * the widest integer type. Arithmetic operators can apply their own bounds to
 * the common family.
 *
 * @param jdbcType
 *          numeric JDBC type
 * @param precision
 *          maximum exact digit count, or zero for approximate types
 * @param scale
 *          decimal scale, or zero for nondecimal types
 */
public record NumericType(int jdbcType, int precision, int scale) {

  /**
   * Resolves a numeric JDBC declaration, reserving every digit of integer types.
   *
   * @param type
   *          JDBC type
   * @param precision
   *          declared decimal precision
   * @param scale
   *          declared decimal scale
   * @return numeric descriptor, or null for nonnumeric types
   */
  public static NumericType of(int type, int precision, int scale) {
    return switch (type) {
      case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> new NumericType(Types.INTEGER, 10, 0);
      case Types.BIGINT -> new NumericType(Types.BIGINT, 19, 0);
      case Types.REAL, Types.FLOAT, Types.DOUBLE -> new NumericType(Types.DOUBLE, 0, 0);
      case Types.NUMERIC, Types.DECIMAL -> new NumericType(Types.DECIMAL, Math.max(1, precision), Math.max(0, scale));
      default -> null;
    };
  }

  /**
   * Resolves a numeric materialized column using retained decimal bounds.
   *
   * @param type
   *          JDBC type
   * @param schema
   *          value schema, or null
   * @return numeric descriptor, or null for nonnumeric types
   */
  public static NumericType of(int type, Schema schema) {
    LogicalTypes.Decimal decimal = schema != null && schema.getLogicalType() instanceof LogicalTypes.Decimal d
        ? d
        : null;
    return of(type, decimal == null ? 0 : decimal.getPrecision(), decimal == null ? 0 : decimal.getScale());
  }

  /**
   * Finds a common type wide enough for either input without decimal rounding.
   *
   * @param other
   *          other numeric type
   * @return common numeric descriptor
   */
  public NumericType common(NumericType other) {
    if (jdbcType == Types.DOUBLE || other.jdbcType == Types.DOUBLE) {
      return of(Types.DOUBLE, 0, 0);
    }
    if (jdbcType == Types.DECIMAL || other.jdbcType == Types.DECIMAL) {
      int commonScale = Math.max(scale, other.scale);
      int integerDigits = Math.max(precision - scale, other.precision - other.scale);
      return of(Types.DECIMAL, integerDigits + commonScale, commonScale);
    }
    return of(jdbcType == Types.BIGINT || other.jdbcType == Types.BIGINT ? Types.BIGINT : Types.INTEGER, 0, 0);
  }

  /**
   * Converts a value to the Java representation of this type.
   *
   * @param value
   *          numeric value, or null
   * @return converted value
   * @throws SQLException
   *           if conversion would lose exact numeric data
   */
  public Object convert(Object value) throws SQLException {
    if (value == null) {
      return null;
    }
    try {
      if (jdbcType == Types.DOUBLE) {
        return ((Number) value).doubleValue();
      }
      if ((jdbcType == Types.INTEGER && value instanceof Integer)
          || (jdbcType == Types.BIGINT && value instanceof Long)) {
        return value;
      }
      BigDecimal decimal = value instanceof BigDecimal bd ? bd : new BigDecimal(value.toString());
      return switch (jdbcType) {
        case Types.INTEGER -> decimal.intValueExact();
        case Types.BIGINT -> decimal.longValueExact();
        case Types.DECIMAL -> {
          BigDecimal scaled = decimal.setScale(scale);
          if (scaled.precision() > precision) {
            throw new ArithmeticException("Decimal precision overflow");
          }
          yield scaled;
        }
        default -> throw new IllegalStateException("Unsupported numeric type: " + jdbcType);
      };
    } catch (ArithmeticException | NumberFormatException e) {
      throw new SQLException("Numeric value cannot be represented by the common type: " + value, "22003", e);
    }
  }

  /**
   * Builds the schema used to preserve the common type through materialization.
   *
   * @return nonnullable Avro value schema
   */
  public Schema schema() {
    return switch (jdbcType) {
      case Types.INTEGER -> Schema.create(Schema.Type.INT);
      case Types.BIGINT -> Schema.create(Schema.Type.LONG);
      case Types.DOUBLE -> Schema.create(Schema.Type.DOUBLE);
      case Types.DECIMAL -> LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
      default -> throw new IllegalStateException("Unsupported numeric type: " + jdbcType);
    };
  }
}
