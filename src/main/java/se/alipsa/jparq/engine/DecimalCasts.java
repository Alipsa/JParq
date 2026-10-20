package se.alipsa.jparq.engine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import net.sf.jsqlparser.expression.CastExpression;
import org.apache.avro.LogicalTypes;

/**
 * Shares decimal CAST names, bounds, and conversion rules with JDBC metadata.
 */
public final class DecimalCasts {
  private static final Set<String> TYPE_NAMES = Set.of("decimal", "numeric", "number");

  /** Prevents utility class instantiation. */
  private DecimalCasts() {
  }

  /**
   * Identifies decimal type names, including parameterized declarations.
   *
   * @param name
   *          the SQL type name
   * @return whether this is DECIMAL, NUMERIC, or NUMBER
   */
  public static boolean isDecimalType(String name) {
    if (name == null) {
      return false;
    }
    int parameters = name.indexOf('(');
    String base = parameters < 0 ? name : name.substring(0, parameters);
    return TYPE_NAMES.contains(base.trim().toLowerCase(Locale.ROOT));
  }

  /**
   * Resolves valid, explicitly declared decimal bounds.
   *
   * @param cast
   *          the CAST expression
   * @return the decimal type, or null for absent or invalid bounds or other types
   */
  public static LogicalTypes.Decimal declaredType(CastExpression cast) {
    if (cast == null || cast.getColDataType() == null) {
      return null;
    }
    var type = cast.getColDataType();
    if (!isDecimalType(type.getDataType())) {
      return null;
    }
    int[] bounds = decimalBounds(cast);
    if (bounds == null) {
      return null;
    }
    int precision = bounds[0];
    int scale = bounds[1];
    if (precision < 1 || scale < 0 || scale > precision) {
      return null;
    }
    return LogicalTypes.decimal(precision, scale);
  }

  /**
   * Applies declared scale before checking precision, preserving unbounded casts.
   *
   * @param cast
   *          the decimal CAST expression
   * @param value
   *          the decimal value to convert, possibly null
   * @return the value rounded to the declared scale, or unchanged without bounds
   * @throws IllegalArgumentException
   *           if bounds are invalid or the rounded value overflows; the cause is
   *           a SQLException with SQLState 22003
   */
  public static BigDecimal convert(CastExpression cast, BigDecimal value) {
    var declaration = cast.getColDataType();
    if (decimalBounds(cast) == null) {
      return value;
    }
    LogicalTypes.Decimal type = declaredType(cast);
    if (type == null) {
      throw numericFailure("Invalid decimal bounds: " + declaration);
    }
    if (value == null) {
      return null;
    }
    BigDecimal result = value.setScale(type.getScale(), RoundingMode.HALF_UP);
    if (result.precision() > type.getPrecision()) {
      throw numericFailure("Value '" + value + "' exceeds decimal precision " + type.getPrecision());
    }
    return result;
  }

  /**
   * Finds a numeric range error across unchecked expression-evaluation
   * boundaries.
   *
   * @param failure
   *          the exception or wrapper to inspect
   * @return the SQLState 22003 exception, or null when none is present
   */
  public static SQLException findNumericFailure(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof SQLException sql && "22003".equals(sql.getSQLState())) {
        return sql;
      }
      current = current.getCause();
    }
    return null;
  }

  /**
   * Reads bounds from parsed declarations or programmatically constructed CASTs.
   *
   * @param cast
   *          the decimal CAST expression
   * @return precision and scale, or null without an explicit precision
   */
  private static int[] decimalBounds(CastExpression cast) {
    var type = cast.getColDataType();
    if (type == null) {
      return null;
    }
    if (type.getPrecision() != null) {
      return new int[]{
          type.getPrecision(), type.getScale() == null ? 0 : type.getScale()
      };
    }
    List<String> arguments = type.getArgumentsStringList();
    if (arguments == null || arguments.isEmpty()) {
      String name = type.getDataType();
      int start = name.indexOf('(');
      int end = name.indexOf(')', start + 1);
      if (start < 0 || end < 0) {
        return null;
      }
      arguments = List.of(name.substring(start + 1, end).split(","));
    }
    return new int[]{
        Integer.parseInt(arguments.getFirst().trim()),
        arguments.size() > 1 ? Integer.parseInt(arguments.get(1).trim()) : 0
    };
  }

  /**
   * Wraps a JDBC numeric range error for unchecked expression evaluation.
   *
   * @param message
   *          the conversion failure description
   * @return the unchecked exception carrying SQLState 22003
   */
  private static IllegalArgumentException numericFailure(String message) {
    return new IllegalArgumentException(message, new SQLDataException(message, "22003"));
  }
}
