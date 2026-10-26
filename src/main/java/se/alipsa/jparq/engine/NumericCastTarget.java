package se.alipsa.jparq.engine;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import se.alipsa.jparq.NumericType;

/**
 * Shares numeric CAST aliases and their JDBC types across parsing and
 * evaluation.
 */
public enum NumericCastTarget {
  /** Signed eight-bit integer. */
  TINYINT(new NumericType(Types.TINYINT, 3, 0), "tinyint"),
  /** Signed sixteen-bit integer. */
  SMALLINT(new NumericType(Types.SMALLINT, 5, 0), "smallint", "int2"),
  /** Signed thirty-two-bit integer. */
  INTEGER(NumericType.of(Types.INTEGER, 0, 0), "int", "integer", "signed", "int4"),
  /** Signed sixty-four-bit integer. */
  BIGINT(NumericType.of(Types.BIGINT, 0, 0), "bigint", "int8"),
  /** Single-precision floating point, including precision-qualified FLOAT. */
  REAL(NumericType.of(Types.REAL, 0, 0), "float", "real", "float4"),
  /** Double-precision floating point. */
  DOUBLE(NumericType.of(Types.DOUBLE, 0, 0), "double", "doubleprecision", "float8");

  private static final Map<String, NumericCastTarget> BY_NAME;

  static {
    Map<String, NumericCastTarget> names = new HashMap<>();
    for (NumericCastTarget target : values()) {
      for (String alias : target.aliases) {
        names.put(alias, target);
      }
    }
    BY_NAME = Map.copyOf(names);
  }

  private final NumericType numericType;
  private final List<String> aliases;

  /**
   * Defines a target and its SQL aliases.
   *
   * @param numericType
   *          the JDBC numeric type
   * @param aliases
   *          the normalized SQL names
   */
  NumericCastTarget(NumericType numericType, String... aliases) {
    this.numericType = numericType;
    this.aliases = List.of(aliases);
  }

  /**
   * Resolves a numeric name, ignoring case, spaces, underscores, and parameters.
   *
   * @param name
   *          the SQL target name, possibly null
   * @return the numeric target, or null for an unrecognized name
   */
  public static NumericCastTarget fromName(String name) {
    if (name == null) {
      return null;
    }
    int parameters = name.indexOf('(');
    String base = parameters < 0 ? name : name.substring(0, parameters);
    return BY_NAME.get(base.toLowerCase(Locale.ROOT).trim().replace(" ", "").replace("_", ""));
  }

  /**
   * Returns the JDBC type shared by aliases of this target.
   *
   * @return the numeric type including integer precision
   */
  public NumericType numericType() {
    return numericType;
  }
}
