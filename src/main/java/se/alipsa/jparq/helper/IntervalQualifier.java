package se.alipsa.jparq.helper;

import java.util.Locale;

/** SQL interval field ranges retained for text output and arithmetic. */
public enum IntervalQualifier {
  /** Years only. */
  YEAR(0, 0),
  /** Months only. */
  MONTH(1, 1),
  /** Days only. */
  DAY(2, 2),
  /** Hours only. */
  HOUR(3, 3),
  /** Minutes only. */
  MINUTE(4, 4),
  /** Seconds, including fractional seconds. */
  SECOND(5, 5),
  /** Years and months. */
  YEAR_TO_MONTH(0, 1),
  /** Days and hours. */
  DAY_TO_HOUR(2, 3),
  /** Days through minutes. */
  DAY_TO_MINUTE(2, 4),
  /** Days through fractional seconds. */
  DAY_TO_SECOND(2, 5),
  /** Hours and minutes. */
  HOUR_TO_MINUTE(3, 4),
  /** Hours through fractional seconds. */
  HOUR_TO_SECOND(3, 5),
  /** Minutes through fractional seconds. */
  MINUTE_TO_SECOND(4, 5),
  /** Legacy extension combining calendar months and day/time fields. */
  MIXED(0, 5);

  private final int leading;
  private final int trailing;

  /**
   * Defines an inclusive range of datetime fields.
   *
   * @param leading
   *          leading field index
   * @param trailing
   *          trailing field index
   */
  IntervalQualifier(int leading, int trailing) {
    this.leading = leading;
    this.trailing = trailing;
  }

  /**
   * Resolves a supported SQL unit or compound qualifier.
   *
   * @param unit
   *          unit name, including plural aliases
   * @return the field qualifier
   */
  public static IntervalQualifier fromUnit(String unit) {
    String name = unit.toUpperCase(Locale.ROOT).trim().replace(' ', '_');
    if (!name.contains("_TO_") && name.endsWith("S")) {
      name = name.substring(0, name.length() - 1);
    }
    return valueOf(name);
  }

  /**
   * Finds the result range of interval addition or subtraction.
   *
   * @param other
   *          other operand's field range
   * @return the common field range, or MIXED for the legacy mixed-family
   *         extension
   */
  public IntervalQualifier common(IntervalQualifier other) {
    int start = Math.min(leading, other.leading);
    int end = Math.max(trailing, other.trailing);
    for (IntervalQualifier qualifier : values()) {
      if (qualifier.leading == start && qualifier.trailing == end) {
        return qualifier;
      }
    }
    return MIXED;
  }

  /**
   * Reports the first field index in YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
   * order.
   *
   * @return first field index
   */
  public int leading() {
    return leading;
  }

  /**
   * Reports the last field index in YEAR, MONTH, DAY, HOUR, MINUTE, SECOND order.
   *
   * @return last field index
   */
  public int trailing() {
    return trailing;
  }
}
