package se.alipsa.jparq;

/**
 * Canonical runtime version information for JParq.
 *
 * <p>
 * This centralizes the values exposed through JDBC metadata so
 * version-reporting methods stay consistent across the driver surface.
 * </p>
 */
public final class JParqVersion {

  /** Major release number. */
  public static final int MAJOR = 1;

  /** Minor release number. */
  public static final int MINOR = 3;

  /** Patch release number. */
  public static final int PATCH = 0;

  /** Full semantic version string. */
  public static final String VERSION = MAJOR + "." + MINOR + "." + PATCH;

  private JParqVersion() {
    // Utility class
  }
}
