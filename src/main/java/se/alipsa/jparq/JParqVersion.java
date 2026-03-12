package se.alipsa.jparq;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Canonical runtime metadata for JParq derived from the package manifest.
 *
 * <p>
 * The manifest is scanned once during class initialization and the parsed
 * values are cached for reuse across the driver and CLI surfaces.
 * </p>
 */
public final class JParqVersion {

  private static final Logger LOG = LoggerFactory.getLogger(JParqVersion.class);
  private static final String IMPLEMENTATION_TITLE = "jparq";
  private static final String MAX_JDK_OVERRIDE_PROPERTY = "jparq.maxJdkVersion";
  private static final Pattern SEMANTIC_VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)(?:[.-].*)?$");
  private static final ManifestMetadata METADATA = loadManifestMetadata();

  private JParqVersion() {
    // Utility class
  }

  /**
   * Get the normalized semantic version string for the running JParq build.
   *
   * @return the {@code major.minor.patch} version string
   */
  public static String getVersion() {
    return METADATA.version();
  }

  /**
   * Get the major semantic version number.
   *
   * @return the major version number
   */
  public static int getMajor() {
    return METADATA.major();
  }

  /**
   * Get the minor semantic version number.
   *
   * @return the minor version number
   */
  public static int getMinor() {
    return METADATA.minor();
  }

  /**
   * Get the patch semantic version number.
   *
   * @return the patch version number
   */
  public static int getPatch() {
    return METADATA.patch();
  }

  /**
   * Get the maximum supported JDK version.
   *
   * <p>
   * The system property {@code jparq.maxJdkVersion} overrides the manifest value
   * when present so tests and CLI runs can force a specific limit.
   * </p>
   *
   * @return the configured maximum JDK version, or {@code null} if none is
   *         available
   */
  public static String getMaxJdkVersion() {
    String override = trimToNull(System.getProperty(MAX_JDK_OVERRIDE_PROPERTY));
    return override == null ? METADATA.maxJdkVersion() : override;
  }

  /**
   * Load JParq manifest metadata from the classpath.
   *
   * @return the parsed manifest metadata
   */
  private static ManifestMetadata loadManifestMetadata() {
    try {
      Enumeration<URL> resources = JParqVersion.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
      ManifestMetadata metadata = loadManifestMetadata(resources);
      if (metadata != null) {
        return metadata;
      }
    } catch (IOException e) {
      LOG.warn("Unable to enumerate manifest resources for JParq metadata: {}", e.getMessage());
    }
    return fallbackManifestMetadata();
  }

  /**
   * Load JParq manifest metadata from candidate manifest resources.
   *
   * @param resources
   *          the manifest resource URLs to inspect
   * @return the parsed manifest metadata, or {@code null} if no matching manifest
   *         entry is found
   */
  private static ManifestMetadata loadManifestMetadata(Enumeration<URL> resources) {
    while (resources.hasMoreElements()) {
      URL url = resources.nextElement();
      try (InputStream stream = url.openStream()) {
        ManifestMetadata metadata = manifestMetadata(new Manifest(stream));
        if (metadata != null) {
          return metadata;
        }
      } catch (IOException e) {
        LOG.warn("Unable to read JParq manifest metadata: {}", e.getMessage());
      }
    }
    return null;
  }

  /**
   * Parse JParq metadata from a manifest.
   *
   * @param manifest
   *          the manifest to inspect
   * @return the parsed metadata, or {@code null} when the manifest does not
   *         belong to JParq
   */
  private static ManifestMetadata manifestMetadata(Manifest manifest) {
    Attributes attributes = manifest.getMainAttributes();
    String title = trimToNull(attributes.getValue("Implementation-Title"));
    if (!IMPLEMENTATION_TITLE.equalsIgnoreCase(title)) {
      return null;
    }
    String implementationVersion = trimToNull(attributes.getValue("Implementation-Version"));
    if (implementationVersion == null) {
      return null;
    }
    if (implementationVersion.contains("${")) {
      LOG.warn("Skipping unresolved JParq Implementation-Version placeholder: {}", implementationVersion);
      return null;
    }
    SemanticVersion version = parseSemanticVersion(implementationVersion);
    if (version == null) {
      LOG.warn("Skipping unsupported JParq Implementation-Version: {}", implementationVersion);
      return null;
    }
    return new ManifestMetadata(version.version(), version.major(), version.minor(), version.patch(),
        trimToNull(attributes.getValue("Max-Jdk-Version")));
  }

  /**
   * Build fallback metadata when no manifest entry can be resolved.
   *
   * @return fallback metadata derived from the package or default values
   */
  private static ManifestMetadata fallbackManifestMetadata() {
    Package pkg = JParqVersion.class.getPackage();
    String implementationVersion = pkg == null ? null : trimToNull(pkg.getImplementationVersion());
    if (implementationVersion != null) {
      SemanticVersion version = parseSemanticVersion(implementationVersion);
      if (version != null) {
        return new ManifestMetadata(version.version(), version.major(), version.minor(), version.patch(), null);
      }
    }
    LOG.warn("JParq manifest metadata not found; defaulting version information to 0.0.0.");
    return new ManifestMetadata("0.0.0", 0, 0, 0, null);
  }

  /**
   * Parse a manifest implementation version for semantic version reporting.
   *
   * @param implementationVersion
   *          the raw implementation version
   * @return the parsed semantic version, or {@code null} when the value does not
   *         begin with {@code major.minor.patch}
   */
  private static SemanticVersion parseSemanticVersion(String implementationVersion) {
    String trimmedVersion = implementationVersion.trim();
    Matcher matcher = SEMANTIC_VERSION_PATTERN.matcher(trimmedVersion);
    if (!matcher.matches()) {
      return null;
    }
    return new SemanticVersion(matcher.group(1) + "." + matcher.group(2) + "." + matcher.group(3),
        Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)), Integer.parseInt(matcher.group(3)));
  }

  /**
   * Trim a string and convert blanks to {@code null}.
   *
   * @param value
   *          the string to normalize
   * @return the trimmed string, or {@code null} when blank
   */
  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  /**
   * Immutable manifest-derived metadata for JParq.
   *
   * @param version
   *          the normalized semantic version string
   * @param major
   *          the major version number
   * @param minor
   *          the minor version number
   * @param patch
   *          the patch version number
   * @param maxJdkVersion
   *          the maximum supported JDK version
   */
  private record ManifestMetadata(String version, int major, int minor, int patch, String maxJdkVersion) {
  }

  /**
   * Immutable parsed semantic version components.
   *
   * @param version
   *          the normalized semantic version string
   * @param major
   *          the major version number
   * @param minor
   *          the minor version number
   * @param patch
   *          the patch version number
   */
  private record SemanticVersion(String version, int major, int minor, int patch) {
  }
}
