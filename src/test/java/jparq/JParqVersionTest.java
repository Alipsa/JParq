package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqVersion;

/**
 * Tests for manifest-derived runtime metadata in {@link JParqVersion}.
 */
class JParqVersionTest {

  /**
   * Verify the cached version data is normalized from the manifest.
   */
  @Test
  void shouldExposeNormalizedVersionFromManifest() {
    String previous = System.getProperty("jparq.maxJdkVersion");
    try {
      System.clearProperty("jparq.maxJdkVersion");
      assertEquals("1.3.0", JParqVersion.getVersion());
      assertEquals(1, JParqVersion.getMajor());
      assertEquals(3, JParqVersion.getMinor());
      assertEquals(0, JParqVersion.getPatch());
      assertEquals("99", JParqVersion.getMaxJdkVersion());
    } finally {
      restoreProperty("jparq.maxJdkVersion", previous);
    }
  }

  /**
   * Verify the maximum JDK version can be overridden at runtime.
   */
  @Test
  void shouldPreferMaxJdkOverrideProperty() {
    String previous = System.getProperty("jparq.maxJdkVersion");
    try {
      System.setProperty("jparq.maxJdkVersion", "77");
      assertEquals("77", JParqVersion.getMaxJdkVersion());
    } finally {
      restoreProperty("jparq.maxJdkVersion", previous);
    }
  }

  /**
   * Verify unreadable manifest entries are skipped and snapshot suffixes are
   * removed during parsing.
   *
   * @param tempDir
   *          temporary directory for the synthetic manifest
   * @throws ReflectiveOperationException
   *           if reflective access fails
   * @throws java.io.IOException
   *           if the manifest file cannot be written
   */
  @Test
  void loadManifestMetadataShouldSkipUnreadableResources(@TempDir Path tempDir)
      throws ReflectiveOperationException, java.io.IOException {
    Method loadMethod = JParqVersion.class.getDeclaredMethod("loadManifestMetadata", java.util.Enumeration.class);
    loadMethod.setAccessible(true);
    Path manifestPath = tempDir.resolve("MANIFEST.MF");
    Files.writeString(manifestPath, """
        Manifest-Version: 1.0
        Implementation-Title: jparq
        Implementation-Version: 4.5.6-SNAPSHOT
        Max-Jdk-Version: 55
        """);
    URL unreadable = new URL("jar:file:/definitely-missing.jar!/META-INF/MANIFEST.MF");
    URL valid = manifestPath.toUri().toURL();

    Object metadata = loadMethod.invoke(null, java.util.Collections.enumeration(List.of(unreadable, valid)));

    assertNotNull(metadata);
    assertEquals("4.5.6", invokeMetadataAccessor(metadata, "version"));
    assertEquals(4, invokeMetadataAccessor(metadata, "major"));
    assertEquals(5, invokeMetadataAccessor(metadata, "minor"));
    assertEquals(6, invokeMetadataAccessor(metadata, "patch"));
    assertEquals("55", invokeMetadataAccessor(metadata, "maxJdkVersion"));
  }

  /**
   * Invoke a no-argument accessor on the private manifest metadata record.
   *
   * @param metadata
   *          the metadata instance
   * @param accessorName
   *          the accessor method name
   * @return the accessor result
   * @throws ReflectiveOperationException
   *           if reflective access fails
   */
  private Object invokeMetadataAccessor(Object metadata, String accessorName) throws ReflectiveOperationException {
    Method accessor = metadata.getClass().getDeclaredMethod(accessorName);
    accessor.setAccessible(true);
    return accessor.invoke(metadata);
  }

  /**
   * Restore a system property to its previous value.
   *
   * @param key
   *          the property key
   * @param value
   *          the property value to restore, or {@code null} to clear it
   */
  private void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }
}
