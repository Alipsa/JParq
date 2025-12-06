package jparq.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.slf4j.simple.SimpleLogger;
import se.alipsa.jparq.cli.JParqCli;

/**
 * Tests for internal utility methods in {@link JParqCli}.
 */
class JParqCliUtilityMethodsTest {

  /**
   * Verify version parsing supports both legacy (1.x) and modern version formats.
   *
   * @throws ReflectiveOperationException
   *           if reflection fails
   */
  @Test
  void parseFeatureVersionShouldReturnFeatureForModernAndLegacyVersions() throws ReflectiveOperationException {
    Method parseMethod = getAccessibleMethod("parseFeatureVersion", String.class);
    int legacyFeature = (int) parseMethod.invoke(null, "1.8.0_372");
    int modernFeature = (int) parseMethod.invoke(null, "17.0.8.1");
    int singleFeature = (int) parseMethod.invoke(null, "21");
    assertEquals(8, legacyFeature);
    assertEquals(17, modernFeature);
    assertEquals(21, singleFeature);
  }

  /**
   * Ensure an explicit directory argument is normalized and used as the initial
   * directory.
   *
   * @throws ReflectiveOperationException
   *           if reflection fails
   */
  @Test
  void resolveInitialDirectoryShouldHonorProvidedArgument() throws ReflectiveOperationException {
    Method resolveMethod = getAccessibleMethod("resolveInitialDirectory", String[].class);
    Path expected = Paths.get("src/test/resources/datasets").toAbsolutePath().normalize();
    Path resolved = (Path) resolveMethod.invoke(null, new Object[]{
        new String[]{
            "src/test/resources/datasets"
        }
    });
    assertEquals(expected, resolved);
  }

  /**
   * Verify the current working directory is used when no valid argument is
   * provided.
   *
   * @throws ReflectiveOperationException
   *           if reflection fails
   */
  @Test
  void resolveInitialDirectoryShouldUseWorkingDirWhenMissing() throws ReflectiveOperationException {
    Method resolveMethod = getAccessibleMethod("resolveInitialDirectory", String[].class);
    Path workingDir = Paths.get("").toAbsolutePath().normalize();
    Path resolvedNull = (Path) resolveMethod.invoke(null, new Object[]{
        null
    });
    Path resolvedBlank = (Path) resolveMethod.invoke(null, new Object[]{
        new String[]{
            "   "
        }
    });
    Path resolvedEmpty = (Path) resolveMethod.invoke(null, new Object[]{
        new String[]{}
    });
    assertEquals(workingDir, resolvedNull);
    assertEquals(workingDir, resolvedBlank);
    assertEquals(workingDir, resolvedEmpty);
  }

  /**
   * Validate that the logging configuration enforces error level defaults.
   *
   * @throws ReflectiveOperationException
   *           if reflection fails
   */
  @Test
  void configureLoggingShouldSetDefaultErrorLevels() throws ReflectiveOperationException {
    Method configureMethod = getAccessibleMethod("configureLogging");
    String defaultLevelKey = SimpleLogger.DEFAULT_LOG_LEVEL_KEY;
    String previousDefault = System.getProperty(defaultLevelKey);
    String previousHadoop = System.getProperty("org.slf4j.simpleLogger.log.org.apache.hadoop");
    String previousParquet = System.getProperty("org.slf4j.simpleLogger.log.org.apache.parquet");
    try {
      System.clearProperty(defaultLevelKey);
      System.setProperty("org.slf4j.simpleLogger.log.org.apache.hadoop", "info");
      System.clearProperty("org.slf4j.simpleLogger.log.org.apache.parquet");
      configureMethod.invoke(null);
      assertEquals("error", System.getProperty(defaultLevelKey));
      assertEquals("error", System.getProperty("org.slf4j.simpleLogger.log.org.apache.hadoop"));
      assertEquals("error", System.getProperty("org.slf4j.simpleLogger.log.org.apache.parquet"));
    } finally {
      restoreProperty(defaultLevelKey, previousDefault);
      restoreProperty("org.slf4j.simpleLogger.log.org.apache.hadoop", previousHadoop);
      restoreProperty("org.slf4j.simpleLogger.log.org.apache.parquet", previousParquet);
    }
  }

  /**
   * Verify that a manifest entry is used when no system property override is
   * present.
   *
   * @throws ReflectiveOperationException
   *           if reflection fails
   */
  @Test
  void resolveMaxJdkVersionShouldReadFromManifest() throws ReflectiveOperationException {
    Method resolveMethod = getAccessibleMethod("resolveMaxJdkVersion");
    String previous = System.getProperty("jparq.maxJdkVersion");
    try {
      System.clearProperty("jparq.maxJdkVersion");
      String resolved = (String) resolveMethod.invoke(null);
      assertEquals("99", resolved);
    } finally {
      restoreProperty("jparq.maxJdkVersion", previous);
    }
  }

  /**
   * Ensure Java version validation passes when the manifest allows the current
   * runtime.
   *
   * @throws ReflectiveOperationException
   *           if reflection fails
   */
  @Test
  void validateJavaVersionShouldAllowWhenManifestVersionIsHigher() throws ReflectiveOperationException {
    Method validateMethod = JParqCli.class.getMethod("validateJavaVersion");
    String previous = System.getProperty("jparq.maxJdkVersion");
    try {
      System.clearProperty("jparq.maxJdkVersion");
      validateMethod.invoke(null);
    } finally {
      restoreProperty("jparq.maxJdkVersion", previous);
    }
  }

  /**
   * Make a private static method accessible for reflective invocation.
   *
   * @param name
   *          the method name
   * @param parameterTypes
   *          the parameter types of the method
   * @return an accessible {@link Method} instance
   * @throws NoSuchMethodException
   *           if no method is found with the provided signature
   */
  private Method getAccessibleMethod(String name, Class<?>... parameterTypes) throws NoSuchMethodException {
    Method method = JParqCli.class.getDeclaredMethod(name, parameterTypes);
    method.setAccessible(true);
    return method;
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
