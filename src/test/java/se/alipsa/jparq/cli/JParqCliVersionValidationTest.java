package se.alipsa.jparq.cli;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for validating JVM version checks performed at CLI startup.
 */
class JParqCliVersionValidationTest {

  private static final String MAX_PROP = "jparq.maxJdkVersion";
  private String previous;

  @AfterEach
  void restoreProperty() {
    if (previous == null) {
      System.clearProperty(MAX_PROP);
    } else {
      System.setProperty(MAX_PROP, previous);
    }
  }

  @Test
  void shouldAllowCurrentRuntimeVersion() {
    previous = System.getProperty(MAX_PROP);
    int current = Runtime.version().feature();
    System.setProperty(MAX_PROP, Integer.toString(current));
    assertDoesNotThrow(JParqCli::validateJavaVersion);
  }

  @Test
  void shouldRejectNewerThanConfiguredVersion() {
    previous = System.getProperty(MAX_PROP);
    int current = Runtime.version().feature();
    System.setProperty(MAX_PROP, Integer.toString(Math.max(0, current - 1)));
    assertThrows(IllegalStateException.class, JParqCli::validateJavaVersion);
  }
}
