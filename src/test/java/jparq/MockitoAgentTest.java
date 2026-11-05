package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mockStatic;

import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Verifies startup agent configuration and inline mocking in the forked test
 * JVM.
 */
class MockitoAgentTest {

  /**
   * Checks that Mockito was supplied as a startup agent rather than
   * self-attaching.
   *
   * @throws URISyntaxException
   *           if the Mockito code source is not a valid URI
   */
  @Test
  void mockitoIsConfiguredAsStartupAgent() throws URISyntaxException {
    assumeTrue(System.getProperty("surefire.test.class.path") != null,
        "Startup-agent check only applies to Maven-forked test JVMs");
    var mockitoJar = Path.of(Mockito.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    assertTrue(ManagementFactory.getRuntimeMXBean().getInputArguments().contains("-javaagent:" + mockitoJar),
        "The test JVM must load Mockito with -javaagent");
  }

  /**
   * Exercises inline static mocking using the configured agent.
   */
  @Test
  void staticMockingWorks() {
    var expected = new UUID(123, 456);
    try (var mocked = mockStatic(UUID.class)) {
      mocked.when(UUID::randomUUID).thenReturn(expected);
      assertEquals(expected, UUID.randomUUID());
      mocked.verify(UUID::randomUUID);
    }
  }
}
