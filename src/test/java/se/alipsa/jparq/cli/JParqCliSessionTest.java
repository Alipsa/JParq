package se.alipsa.jparq.cli;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * Integration style tests for the CLI session to ensure command handling works
 * as expected.
 */
class JParqCliSessionTest {

  private static final Path DATA_DIR = Paths.get("src/test/resources/datasets").toAbsolutePath().normalize();

  @Test
  void shouldConnectListAndDescribeTables() {
    StringWriter outBuffer = new StringWriter();
    StringWriter errBuffer = new StringWriter();
    try (JParqCliSession session = new JParqCliSession(new PrintWriter(outBuffer, true),
        new PrintWriter(errBuffer, true))) {
      session.handleLine("/connect " + DATA_DIR);
      String output = outBuffer.toString();
      assertTrue(output.contains("Connected to " + DATA_DIR));
      assertTrue(session.prompt().contains(DATA_DIR.getFileName().toString()));
      outBuffer.getBuffer().setLength(0);

      session.handleLine("/info");
      output = outBuffer.toString();
      assertTrue(output.contains("JDBC URL: jdbc:jparq:" + DATA_DIR));
      assertTrue(output.contains("Base directory:"));
      assertTrue(output.contains("Version:"));
      outBuffer.getBuffer().setLength(0);

      session.handleLine("/list");
      output = outBuffer.toString();
      assertTrue(output.contains("PUBLIC.mtcars"));
      outBuffer.getBuffer().setLength(0);

      session.handleLine("/describe mtcars");
      output = outBuffer.toString().toLowerCase(Locale.ROOT);
      assertTrue(output.contains("model"));
      assertTrue(output.contains("mpg"));
      assertTrue(errBuffer.toString().isEmpty());
    }
  }

  @Test
  void shouldExecuteQueryAndCloseConnection() {
    StringWriter outBuffer = new StringWriter();
    StringWriter errBuffer = new StringWriter();
    try (JParqCliSession session = new JParqCliSession(new PrintWriter(outBuffer, true),
        new PrintWriter(errBuffer, true))) {
      session.connectDirectory(DATA_DIR.toString());
      String output = outBuffer.toString();
      assertTrue(output.contains("Connected to"));
      outBuffer.getBuffer().setLength(0);
      errBuffer.getBuffer().setLength(0);

      session.handleLine("SELECT COUNT(*) FROM mtcars");
      output = outBuffer.toString().toLowerCase(Locale.ROOT);
      assertTrue(output.contains("count"));
      assertTrue(output.contains("32"));
      outBuffer.getBuffer().setLength(0);

      session.handleLine("/close");
      output = outBuffer.toString();
      assertTrue(output.contains("Connection closed"));
      outBuffer.getBuffer().setLength(0);

      session.handleLine("SELECT * FROM mtcars LIMIT 1");
      assertFalse(errBuffer.toString().isEmpty());
      assertTrue(errBuffer.toString().contains("Not connected"));
    }
  }

  @Test
  void shouldNormalizeRelativePathOnConnect() {
    StringWriter outBuffer = new StringWriter();
    StringWriter errBuffer = new StringWriter();
    Path relativePath = Paths.get("src", "test", "resources", "datasets");
    try (JParqCliSession session = new JParqCliSession(new PrintWriter(outBuffer, true),
        new PrintWriter(errBuffer, true))) {
      session.connectDirectory(relativePath.toString());
      assertTrue(session.prompt().contains(DATA_DIR.getFileName().toString()));
      assertTrue(errBuffer.toString().isEmpty());
    }
  }

  @Test
  void promptShouldBeColoredLightGray() {
    StringWriter outBuffer = new StringWriter();
    StringWriter errBuffer = new StringWriter();
    try (JParqCliSession session = new JParqCliSession(new PrintWriter(outBuffer, true),
        new PrintWriter(errBuffer, true))) {
      String prompt = session.prompt();
      assertTrue(prompt.startsWith("\u001B[2;37m"));
      assertTrue(prompt.contains("\u001B[0m "));
    }
  }
}
