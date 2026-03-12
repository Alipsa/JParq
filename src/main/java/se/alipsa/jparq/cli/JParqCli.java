package se.alipsa.jparq.cli;

import static se.alipsa.jparq.cli.JParqCliSession.ANSI_RESET;
import static se.alipsa.jparq.cli.JParqCliSession.PROMPT_COLOR;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import se.alipsa.jparq.JParqVersion;

/**
 * Entry point for the interactive JParq command line interface.
 */
public final class JParqCli {

  private static final Logger LOG = LoggerFactory.getLogger(JParqCli.class);

  private JParqCli() {
    // utility class
  }

  /**
   * Start the CLI.
   *
   * @param args
   *          optional first argument specifying the base directory of Parquet
   *          files; when omitted the current working directory is used
   */
  public static void main(String[] args) {
    try {
      configureLogging();
      validateJavaVersion();
      Terminal terminal = TerminalBuilder.builder().system(true).build();
      Path historyFile = Paths.get(System.getProperty("user.home"), ".jparq_history");
      LineReader reader = LineReaderBuilder.builder().terminal(terminal).appName("jparq")
          .variable(LineReader.HISTORY_FILE, historyFile).highlighter(new UserInputHighlighter()).build();
      PrintWriter out = new PrintWriter(terminal.output(), true);
      PrintWriter err = new PrintWriter(terminal.output(), true);
      try (JParqCliSession session = new JParqCliSession(out, err)) {
        out.println(PROMPT_COLOR + "JParq CLI version " + JParqCliSession.cliVersion() + ANSI_RESET);
        Path initialDir = resolveInitialDirectory(args);
        session.connectDirectory(initialDir.toString());
        boolean running = true;
        while (running) {
          String line;
          try {
            line = reader.readLine(session.prompt());
          } catch (UserInterruptException e) {
            // Continue on interrupt to let users type /exit
            continue;
          } catch (EndOfFileException e) {
            break;
          }
          running = session.handleLine(line);
        }
      }
    } catch (IllegalStateException e) {
      LOG.error(e.getMessage());
      System.exit(1);
    } catch (IOException e) {
      LOG.error("Failed to start JParq CLI: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  private static void configureLogging() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "error");
    System.setProperty("org.slf4j.simpleLogger.log.org.apache.hadoop", "error");
    System.setProperty("org.slf4j.simpleLogger.log.org.apache.parquet", "error");
  }

  /**
   * Ensure the running Java version does not exceed the supported maximum. The
   * maximum is resolved by {@link JParqVersion#getMaxJdkVersion()}. Throws an
   * {@link IllegalStateException} when the current runtime is newer than the
   * configured limit.
   */
  public static void validateJavaVersion() {
    String maxJdk = JParqVersion.getMaxJdkVersion();
    if (maxJdk == null || maxJdk.isBlank()) {
      LOG.warn("Max-Jdk-Version not specified; skipping Java version check.");
      return;
    }
    int maxFeature = parseFeatureVersion(maxJdk);
    int currentFeature = Runtime.version().feature();
    if (currentFeature > maxFeature) {
      throw new IllegalStateException(
          "Java " + Runtime.version() + " is not supported. Maximum supported major version is " + maxFeature + ".");
    }
  }

  /**
   * Convert a Java version string to its feature (major) integer component.
   *
   * @param version
   *          the version string, e.g. {@code "21"} or {@code "1.8"}
   * @return the major version number
   */
  private static int parseFeatureVersion(String version) {
    String normalized = version.trim();
    if (normalized.startsWith("1.")) {
      normalized = normalized.substring(2);
    }
    int dot = normalized.indexOf('.');
    String major = dot > -1 ? normalized.substring(0, dot) : normalized;
    return Integer.parseInt(major);
  }

  private static Path resolveInitialDirectory(String[] args) {
    if (args != null && args.length > 0 && args[0] != null && !args[0].isBlank()) {
      return Paths.get(args[0]).toAbsolutePath().normalize();
    }
    return Paths.get("").toAbsolutePath().normalize();
  }
}
