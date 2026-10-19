package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import jparq.probe.PackagedDriverProbe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.HadoopConfigurations;

/**
 * Verifies distributed artifacts in fresh JVMs without the Maven application
 * classpath.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JParqPackagedDriverIT {
  @TempDir
  static Path temporary;
  private Path executable;
  private Path artifact;
  private Path probeClasses;
  private Path source;
  private Path database;
  private Path expected;
  private Path fatUrls;
  private Path thinUrls;
  private int runtime;
  private int release;

  @BeforeAll
  void prepare() throws Exception {
    Path home = Path.of(required("javaHome"));
    executable = home.resolve("bin").resolve(System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java");
    assertTrue(Files.isExecutable(executable), "Invalid jparq.it.javaHome executable: " + executable);
    artifact = Path.of(required("artifact"));
    source = Path.of(required("probeSource"));
    release = Integer.parseInt(required("compilerRelease"));
    final Path testClasses = Path.of(required("testClasses"));
    final Path resources = Path.of(required("resources"));
    final Path thin = Path.of(required("thinArtifact"));
    runtime = runtimeVersion(home);
    assertTrue(runtime >= 17, "jparq.it.javaHome must select Java 17 or later");
    System.out.println("Packaged driver runtime: " + home + "; feature=" + runtime + "; compiler release=" + release);
    assertTrue(Files.isRegularFile(artifact), "Package the fat JAR before running this IT");
    assertTrue(Files.isRegularFile(source), "Probe source must exist");
    probeClasses = Files.createDirectory(temporary.resolve("probe"));
    Path packageDirectory = Files.createDirectories(probeClasses.resolve("jparq/probe"));
    for (String name : List.of("PackagedDriverProbe.class", "PackagedDriverProbe$OtherDriver.class")) {
      Files.copy(testClasses.resolve("jparq/probe").resolve(name), packageDirectory.resolve(name));
    }
    Path other = temporary.resolve("other-provider.jar");
    try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(other))) {
      output.putNextEntry(new JarEntry("META-INF/services/java.sql.Driver"));
      output.write("jparq.probe.PackagedDriverProbe$OtherDriver\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      output.closeEntry();
      output.putNextEntry(new JarEntry("jparq/probe/PackagedDriverProbe$OtherDriver.class"));
      Files.copy(packageDirectory.resolve("PackagedDriverProbe$OtherDriver.class"), output);
      output.closeEntry();
    }
    fatUrls = temporary.resolve("fat-urls.txt");
    Files.write(fatUrls, List.of(artifact.toString(), other.toString()));
    thinUrls = temporary.resolve("thin-urls.txt");
    var dependencies = new ArrayList<String>();
    dependencies.add(thin.toString());
    for (String entry : Files.readString(Path.of(required("runtimeClasspathFile"))).trim()
        .split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
      dependencies.add(Path.of(entry).toAbsolutePath().toString());
    }
    Files.write(thinUrls, dependencies);
    if (runtime >= release) {
      database = Files.createDirectory(temporary.resolve("database"));
      Files.copy(resources.resolve("acme/salary.parquet"), database.resolve("salary.parquet"));
      Files.copy(resources.resolve("mtcars.parquet"), database.resolve("mtcars.parquet"));
      Files.copy(resources.resolve("hf/optimized.parquet"), database.resolve("optimized.parquet"));
      writeGzip();
      expected = temporary.resolve("expected.properties");
      expectedReads();
    }
  }

  private static String required(String name) {
    String value = System.getProperty("jparq.it." + name);
    assertTrue(value != null && !value.isBlank(), "Missing or blank required property: jparq.it." + name);
    return value;
  }

  private int runtimeVersion(Path home) throws Exception {
    Path releaseFile = home.resolve("release");
    String version;
    if (Files.isRegularFile(releaseFile)) {
      Properties properties = new Properties();
      try (var reader = Files.newBufferedReader(releaseFile)) {
        properties.load(reader);
      }
      version = properties.getProperty("JAVA_VERSION");
      assertNotNull(version, "JAVA_VERSION missing from " + releaseFile);
      version = version.replace("\"", "");
    } else {
      final Path log = temporary.resolve("version.log");
      Process process = new ProcessBuilder(executable.toString(), "-version").redirectErrorStream(true)
          .redirectOutput(log.toFile()).start();
      await(process, log);
      var matcher = java.util.regex.Pattern.compile("version \"([^\"]+)\"").matcher(Files.readString(log));
      assertTrue(matcher.find(), "Cannot detect selected Java version: " + Files.readString(log));
      version = matcher.group(1);
    }
    return Runtime.Version.parse(version).feature();
  }

  private void writeGzip() throws IOException {
    Schema schema = new Schema.Parser().parse("""
        {"type":"record","name":"CompressedProbe","fields":[
        {"name":"id","type":"int"},{"name":"text","type":"string"}]}
        """);
    try (var writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.parquet.io.LocalOutputFile(database.resolve("gzip.parquet")))
        .withConf(HadoopConfigurations.create()).withSchema(schema).withCompressionCodec(CompressionCodecName.GZIP)
        .build()) {
      for (int id = 1; id <= 3; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("text", "gzip-" + id);
        writer.write(row);
      }
    }
  }

  private void expectedReads() throws Exception {
    Properties values = new Properties();
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + database)) {
      for (String table : List.of("salary", "mtcars", "optimized", "gzip")) {
        try (var statement = connection.createStatement();
            var rows = statement.executeQuery("SELECT * FROM " + table)) {
          values.setProperty(table + ".columns", Integer.toString(rows.getMetaData().getColumnCount()));
          for (int column = 1; column <= rows.getMetaData().getColumnCount(); column++) {
            values.setProperty(table + ".column." + column, rows.getMetaData().getColumnName(column));
          }
          values.setProperty(table, PackagedDriverProbe.digest(rows));
        }
        try (var statement = connection.createStatement();
            var rows = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
          assertTrue(rows.next());
          int count = switch (table) {
            case "salary" -> 8;
            case "mtcars" -> 32;
            case "optimized" -> 16384;
            default -> 3;
          };
          assertEquals(count, rows.getInt(1), table);
        }
      }
      try (var statement = connection.createStatement();
          var rows = statement.executeQuery("SELECT * FROM gzip ORDER BY id")) {
        for (int id = 1; id <= 3; id++) {
          assertTrue(rows.next());
          assertEquals(id, rows.getInt(1));
          assertEquals("gzip-" + id, rows.getString(2));
        }
        assertFalse(rows.next());
      }
    } finally {
      TimeZone.setDefault(previous);
    }
    try (var writer = Files.newBufferedWriter(expected)) {
      values.store(writer, "Reference reads on the ordinary Maven classpath; UTC timezone");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "automatic", "explicit", "driver", "foreign", "foreign-first", "concurrent", "replacement", "discovery"
  })
  void packagedScenarios(String scenario) throws Exception {
    assumeTrue(runtime >= release, "Runtime below compiler release; functional scenario skipped");
    run(scenario, fatUrls);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "driver", "foreign"
  })
  void thinControls(String scenario) throws Exception {
    assumeTrue(runtime >= release, "Runtime below compiler release; thin control skipped");
    run(scenario, thinUrls);
  }

  /**
   * Exercises SQL Workbench/J's own driver loader, metadata commands, and batch
   * result handling.
   */
  @Test
  void sqlWorkbenchBatchClient() throws Exception {
    String client = System.getProperty("jparq.it.sqlWorkbenchJar", "");
    assumeTrue(!client.isBlank(), "Optional SQL Workbench/J installation not selected");
    assumeTrue(runtime >= release, "Runtime below compiler release; client scenario skipped");
    Path clientJar = Path.of(client).toAbsolutePath();
    assertTrue(Files.isRegularFile(clientJar), "Invalid jparq.it.sqlWorkbenchJar: " + clientJar);
    Path work = Files.createTempDirectory(temporary, "workbench-");
    Path script = work.resolve("probe.sql");
    Files.writeString(script, """
        WbListSchemas;
        WbList;
        DESCRIBE salary;
        SELECT COUNT(*) AS salary_count FROM salary;
        SELECT model FROM mtcars LIMIT 1;
        SELECT id, text FROM optimized WHERE id = 1;
        SELECT id, text FROM gzip WHERE id = 1;
        WbEcho JPARQ_WORKBENCH_OK;
        """);
    try (JarFile jar = new JarFile(clientJar.toFile())) {
      String build = jar.getManifest().getMainAttributes().getValue("WbBuild-Number");
      assertNotNull(build, "Selected JAR must identify its SQL Workbench/J build");
      System.out.println("SQL Workbench/J build " + build + ": " + clientJar);
    }
    for (int session = 1; session <= 2; session++) {
      Path log = work.resolve("session-" + session + ".log");
      Process process = workbench(clientJar, script, work, log);
      await(process, log);
      String output = Files.readString(log);
      assertTrue(output.lines().anyMatch(line -> line.trim().equals("JPARQ_WORKBENCH_OK")), output);
      assertTrue(output.contains("PUBLIC") && output.contains("change_date"), output);
      assertTrue(java.util.regex.Pattern.compile("(?m)^salary_count\\s*\\R-+\\R\\s*8\\s*$").matcher(output).find(),
          output);
      for (String value : List.of("Mazda RX4", "prefix-00001", "gzip-1")) {
        assertTrue(output.contains(value), output);
      }
    }
    // Guard the client harness itself: a failed statement must never reach the
    // success marker.
    Files.writeString(script, "SELECT * FROM missing_table;\nWbEcho JPARQ_WORKBENCH_OK;\n");
    Path failureLog = work.resolve("failure.log");
    Process failure = workbench(clientJar, script, work, failureLog);
    boolean finished = failure.waitFor(120, TimeUnit.SECONDS);
    if (!finished) {
      failure.destroyForcibly();
      failure.waitFor(10, TimeUnit.SECONDS);
    }
    assertTrue(finished, "SQL Workbench/J failure scenario timed out");
    String failureOutput = Files.readString(failureLog);
    assertFalse(failureOutput.lines().anyMatch(line -> line.trim().equals("JPARQ_WORKBENCH_OK")), failureOutput);
    assertTrue(failureOutput.contains("missing_table"), failureOutput);
    assertTrue(failure.exitValue() != 0, failureOutput);
  }

  private Process workbench(Path client, Path script, Path work, Path log) throws IOException {
    return new ProcessBuilder(executable.toString(), "-Djava.awt.headless=true", "-Duser.timezone=UTC", "-jar",
        client.toString(), "-configDir=" + work.resolve("config"), "-driver=se.alipsa.jparq.JParqDriver",
        "-driverJar=" + artifact, "-url=jdbc:jparq:" + database, "-username=", "-password=", "-autocommit=true",
        "-script=" + script, "-displayResult=true", "-abortOnError=true").directory(work.toFile())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
  }

  @Test
  void rejectsOlderRuntime() throws Exception {
    assumeTrue(runtime < release, "Runtime meets compiler release; rejection scenario skipped");
    run("reject", fatUrls);
  }

  @Test
  void packaging() throws Exception {
    try (JarFile jar = new JarFile(artifact.toFile())) {
      assertFalse(jar.stream().anyMatch(entry -> entry.getName().equals("META-INF/INDEX.LIST")
          || entry.getName().matches("META-INF/[^/]+\\.(SF|RSA|DSA|EC)")));
      assertEquals("true", jar.getManifest().getMainAttributes().getValue("Multi-Release"));
      assertTrue(jar.isMultiRelease());
      assertFalse(jar.stream().anyMatch(entry -> entry.getName().equals("module-info.class")
          || entry.getName().matches("META-INF/versions/[^/]+/module-info.class")));
      assertEquals(List.of("se.alipsa.jparq.JParqDriver"), providers(jar, "java.sql.Driver"));
      String filesystemService = "org.apache.hadoop.fs.FileSystem";
      List<String> merged = providers(jar, filesystemService);
      assertTrue(merged.contains("org.apache.hadoop.fs.LocalFileSystem"));
      String classpath = Files.readString(Path.of(required("runtimeClasspathFile"))).trim();
      for (String dependency : classpath.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
        try (JarFile input = new JarFile(dependency)) {
          if (input.getJarEntry("META-INF/services/" + filesystemService) != null) {
            assertTrue(merged.containsAll(providers(input, filesystemService)), "Lost providers from " + dependency);
          }
        }
      }
      for (JarEntry entry : jar.stream()
          .filter(e -> e.getName().matches("META-INF/services/[A-Za-z0-9_.$]+") && !e.isDirectory()).toList()) {
        for (String provider : providers(jar, entry.getName().substring("META-INF/services/".length()))) {
          assertNotNull(jar.getJarEntry(provider.replace('.', '/') + ".class"), "Missing provider: " + provider);
        }
      }
      // JarFile's runtime view must select a real versioned dependency
      // implementation.
      try (JarFile runtimeJar = new JarFile(artifact.toFile(), true, JarFile.OPEN_READ, Runtime.version())) {
        var candidate = jar.stream()
            .filter(e -> e.getName().startsWith("META-INF/versions/11/") && e.getName().endsWith(".class")).findFirst()
            .orElseThrow();
        String base = candidate.getName().substring("META-INF/versions/11/".length());
        assertTrue(runtimeJar.getJarEntry(base).getRealName().startsWith("META-INF/versions/"));
      }
    }
  }

  /**
   * Verifies JParq's license survives in both artifacts and each dependency license
   * and notice survives assembly, including suffixed filenames.
   */
  @Test
  void dependencyAttributions() throws Exception {
    String classpath = Files.readString(Path.of(required("runtimeClasspathFile"))).trim();
    String projectLicense = Files.readString(Path.of(required("resources")).resolve("../../../LICENSE").normalize())
        .replace("\r\n", "\n").strip();
    for (Path packaged : List.of(artifact, Path.of(required("thinArtifact")))) {
      try (JarFile jar = new JarFile(packaged.toFile())) {
        assertEquals(projectLicense, archiveText(jar, jar.getJarEntry("LICENSE")),
            "JParq's license must survive in " + packaged);
      }
    }
    try (JarFile bundled = new JarFile(artifact.toFile())) {
      String aggregate = archiveText(bundled, bundled.getJarEntry("META-INF/NOTICE"));
      for (String kind : List.of("LICENSE", "NOTICE")) {
        int sources = 0;
        for (String dependency : classpath.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
          try (JarFile input = new JarFile(dependency)) {
            for (JarEntry entry : input.stream().filter(candidate -> !candidate.isDirectory()).toList()) {
              String name = entry.getName().toUpperCase(java.util.Locale.ROOT);
              if (name.startsWith("META-INF/" + kind) && !name.substring("META-INF/".length()).contains("/")) {
                String original = archiveText(input, entry);
                assertTrue(aggregate.contains(original), "Lost " + dependency + "!/" + entry.getName());
                sources++;
              }
            }
          }
        }
        assertTrue(sources > 1, "Expected multiple dependency " + kind + " files");
      }
    }
  }

  private static String archiveText(JarFile jar, JarEntry entry) throws IOException {
    assertNotNull(entry, "Missing aggregated attribution in " + jar.getName());
    try (InputStream input = jar.getInputStream(entry)) {
      return new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).replace("\r\n", "\n").strip();
    }
  }

  private static List<String> providers(JarFile jar, String service) throws IOException {
    JarEntry entry = jar.getJarEntry("META-INF/services/" + service);
    assertNotNull(entry, "Missing service: " + service);
    try (InputStream input = jar.getInputStream(entry)) {
      return new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).lines()
          .map(line -> line.split("#", 2)[0].trim()).filter(line -> !line.isEmpty()).distinct().toList();
    }
  }

  private void run(String scenario, Path urls) throws Exception {
    Path work = Files.createTempDirectory(temporary, scenario + "-");
    final Path log = work.resolve("probe.log");
    var command = new ArrayList<String>();
    command.add(executable.toString());
    command.add("-Duser.timezone=UTC");
    if ("discovery".equals(scenario)) {
      command.add("-Xlog:class+load:file=class-load.log");
    }
    command.add("-cp");
    command.add(probeClasses
        + (("automatic".equals(scenario) || "explicit".equals(scenario)) ? java.io.File.pathSeparator + artifact : ""));
    command.add("reject".equals(scenario) ? source.toString() : PackagedDriverProbe.class.getName());
    command.addAll(List.of(scenario, urls.toString(), database == null ? "unused" : database.toString(),
        expected == null ? "unused" : expected.toString()));
    Process process = new ProcessBuilder(command).directory(work.toFile()).redirectErrorStream(true)
        .redirectOutput(log.toFile()).start();
    await(process, log);
    String output = Files.readString(log);
    assertTrue(output.contains("PROBE_STARTED " + runtime), "Probe startup failed: " + output);
    assertTrue(output.contains("reject".equals(scenario) ? "DRIVER_REJECTED " : "PROBE_OK " + scenario), output);
    assertFalse(output.contains("UnsatisfiedLinkError"), output);
    if (output.contains("NoClassDefFoundError: org/apache/hadoop/util/ShutdownHookManager")) {
      System.out.println("Hadoop shutdown hook attempted class loading after URLClassLoader.close(): " + scenario);
    }
    if ("discovery".equals(scenario)) {
      String loads = Files.readString(work.resolve("class-load.log"));
      assertFalse(loads.contains("org.apache.hadoop."), loads);
      assertFalse(loads.contains("org.apache.parquet."), loads);
    }
  }

  private static void await(Process process, Path log) throws Exception {
    boolean finished = process.waitFor(120, TimeUnit.SECONDS);
    if (!finished) {
      process.destroyForcibly();
      process.waitFor(10, TimeUnit.SECONDS);
    }
    assertTrue(finished, "Probe timed out: " + Files.readString(log));
    assertEquals(0, process.exitValue(), () -> {
      try {
        return Files.readString(log);
      } catch (IOException exception) {
        return exception.toString();
      }
    });
  }
}
