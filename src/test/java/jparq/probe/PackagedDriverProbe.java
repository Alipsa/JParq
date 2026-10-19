package jparq.probe;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.jar.JarFile;
import java.util.logging.Logger;

/** JDK-only subprocess probe, also launchable as Java 17 single-file source. */
public final class PackagedDriverProbe {
  private static final String DRIVER = "se.alipsa.jparq.JParqDriver";

  private PackagedDriverProbe() {
  }

  /**
   * Runs one isolated packaged-driver scenario.
   *
   * @param args
   *          scenario, artifact URL-list file, database directory, expected reads
   *          file
   * @throws Exception
   *           if a scenario fails
   */
  public static void main(String[] args) throws Exception {
    System.out.println("PROBE_STARTED " + Runtime.version().feature());
    String scenario = args[0];
    List<URL> urls = new ArrayList<>();
    for (String entry : Files.readAllLines(Path.of(args[1]))) {
      urls.add(Path.of(entry).toUri().toURL());
    }
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      if ("reject".equals(scenario)) {
        try (URLClassLoader loader = loader(urls)) {
          try {
            Class.forName(DRIVER, true, loader);
          } catch (UnsupportedClassVersionError exception) {
            require(exception.getMessage().replace('/', '.').contains(DRIVER),
                "Rejection must name JParqDriver: " + exception);
            System.out.println("DRIVER_REJECTED " + exception);
            return;
          }
          throw new AssertionError("Driver unexpectedly loaded");
        }
      }
      if ("automatic".equals(scenario) || "explicit".equals(scenario)) {
        verifyRuntimeView(ClassLoader.getSystemClassLoader(), urls.get(0));
        if ("explicit".equals(scenario)) {
          Class.forName(DRIVER);
        }
        try (Connection connection = DriverManager.getConnection("jdbc:jparq:" + args[2])) {
          verify(connection, Path.of(args[3]));
        }
      } else {
        try (URLClassLoader loader = loader(urls)) {
          verifyRuntimeView(loader, urls.get(0));
          Thread.currentThread()
              .setContextClassLoader("driver".equals(scenario) ? loader : ClassLoader.getPlatformClassLoader());
          if ("discovery".equals(scenario)) {
            discovery(loader);
          } else {
            if ("foreign-first".equals(scenario)) {
              // Populate this very loader's Hadoop cache before JParq is loaded.
              Class<?> fs = Class.forName("org.apache.hadoop.fs.FileSystem", true, loader);
              try {
                fs.getMethod("getFileSystemClass", String.class,
                    Class.forName("org.apache.hadoop.conf.Configuration", true, loader)).invoke(null, "file", null);
              } catch (java.lang.reflect.InvocationTargetException exception) {
                require(exception.getCause().getClass().getSimpleName().equals("UnsupportedFileSystemException"),
                    "Expected cold foreign-TCCL provider lookup failure: " + exception.getCause());
              }
            }
            Driver driver = (Driver) Class.forName(DRIVER, true, loader).getConstructor().newInstance();
            if ("concurrent".equals(scenario)) {
              concurrent(driver, args);
            } else {
              connect(driver, args);
            }
          }
        }
        if ("replacement".equals(scenario)) {
          try (URLClassLoader second = loader(urls)) {
            Driver driver = (Driver) Class.forName(DRIVER, true, second).getConstructor().newInstance();
            connect(driver, args);
          }
        }
      }
      System.out.println("PROBE_OK " + scenario);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  private static void verifyRuntimeView(ClassLoader loader, URL artifact) throws Exception {
    try (JarFile jar = new JarFile(Path.of(artifact.toURI()).toFile())) {
      if (!jar.isMultiRelease()) {
        return; // The ordinary thin JAR has no versioned classes of its own.
      }
      var entry = jar.stream().filter(candidate -> candidate.getName().startsWith("META-INF/versions/11/")
          && candidate.getName().endsWith(".class")).findFirst().orElseThrow();
      String base = entry.getName().substring("META-INF/versions/11/".length());
      URL selected = loader.getResource(base);
      require(selected != null && selected.toString().contains("META-INF/versions/"),
          "Runtime must activate versioned dependency classes: " + selected);
    }
  }

  private static URLClassLoader loader(List<URL> urls) {
    return new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader());
  }

  private static void discovery(ClassLoader loader) throws SQLException {
    boolean found = false;
    boolean other = false;
    for (Driver driver : ServiceLoader.load(Driver.class, loader)) {
      if (DRIVER.equals(driver.getClass().getName())) {
        found = true;
        require(driver.acceptsURL("jdbc:jparq:/unused"), "URL acceptance");
        driver.getPropertyInfo("jdbc:jparq:/unused", new Properties());
        require(driver.connect("jdbc:other:/unused", new Properties()) == null, "Unknown URL");
      }
      other |= driver.getClass().getName().equals(OtherDriver.class.getName());
    }
    require(found && other, "Both JDBC providers must be discovered");
  }

  private static void concurrent(Driver driver, String[] args) throws Exception {
    var executor = Executors.newFixedThreadPool(3);
    try {
      var tasks = new ArrayList<java.util.concurrent.Callable<Void>>();
      for (int i = 0; i < 3; i++) {
        tasks.add(() -> {
          connect(driver, args);
          return null;
        });
      }
      for (var result : executor.invokeAll(tasks)) {
        result.get();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static void connect(Driver driver, String[] args) throws Exception {
    ClassLoader context = Thread.currentThread().getContextClassLoader();
    try (Connection connection = driver.connect("jdbc:jparq:" + args[2], new Properties())) {
      verify(connection, Path.of(args[3]));
    }
    require(context == Thread.currentThread().getContextClassLoader(), "Successful read changed TCCL");
    try (Connection connection = driver.connect("jdbc:jparq:" + args[2], new Properties());
        Statement statement = connection.createStatement()) {
      try {
        statement.executeQuery("SELECT * FROM missing_table");
        throw new AssertionError("Missing table must fail");
      } catch (SQLException expected) {
        require(context == Thread.currentThread().getContextClassLoader(), "Failed read changed TCCL");
      }
    }
  }

  private static void verify(Connection connection, Path expectedPath) throws Exception {
    require(connection != null && connection.isValid(2), "Connection validation");
    var metadata = connection.getMetaData();
    try (ResultSet schemas = metadata.getSchemas()) {
      require(schemas.next(), "Schema discovery");
    }
    Properties expected = new Properties();
    try (var reader = Files.newBufferedReader(expectedPath)) {
      expected.load(reader);
    }
    for (String table : List.of("salary", "mtcars", "optimized", "gzip")) {
      try (ResultSet tables = metadata.getTables(null, "PUBLIC", table, new String[]{
          "TABLE"
      })) {
        require(tables.next() && table.equalsIgnoreCase(tables.getString("TABLE_NAME")), "Table " + table);
        require(!tables.next(), "Exactly one table " + table);
      }
      try (ResultSet columns = metadata.getColumns(null, "PUBLIC", table, "%")) {
        int count = 0;
        while (columns.next()) {
          count++;
          require(columns.getString("COLUMN_NAME").equals(expected.getProperty(table + ".column." + count)),
              "Column name/order " + table + ":" + count);
        }
        require(count == Integer.parseInt(expected.getProperty(table + ".columns")), "Columns " + table);
      }
      try (Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("SELECT * FROM " + table)) {
        require(digest(rows).equals(expected.getProperty(table)), "Every value in " + table);
        require(!rows.next(), "Result exhaustion " + table);
      }
    }
  }

  /**
   * Hashes every row and column with explicit NULL markers and length framing.
   *
   * @param rows
   *          rows to consume
   * @return hexadecimal SHA-256 digest
   * @throws Exception
   *           if JDBC access or digest creation fails
   */
  public static String digest(ResultSet rows) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    int columns = rows.getMetaData().getColumnCount();
    while (rows.next()) {
      for (int column = 1; column <= columns; column++) {
        Object object = rows.getObject(column);
        String value = object instanceof Object[] array ? java.util.Arrays.deepToString(array) : rows.getString(column);
        byte[] bytes = (value == null ? "N" : "V" + value.length() + ":" + value).getBytes(StandardCharsets.UTF_8);
        digest.update(bytes);
      }
      digest.update((byte) '\n');
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  /**
   * Additional harmless provider used to ensure discovery continues after JParq.
   */
  public static final class OtherDriver implements Driver {
    /** Creates a provider without initialization work. */
    public OtherDriver() {
    }

    @Override
    public Connection connect(String url, Properties info) {
      return null;
    }

    @Override
    public boolean acceptsURL(String url) {
      return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 1;
    }

    @Override
    public int getMinorVersion() {
      return 0;
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }

    @Override
    public Logger getParentLogger() {
      return Logger.getGlobal();
    }
  }
}
