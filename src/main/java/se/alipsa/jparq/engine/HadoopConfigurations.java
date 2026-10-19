package se.alipsa.jparq.engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

/**
 * Creates private Hadoop configurations independent of a client's context class
 * loader.
 */
public final class HadoopConfigurations {
  private HadoopConfigurations() {
  }

  /**
   * Creates a configuration for JParq local file reads without Hadoop default
   * resources.
   *
   * @return driver-aware local filesystem configuration
   */
  public static Configuration create() {
    return configure(new Configuration(false));
  }

  /**
   * Copies caller options and supplies the driver loader and local filesystem
   * implementation. The caller's configuration remains unchanged.
   *
   * @param source
   *          caller-owned configuration
   * @return a private driver-aware copy
   */
  public static Configuration copy(Configuration source) {
    return configure(new Configuration(source));
  }

  private static Configuration configure(Configuration configuration) {
    configuration.setClassLoader(HadoopConfigurations.class.getClassLoader());
    configuration.set("fs.file.impl", LocalFileSystem.class.getName());
    return configuration;
  }
}
