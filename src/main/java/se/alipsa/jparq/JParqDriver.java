package se.alipsa.jparq;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * An implementation of the java.sql.Driver interface for parquet files. JDBC
 * URL format: jdbc:jparq:/abs/path/to/dir jdbc:jparq:file:///abs/path/to/dir
 * jdbc:jparq:/abs/path?caseSensitive=false
 *
 * <p>
 * Each .parquet file in the directory is exposed as a table named by its base
 * filename (without extension).
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqDriver implements Driver {

  public static final String URL_PREFIX = "jdbc:jparq:"; // <-- fix

  static {
    try {
      DriverManager.registerDriver(new JParqDriver());
    } catch (SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null; // allow DriverManager to try others
    }
    return new JParqConnection(url, info);
  }

  @Override
  public boolean acceptsURL(String url) {
    return url != null && url.startsWith(URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    return new DriverPropertyInfo[]{
        new DriverPropertyInfo("caseSensitive", info.getProperty("caseSensitive", "false"))
    };
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() {
    return Logger.getLogger("se.alipsa.jparq");
  } // optional tidy
}
