package jparq;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests specific behaviours of {@link se.alipsa.jparq.JParqConnection}. */
public class JParqConnectionTest {

  private Connection newConnection() throws SQLException, URISyntaxException {
    URL mtcarsUrl = getClass().getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    String jdbcUrl = "jdbc:jparq:" + dir.toAbsolutePath();
    return DriverManager.getConnection(jdbcUrl);
  }

  @Test
  void createStatementWithTypeAndConcurrencyIsUnsupported() throws SQLException, URISyntaxException {
    try (Connection connection = newConnection()) {
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
          () -> connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    }
  }
}
