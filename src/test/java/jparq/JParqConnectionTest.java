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
  void createStatementWithTypeAndConcurrencySupportsReadOnlyForwardOnly() throws SQLException, URISyntaxException {
    try (Connection connection = newConnection()) {
      try (var statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        Assertions.assertNotNull(statement);
      }
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
          () -> connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
          () -> connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
    }
  }

  @Test
  void createStatementWithHoldabilitySupportsCloseCursorsAtCommit() throws SQLException, URISyntaxException {
    try (Connection connection = newConnection()) {
      try (var statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
          ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
        Assertions.assertNotNull(statement);
      }
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
          () -> connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
              ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }
  }
}
