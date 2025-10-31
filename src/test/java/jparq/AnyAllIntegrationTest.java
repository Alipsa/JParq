package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Integration tests verifying SQL {@code ANY}/{@code ALL} handling through the JDBC driver.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class AnyAllIntegrationTest {

  private static Connection connection;

  /**
   * Open a connection to the bundled {@code acme} test dataset.
   *
   * @throws SQLException
   *           if the connection cannot be established
   * @throws URISyntaxException
   *           if the dataset URL cannot be converted to a path
   */
  @BeforeAll
  static void setUp() throws SQLException, URISyntaxException {
    URL root = AnyAllIntegrationTest.class.getResource("/acme");
    if (root == null) {
      throw new IllegalStateException("The acme dataset must be available on the classpath");
    }
    Path dir = Paths.get(root.toURI());
    connection = DriverManager.getConnection("jdbc:jparq:" + dir.toAbsolutePath());
  }

  /**
   * Close the shared connection after all tests have executed.
   *
   * @throws SQLException
   *           if closing fails
   */
  @AfterAll
  static void tearDown() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  @DisplayName("ANY subquery comparisons return matching rows")
  void anyComparisonMatches() throws SQLException {
    String sql = """
        SELECT first_name
        FROM employees
        WHERE id = ANY (SELECT employee FROM salary WHERE salary > 200000)
        ORDER BY first_name
        """;
    try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue(rs.next(), "At least one employee should satisfy the ANY comparison");
      Object raw = rs.getObject(1);
      String actual = raw instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : raw.toString();
      assertEquals("Sixten", actual);
      assertFalse(rs.next(), "Only one employee has a salary above the threshold");
    }
  }

  @Test
  @DisplayName("ALL subquery comparisons evaluate all candidate rows")
  void allComparisonMatches() throws SQLException {
    String sql = """
        SELECT salary
        FROM salary
        WHERE salary >= ALL (SELECT salary FROM salary WHERE employee = 1)
        ORDER BY salary
        """;
    List<Long> values = new ArrayList<>();
    try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        values.add(rs.getLong(1));
      }
    }
    assertEquals(List.of(165_000L, 180_000L, 195_000L, 230_000L), values);
  }

  @Test
  @DisplayName("SELECT ALL behaves like the default projection")
  void selectAllKeywordIsAccepted() throws SQLException {
    String sql = "SELECT ALL id FROM employees ORDER BY id";
    List<Integer> ids = new ArrayList<>();
    try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        ids.add(rs.getInt(1));
      }
    }
    assertEquals(List.of(1, 2, 3, 4, 5), ids);
  }
}
